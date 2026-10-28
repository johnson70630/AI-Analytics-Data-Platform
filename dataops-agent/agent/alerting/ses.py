"""Amazon SES v2 adapter for provider-neutral incident email delivery."""

from __future__ import annotations

from typing import Any, Protocol

import boto3
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
    PartialCredentialsError,
    ReadTimeoutError,
)

from agent.alerting.delivery import (
    EmailDeliveryReceipt,
    PermanentEmailDeliveryError,
    RenderedEmail,
    RetryableEmailDeliveryError,
)


_RETRYABLE_ERROR_CODES = frozenset(
    {
        "InternalServiceError",
        "InternalServiceErrorException",
        "LimitExceededException",
        "RequestTimeout",
        "RequestTimeoutException",
        "ServiceUnavailable",
        "ServiceUnavailableException",
        "Throttling",
        "ThrottlingException",
        "TooManyRequestsException",
    }
)

_PERMANENT_ERROR_CODES = frozenset(
    {
        "AccessDenied",
        "AccessDeniedException",
        "AccountSuspendedException",
        "BadRequestException",
        "MailFromDomainNotVerifiedException",
        "MessageRejected",
        "NotFoundException",
        "SendingPausedException",
        "ValidationException",
    }
)

_RETRYABLE_TRANSPORT_ERRORS = (
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)


class SesV2Client(Protocol):
    """Minimal injectable subset of the boto3 SES v2 client."""

    def send_email(self, **kwargs: Any) -> dict[str, Any]: ...


def _safe_error_code(error: ClientError) -> str:
    value = str(error.response.get("Error", {}).get("Code") or "UnknownSesError")
    sanitized = "".join(character for character in value if character.isalnum() or character in "._-")
    return sanitized[:100] or "UnknownSesError"


def _safe_message_id(value: object) -> str:
    candidate = str(value or "").strip()
    return "".join(
        character
        for character in candidate
        if character.isalnum() or character in "._:@/+=-"
    )[:500]


class SesEmailSender:
    """SES v2 sender that reports API acceptance, not confirmed inbox delivery."""

    provider_name = "ses"

    def __init__(
        self,
        *,
        client: SesV2Client | None = None,
        region_name: str | None = None,
    ) -> None:
        self._client = client or boto3.client("sesv2", region_name=region_name)

    def send(self, message: RenderedEmail) -> EmailDeliveryReceipt:
        request: dict[str, Any] = {
            "FromEmailAddress": message.sender_address,
            "Destination": {"ToAddresses": list(message.recipient_addresses)},
            "Content": {
                "Simple": {
                    "Subject": {"Data": message.subject, "Charset": "UTF-8"},
                    "Body": {
                        "Text": {"Data": message.text_body, "Charset": "UTF-8"},
                        "Html": {"Data": message.html_body, "Charset": "UTF-8"},
                    },
                }
            },
            "EmailTags": [
                {"Name": "dataops_notification_id", "Value": message.notification_id}
            ],
        }
        if message.reply_to:
            request["ReplyToAddresses"] = [message.reply_to]

        try:
            response = self._client.send_email(**request)
        except (NoCredentialsError, PartialCredentialsError) as exc:
            raise PermanentEmailDeliveryError(
                provider=self.provider_name,
                safe_message="SES credentials are unavailable or incomplete",
            ) from exc
        except _RETRYABLE_TRANSPORT_ERRORS as exc:
            raise RetryableEmailDeliveryError(
                provider=self.provider_name,
                safe_message=f"SES transport failure ({type(exc).__name__})",
            ) from exc
        except ClientError as exc:
            code = _safe_error_code(exc)
            if code in _RETRYABLE_ERROR_CODES:
                raise RetryableEmailDeliveryError(
                    provider=self.provider_name,
                    safe_message=f"SES temporarily rejected the request ({code})",
                ) from exc
            if code in _PERMANENT_ERROR_CODES:
                raise PermanentEmailDeliveryError(
                    provider=self.provider_name,
                    safe_message=f"SES permanently rejected the request ({code})",
                ) from exc
            raise PermanentEmailDeliveryError(
                provider=self.provider_name,
                safe_message=f"SES rejected the request with an unclassified error ({code})",
            ) from exc
        except Exception as exc:
            raise RetryableEmailDeliveryError(
                provider=self.provider_name,
                safe_message=f"unexpected SES delivery failure ({type(exc).__name__})",
            ) from exc

        message_id = _safe_message_id(response.get("MessageId"))
        if not message_id:
            raise RetryableEmailDeliveryError(
                provider=self.provider_name,
                safe_message="SES acceptance response did not include a message identifier",
            )
        return EmailDeliveryReceipt(
            provider_name=self.provider_name,
            provider_message_id=message_id,
            status="accepted",
        )
