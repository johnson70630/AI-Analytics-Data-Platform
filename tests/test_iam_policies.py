import json
import unittest
from pathlib import Path


IAM_ROOT = Path(__file__).resolve().parents[1] / "infra" / "iam"
BUCKET_ARN = "arn:aws:s3:::${S3_BUCKET_NAME}"


def load_policy(name: str) -> dict:
    return json.loads((IAM_ROOT / name).read_text(encoding="utf-8"))


def statements_by_sid(policy: dict) -> dict[str, dict]:
    return {statement["Sid"]: statement for statement in policy["Statement"]}


class IAMPolicyTests(unittest.TestCase):
    def test_policy_documents_are_valid_json_with_current_version(self):
        for name in ("generator_s3_policy.json", "ingestion_s3_policy.json"):
            policy = load_policy(name)
            self.assertEqual(policy["Version"], "2012-10-17")
            self.assertTrue(policy["Statement"])

    def test_generator_is_scoped_to_owned_prefixes(self):
        statements = statements_by_sid(load_policy("generator_s3_policy.json"))
        approved_objects = {
            f"{BUCKET_ARN}/raw/*",
            f"{BUCKET_ARN}/bronze/*",
            f"{BUCKET_ARN}/quality/injection_manifest/*",
        }
        self.assertEqual(
            set(statements["ReadApprovedGeneratorObjects"]["Resource"]),
            approved_objects,
        )
        self.assertEqual(
            set(statements["WriteApprovedGeneratorObjects"]["Resource"]),
            approved_objects,
        )
        self.assertEqual(
            set(
                statements["ListApprovedGeneratorPrefixes"]["Condition"]
                ["StringLike"]["s3:prefix"]
            ),
            {"raw/*", "bronze/*", "quality/injection_manifest/*"},
        )

    def test_ingestion_is_bronze_read_only(self):
        statements = statements_by_sid(load_policy("ingestion_s3_policy.json"))
        self.assertEqual(statements["ReadBronzeObjects"]["Action"], "s3:GetObject")
        self.assertEqual(
            statements["ReadBronzeObjects"]["Resource"],
            f"{BUCKET_ARN}/bronze/*",
        )
        self.assertEqual(
            statements["ListBronzePartitions"]["Condition"]["StringLike"]
            ["s3:prefix"],
            "bronze/*",
        )

    def test_policies_exclude_delete_admin_and_wildcard_actions(self):
        forbidden = {
            "s3:*",
            "s3:DeleteBucket",
            "s3:DeleteObject",
            "s3:PutBucketPolicy",
        }
        for name in ("generator_s3_policy.json", "ingestion_s3_policy.json"):
            actions = {
                action
                for statement in load_policy(name)["Statement"]
                for action in (
                    statement["Action"]
                    if isinstance(statement["Action"], list)
                    else [statement["Action"]]
                )
            }
            self.assertTrue(actions.isdisjoint(forbidden))

    def test_ingestion_policy_has_no_write_or_non_bronze_object_access(self):
        policy_text = json.dumps(load_policy("ingestion_s3_policy.json"))
        self.assertNotIn("PutObject", policy_text)
        self.assertNotIn("DeleteObject", policy_text)
        self.assertNotIn("/raw/", policy_text)
        self.assertNotIn("/quality/", policy_text)


if __name__ == "__main__":
    unittest.main()
