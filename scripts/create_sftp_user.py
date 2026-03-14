#!/usr/bin/env python3
"""
Create a new SFTP user for the AWS Transfer Family server.

Generates an SSH key pair, creates a scoped IAM role, registers the
user with Transfer Family, and prints connection instructions.

Usage:
    python scripts/create_sftp_user.py \
        --server-id s-1234567890abcdef0 \
        --username analytics-upload \
        --s3-prefix analytics/ \
        --bucket etl-loader-test-pickybat

    # With a specific output directory for keys:
    python scripts/create_sftp_user.py \
        --server-id s-1234567890abcdef0 \
        --username reporting-upload \
        --s3-prefix reporting/ \
        --bucket etl-loader-test-pickybat \
        --key-dir ./output/sftp-keys
"""

import argparse
import json
import logging
import os
import stat
import subprocess
import sys
import time

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def generate_ssh_key_pair(username: str, key_dir: str) -> tuple[str, str]:
    """
    Generate an ED25519 SSH key pair for the SFTP user.

    Returns (private_key_path, public_key_body).
    """
    os.makedirs(key_dir, exist_ok=True)

    private_key_path = os.path.join(key_dir, f"{username}_sftp_key")
    public_key_path = f"{private_key_path}.pub"

    # Remove existing keys to avoid ssh-keygen prompts
    for path in (private_key_path, public_key_path):
        if os.path.exists(path):
            os.remove(path)

    # Generate ED25519 key (faster, shorter, more secure than RSA for SSH)
    subprocess.run(
        [
            "ssh-keygen",
            "-t", "rsa",
            "-b", "4096",
            "-f", private_key_path,
            "-N", "",  # No passphrase
            "-C", f"sftp-{username}@etl-loader",
        ],
        check=True,
        capture_output=True,
    )

    # Restrictive permissions on private key
    os.chmod(private_key_path, stat.S_IRUSR)

    # Read public key body
    with open(public_key_path) as f:
        public_key_body = f.read().strip()

    logger.info(f"Generated SSH key pair: {private_key_path}")
    return private_key_path, public_key_body


def create_iam_role(
    username: str, bucket: str, s3_prefix: str, region: str
) -> str:
    """
    Create an IAM role scoped to the user's S3 prefix.

    Returns the role ARN.
    """
    iam = boto3.client("iam")
    role_name = f"etl-sftp-{username}-role"

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "transfer.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowListBucketUnderPrefix",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{bucket}",
                "Condition": {
                    "StringLike": {"s3:prefix": [f"{s3_prefix}*"]}
                },
            },
            {
                "Sid": "AllowReadWriteUnderPrefix",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:DeleteObject",
                    "s3:DeleteObjectVersion",
                ],
                "Resource": f"arn:aws:s3:::{bucket}/{s3_prefix}*",
            },
        ],
    }

    try:
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description=f"SFTP access for {username} to s3://{bucket}/{s3_prefix}",
            Tags=[
                {"Key": "Project", "Value": "s3-snowflake-loader"},
                {"Key": "Component", "Value": "sftp-gateway"},
                {"Key": "SftpUser", "Value": username},
            ],
        )
        role_arn = response["Role"]["Arn"]
        logger.info(f"Created IAM role: {role_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            logger.info(f"IAM role {role_name} already exists — reusing")
            response = iam.get_role(RoleName=role_name)
            role_arn = response["Role"]["Arn"]
        else:
            raise

    # Attach the inline policy
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=f"sftp-s3-{username}",
        PolicyDocument=json.dumps(s3_policy),
    )
    logger.info(f"Attached S3 policy to {role_name}")

    # IAM role propagation delay — Transfer Family needs the role to exist
    logger.info("Waiting 10s for IAM role propagation...")
    time.sleep(10)

    return role_arn


def register_sftp_user(
    server_id: str,
    username: str,
    role_arn: str,
    bucket: str,
    s3_prefix: str,
    public_key_body: str,
) -> None:
    """Register the user with AWS Transfer Family and import their SSH key."""
    transfer = boto3.client("transfer")

    # Create the user with logical home directory mapping
    try:
        transfer.create_user(
            ServerId=server_id,
            UserName=username,
            Role=role_arn,
            HomeDirectoryType="LOGICAL",
            HomeDirectoryMappings=[
                {"Entry": "/", "Target": f"/{bucket}/{s3_prefix}"},
            ],
            Tags=[
                {"Key": "Project", "Value": "s3-snowflake-loader"},
                {"Key": "Component", "Value": "sftp-gateway"},
            ],
        )
        logger.info(f"Created Transfer Family user: {username}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceExistsException":
            logger.info(f"User {username} already exists — updating")
            transfer.update_user(
                ServerId=server_id,
                UserName=username,
                Role=role_arn,
                HomeDirectoryType="LOGICAL",
                HomeDirectoryMappings=[
                    {"Entry": "/", "Target": f"/{bucket}/{s3_prefix}"},
                ],
            )
        else:
            raise

    # Import the SSH public key
    transfer.import_ssh_public_key(
        ServerId=server_id,
        UserName=username,
        SshPublicKeyBody=public_key_body,
    )
    logger.info(f"Imported SSH public key for {username}")


def get_sftp_endpoint(server_id: str) -> str:
    """Get the SFTP endpoint hostname for the Transfer Family server."""
    transfer = boto3.client("transfer")
    response = transfer.describe_server(ServerId=server_id)
    server = response["Server"]

    endpoint = f"{server_id}.server.transfer.{boto3.session.Session().region_name}.amazonaws.com"

    # Check server state
    state = server.get("State", "UNKNOWN")
    if state != "ONLINE":
        logger.warning(f"Server is {state}, not ONLINE. SFTP connections may fail.")

    return endpoint


def print_connection_instructions(
    username: str,
    endpoint: str,
    private_key_path: str,
    s3_prefix: str,
    bucket: str,
) -> None:
    """Print user-friendly connection instructions."""
    abs_key = os.path.abspath(private_key_path)

    print("\n" + "=" * 70)
    print(f"  SFTP User Created: {username}")
    print("=" * 70)
    print()
    print(f"  Endpoint:     {endpoint}")
    print(f"  Username:     {username}")
    print(f"  Private key:  {abs_key}")
    print(f"  S3 target:    s3://{bucket}/{s3_prefix}")
    print()
    print("  ── Connect via command line ──")
    print(f"  sftp -i {abs_key} {username}@{endpoint}")
    print()
    print("  ── Upload a file ──")
    print(f"  sftp -i {abs_key} {username}@{endpoint} <<< 'put local_file.csv TABLE.T/'")
    print()
    print("  ── WinSCP / FileZilla ──")
    print(f"  Host:         {endpoint}")
    print(f"  Port:         22")
    print(f"  Protocol:     SFTP")
    print(f"  Username:     {username}")
    print(f"  Private key:  {abs_key}")
    print(f"                (WinSCP: convert to .ppk with PuTTYgen)")
    print()
    print("  ── Folder structure ──")
    print(f"  Upload files to: /<TABLE_NAME>.<MODE>/filename.ext")
    print(f"  Example:         /CUSTOMERS.T/customers.csv")
    print(f"  This loads into: Snowflake → {s3_prefix.rstrip('/').upper()} DB")
    print()
    print("  ── Share with the user ──")
    print(f"  Send them:")
    print(f"    1. The private key file: {abs_key}")
    print(f"    2. The endpoint: {endpoint}")
    print(f"    3. The username: {username}")
    print(f"    4. A link to docs/sftp-guide.md for folder conventions")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Create a new SFTP user for the ETL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --server-id s-1234567890abcdef0 \\
           --username analytics-upload \\
           --s3-prefix analytics/ \\
           --bucket etl-loader-test-pickybat

  %(prog)s --server-id s-1234567890abcdef0 \\
           --username reporting-upload \\
           --s3-prefix reporting/ \\
           --bucket etl-loader-test-pickybat \\
           --key-dir ./output/sftp-keys
        """,
    )
    parser.add_argument(
        "--server-id", required=True,
        help="AWS Transfer Family server ID (e.g., s-1234567890abcdef0)",
    )
    parser.add_argument(
        "--username", required=True,
        help="SFTP username (e.g., analytics-upload)",
    )
    parser.add_argument(
        "--s3-prefix", required=True,
        help="S3 prefix the user can access (e.g., analytics/)",
    )
    parser.add_argument(
        "--bucket", required=True,
        help="S3 bucket name (e.g., etl-loader-test-pickybat)",
    )
    parser.add_argument(
        "--key-dir", default="./output/sftp-keys",
        help="Directory to save SSH key pair (default: ./output/sftp-keys)",
    )
    parser.add_argument(
        "--region", default=None,
        help="AWS region (default: from AWS config)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Generate SSH keys only, don't create AWS resources",
    )

    args = parser.parse_args()

    # Ensure prefix ends with /
    s3_prefix = args.s3_prefix
    if not s3_prefix.endswith("/"):
        s3_prefix += "/"

    # Set region if specified
    if args.region:
        os.environ["AWS_DEFAULT_REGION"] = args.region

    # 1. Generate SSH key pair
    print(f"\n[1/4] Generating SSH key pair for '{args.username}'...")
    private_key_path, public_key_body = generate_ssh_key_pair(
        args.username, args.key_dir
    )

    if args.dry_run:
        print(f"\n[DRY RUN] SSH key pair generated at: {args.key_dir}/")
        print(f"Public key:\n{public_key_body}")
        print("\nSkipping AWS resource creation (--dry-run).")
        return

    # 2. Create IAM role
    print(f"\n[2/4] Creating IAM role scoped to s3://{args.bucket}/{s3_prefix}...")
    role_arn = create_iam_role(
        args.username, args.bucket, s3_prefix, args.region or "us-east-1"
    )

    # 3. Register with Transfer Family
    print(f"\n[3/4] Registering user with Transfer Family server {args.server_id}...")
    register_sftp_user(
        args.server_id, args.username, role_arn,
        args.bucket, s3_prefix, public_key_body,
    )

    # 4. Get endpoint and print instructions
    print(f"\n[4/4] Fetching SFTP endpoint...")
    endpoint = get_sftp_endpoint(args.server_id)

    print_connection_instructions(
        args.username, endpoint, private_key_path, s3_prefix, args.bucket
    )


if __name__ == "__main__":
    main()
