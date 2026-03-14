#!/usr/bin/env python3
"""
Generate deployment artifacts from config.yaml.

Outputs:
  output/snowflake-setup.sql     — Snowflake objects (admin DB, tables, roles, stages)
  output/infrastructure.yaml     — CloudFormation template (Lambdas, IAM, S3 events)
  output/secrets-bootstrap.sh    — Script to seed Secrets Manager
  output/teardown.sql            — Clean removal of all Snowflake objects
  output/keys/                   — RSA key pairs per database service user
"""

import os
import sys
import textwrap
from pathlib import Path

import yaml

OUTPUT_DIR = Path("output")
KEYS_DIR = OUTPUT_DIR / "keys"


def load_config() -> dict:
    """Load and validate config.yaml."""
    config_path = Path("config.yaml")
    if not config_path.exists():
        print("❌ config.yaml not found. Copy config.example.yaml to config.yaml and fill in your values.")
        sys.exit(1)

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Validate required fields
    required = {
        "aws.region": config.get("aws", {}).get("region"),
        "aws.s3_bucket": config.get("aws", {}).get("s3_bucket"),
        "snowflake.account": config.get("snowflake", {}).get("account"),
        "snowflake.warehouse": config.get("snowflake", {}).get("warehouse"),
        "databases": config.get("databases"),
    }

    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"❌ Missing required config: {', '.join(missing)}")
        sys.exit(1)

    if not config["databases"]:
        print("❌ At least one database mapping is required")
        sys.exit(1)

    # Defaults
    config.setdefault("snowflake", {})
    config["snowflake"].setdefault("admin_database", "ADMIN_DB")
    config["snowflake"].setdefault("admin_schema", "_PIPELINE")
    config.setdefault("lambda", {})
    config["lambda"].setdefault("runtime", "python3.12")
    config["lambda"].setdefault("timeout", 900)
    config["lambda"].setdefault("memory_mb", 512)
    config.setdefault("naming", {})
    config["naming"].setdefault("integration_prefix", "S3_INT")
    config["naming"].setdefault("stage_name", "S3_LOAD_STAGE")
    config["naming"].setdefault("lambda_prefix", "etl")
    config.setdefault("tags", {})
    config.setdefault("defaults", {})

    # Per-database defaults
    for db in config["databases"]:
        db_name = db["name"].upper()
        db.setdefault("schema", "STAGING")
        db.setdefault("service_user", f"SVC_LOADER_{db_name}")
        db.setdefault("role", f"DATA_LOADER_{db_name}")

    return config


def generate_rsa_keys(config: dict) -> dict:
    """Generate RSA key pairs for each database service user."""
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization

    KEYS_DIR.mkdir(parents=True, exist_ok=True)
    keys = {}

    for db in config["databases"]:
        user = db["service_user"]
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

        # Private key (PEM)
        private_pem = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        # Public key (PEM)
        public_pem = key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        private_path = KEYS_DIR / f"{user.lower()}_private.pem"
        public_path = KEYS_DIR / f"{user.lower()}_public.pem"

        private_path.write_bytes(private_pem)
        public_path.write_bytes(public_pem)
        os.chmod(private_path, 0o600)

        # Extract public key body (strip header/footer) for Snowflake
        pub_body = public_pem.decode().replace("-----BEGIN PUBLIC KEY-----", "")
        pub_body = pub_body.replace("-----END PUBLIC KEY-----", "").strip()

        keys[user] = {
            "private_pem": private_pem.decode(),
            "public_key_body": pub_body,
            "private_path": str(private_path),
            "public_path": str(public_path),
        }

        print(f"  🔑 Generated RSA key pair for {user}")

    return keys


def generate_snowflake_sql(config: dict, keys: dict) -> str:
    """Generate Snowflake setup SQL."""
    sf = config["snowflake"]
    admin_db = sf["admin_database"]
    admin_schema = sf["admin_schema"]
    warehouse = sf["warehouse"]
    naming = config["naming"]
    bucket = config["aws"]["s3_bucket"]

    lines = []
    lines.append("-- =============================================================")
    lines.append("-- S3 → Snowflake Loader: Snowflake Setup")
    lines.append(f"-- Generated for account: {sf['account']}")
    lines.append("-- ")
    lines.append("-- Run this as ACCOUNTADMIN or equivalent.")
    lines.append("-- Review each section before executing.")
    lines.append("-- =============================================================")
    lines.append("")

    # --- Admin Database ---
    lines.append("-- ─── 1. Admin Database & Pipeline Tables ────────────────────")
    lines.append(f"CREATE DATABASE IF NOT EXISTS {admin_db};")
    lines.append(f"CREATE SCHEMA IF NOT EXISTS {admin_db}.{admin_schema};")
    lines.append("")

    lines.append(f"CREATE TABLE IF NOT EXISTS {admin_db}.{admin_schema}.LOAD_HISTORY (")
    lines.append("    LOAD_ID             VARCHAR(36)     DEFAULT UUID_STRING(),")
    lines.append("    S3_BUCKET           VARCHAR(256)    NOT NULL,")
    lines.append("    S3_KEY              VARCHAR(1024)   NOT NULL,")
    lines.append("    S3_SIZE_BYTES       NUMBER,")
    lines.append("    S3_ETAG             VARCHAR(256),")
    lines.append("    TARGET_DATABASE     VARCHAR(128)    NOT NULL,")
    lines.append("    TARGET_SCHEMA       VARCHAR(128)    NOT NULL,")
    lines.append("    TARGET_TABLE        VARCHAR(128)    NOT NULL,")
    lines.append("    LOAD_MODE           VARCHAR(10)     NOT NULL,")
    lines.append("    TABLE_CREATED       BOOLEAN         DEFAULT FALSE,")
    lines.append("    FILE_FORMAT_USED    VARCHAR(4000),")
    lines.append("    ROWS_LOADED         NUMBER,")
    lines.append("    ROWS_PARSED         NUMBER,")
    lines.append("    ERRORS_SEEN         NUMBER          DEFAULT 0,")
    lines.append("    STATUS              VARCHAR(20)     NOT NULL,")
    lines.append("    ERROR_MESSAGE       VARCHAR(4000),")
    lines.append("    COPY_INTO_QUERY_ID  VARCHAR(256),")
    lines.append("    LAMBDA_REQUEST_ID   VARCHAR(256),")
    lines.append("    LAMBDA_FUNCTION     VARCHAR(256),")
    lines.append("    STARTED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),")
    lines.append("    COMPLETED_AT        TIMESTAMP_NTZ,")
    lines.append("    DURATION_SECONDS    NUMBER,")
    lines.append("    DUPLICATE_OF        VARCHAR(36),")
    lines.append("    PRIMARY KEY (LOAD_ID)")
    lines.append(");")
    lines.append("")

    lines.append(f"CREATE TABLE IF NOT EXISTS {admin_db}.{admin_schema}.LOAD_ERRORS (")
    lines.append("    ERROR_ID            VARCHAR(36)     DEFAULT UUID_STRING(),")
    lines.append("    LOAD_ID             VARCHAR(36)     NOT NULL,")
    lines.append("    TARGET_DATABASE     VARCHAR(128)    NOT NULL,")
    lines.append("    S3_KEY              VARCHAR(1024)   NOT NULL,")
    lines.append("    ERROR_LINE          NUMBER,")
    lines.append("    ERROR_COLUMN        NUMBER,")
    lines.append("    ERROR_MESSAGE       VARCHAR(4000),")
    lines.append("    REJECTED_RECORD     VARCHAR(16777216),")
    lines.append("    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()")
    lines.append(");")
    lines.append("")

    lines.append(f"-- Notification audit trail")
    lines.append(f"CREATE TABLE IF NOT EXISTS {admin_db}.{admin_schema}.NOTIFICATIONS (")
    lines.append("    NOTIFICATION_ID     VARCHAR(36)     DEFAULT UUID_STRING(),")
    lines.append("    LOAD_ID             VARCHAR(36)     NOT NULL,")
    lines.append("    NOTIFICATION_TYPE   VARCHAR(20)     NOT NULL,")
    lines.append("    CHANNEL             VARCHAR(50)     NOT NULL,")
    lines.append("    RECIPIENT           VARCHAR(256),")
    lines.append("    MESSAGE_SUBJECT     VARCHAR(256),")
    lines.append("    MESSAGE_BODY        VARCHAR(16777216),")
    lines.append("    SENT_AT             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),")
    lines.append("    DELIVERY_STATUS     VARCHAR(20)     NOT NULL,")
    lines.append("    SNS_MESSAGE_ID      VARCHAR(256),")
    lines.append("    PRIMARY KEY (NOTIFICATION_ID)")
    lines.append(");")
    lines.append("")

    lines.append(f"CREATE TABLE IF NOT EXISTS {admin_db}.{admin_schema}.CONTROL_TABLE (")
    lines.append("    TARGET_DATABASE     VARCHAR(128)    NOT NULL,")
    lines.append("    TARGET_SCHEMA       VARCHAR(128)    NOT NULL,")
    lines.append("    TARGET_TABLE        VARCHAR(128)    NOT NULL,")
    lines.append("    LOAD_MODE           VARCHAR(10)     DEFAULT NULL,")
    lines.append("    FILE_FORMAT_NAME    VARCHAR(256)    DEFAULT NULL,")
    lines.append("    FILE_FORMAT_OPTIONS VARCHAR(4000)   DEFAULT NULL,")
    lines.append("    COPY_OPTIONS        VARCHAR(4000)   DEFAULT NULL,")
    lines.append("    PRE_LOAD_SQL        VARCHAR(4000)   DEFAULT NULL,")
    lines.append("    POST_LOAD_SQL       VARCHAR(4000)   DEFAULT NULL,")
    lines.append("    MERGE_KEYS          VARCHAR(1000)   DEFAULT NULL,")
    lines.append("    AUTO_CREATE_TABLE   BOOLEAN         DEFAULT TRUE,")
    lines.append("    CREATE_TABLE_DDL    VARCHAR(16777216) DEFAULT NULL,")
    lines.append("    ENABLED             BOOLEAN         DEFAULT TRUE,")
    lines.append("    NOTES               VARCHAR(2000)   DEFAULT NULL,")
    lines.append("    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),")
    lines.append("    UPDATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),")
    lines.append("    PRIMARY KEY (TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE)")
    lines.append(");")
    lines.append("")

    # --- Warehouse ---
    lines.append("-- ─── 2. Warehouse ─────────────────────────────────────────")
    lines.append(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse}")
    lines.append("    WAREHOUSE_SIZE = 'XSMALL'")
    lines.append("    AUTO_SUSPEND = 60")
    lines.append("    AUTO_RESUME = TRUE")
    lines.append("    INITIALLY_SUSPENDED = TRUE;")
    lines.append("")

    # --- Per-Database Objects ---
    for db in config["databases"]:
        db_name = db["name"].upper()
        schema = db["schema"].upper()
        prefix = db["s3_prefix"]
        user = db["service_user"]
        role = db["role"]
        int_name = f"{naming['integration_prefix']}_{db_name}"
        stage_name = naming["stage_name"]

        lines.append(f"-- ─── 3. Database: {db_name} ────────────────────────────────")
        lines.append(f"CREATE DATABASE IF NOT EXISTS {db_name};")
        lines.append(f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema};")
        lines.append("")

        # Role
        lines.append(f"-- Role for {db_name}")
        lines.append(f"CREATE ROLE IF NOT EXISTS {role};")
        lines.append(f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {role};")
        lines.append("")

        # Target DB grants
        lines.append(f"-- Target database grants")
        lines.append(f"GRANT USAGE ON DATABASE {db_name} TO ROLE {role};")
        lines.append(f"GRANT USAGE ON SCHEMA {db_name}.{schema} TO ROLE {role};")
        lines.append(f"GRANT CREATE TABLE ON SCHEMA {db_name}.{schema} TO ROLE {role};")
        lines.append(f"GRANT SELECT, INSERT, TRUNCATE ON ALL TABLES IN SCHEMA {db_name}.{schema} TO ROLE {role};")
        lines.append(f"GRANT SELECT, INSERT, TRUNCATE ON FUTURE TABLES IN SCHEMA {db_name}.{schema} TO ROLE {role};")
        lines.append("")

        # Admin DB grants
        lines.append(f"-- Admin database grants (logging)")
        lines.append(f"GRANT USAGE ON DATABASE {admin_db} TO ROLE {role};")
        lines.append(f"GRANT USAGE ON SCHEMA {admin_db}.{admin_schema} TO ROLE {role};")
        lines.append(f"GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA {admin_db}.{admin_schema} TO ROLE {role};")
        lines.append("")

        # Service user
        pub_key = keys.get(user, {}).get("public_key_body", "<PASTE_PUBLIC_KEY_HERE>")
        lines.append(f"-- Service user (RSA key auth)")
        lines.append(f"CREATE USER IF NOT EXISTS {user}")
        lines.append(f"    RSA_PUBLIC_KEY = '{pub_key}'")
        lines.append(f"    DEFAULT_ROLE = {role}")
        lines.append(f"    DEFAULT_WAREHOUSE = {warehouse}")
        lines.append(f"    MUST_CHANGE_PASSWORD = FALSE;")
        lines.append(f"GRANT ROLE {role} TO USER {user};")
        lines.append("")

        # Storage integration
        lines.append(f"-- Storage integration (update STORAGE_AWS_ROLE_ARN after CloudFormation deploy)")
        lines.append(f"CREATE STORAGE INTEGRATION IF NOT EXISTS {int_name}")
        lines.append(f"    TYPE = EXTERNAL_STAGE")
        lines.append(f"    STORAGE_PROVIDER = 'S3'")
        lines.append(f"    ENABLED = TRUE")
        lines.append(f"    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/{naming['lambda_prefix']}-{db_name.lower()}-snowflake-role'")
        lines.append(f"    STORAGE_ALLOWED_LOCATIONS = ('s3://{bucket}/{prefix}');")
        lines.append("")
        lines.append(f"-- After creating the integration, run:")
        lines.append(f"-- DESC INTEGRATION {int_name};")
        lines.append(f"-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID")
        lines.append(f"-- into the CloudFormation parameters for cross-account trust.")
        lines.append("")

        # External stage
        lines.append(f"-- External stage")
        lines.append(f"GRANT CREATE STAGE ON SCHEMA {db_name}.{schema} TO ROLE {role};")
        lines.append(f"CREATE OR REPLACE STAGE {db_name}.{schema}.{stage_name}")
        lines.append(f"    URL = 's3://{bucket}/{prefix}'")
        lines.append(f"    STORAGE_INTEGRATION = {int_name};")
        lines.append(f"GRANT USAGE ON STAGE {db_name}.{schema}.{stage_name} TO ROLE {role};")
        lines.append("")
        lines.append("")

    return "\n".join(lines)


def generate_cloudformation(config: dict) -> str:
    """Generate CloudFormation template."""
    aws = config["aws"]
    sf = config["snowflake"]
    lam = config["lambda"]
    naming = config["naming"]
    tags = config.get("tags", {})

    # Build YAML manually for readability
    lines = []
    lines.append("AWSTemplateFormatVersion: '2010-09-09'")
    lines.append("Description: S3 to Snowflake Automated Data Loader")
    lines.append("")

    # --- Parameters ---
    lines.append("Parameters:")
    lines.append("  S3BucketName:")
    lines.append("    Type: String")
    lines.append(f"    Default: '{aws['s3_bucket']}'")
    lines.append("    Description: S3 bucket for data landing")
    lines.append("  S3BucketArn:")
    lines.append("    Type: String")
    bucket_name = aws['s3_bucket']
    lines.append(f"    Default: 'arn:aws:s3:::{bucket_name}'")
    lines.append("    Description: ARN of the S3 bucket")

    for db in config["databases"]:
        db_name = db["name"].upper()
        int_name = f"{naming['integration_prefix']}_{db_name}"
        lines.append(f"  SnowflakeIamArn{db_name.replace('_', '')}:")
        lines.append("    Type: String")
        lines.append(f"    Default: ''")
        lines.append(f"    Description: >-")
        lines.append(f"      STORAGE_AWS_IAM_USER_ARN from DESC INTEGRATION {int_name}")
        lines.append(f"  SnowflakeExternalId{db_name.replace('_', '')}:")
        lines.append("    Type: String")
        lines.append(f"    Default: ''")
        lines.append(f"    Description: >-")
        lines.append(f"      STORAGE_AWS_EXTERNAL_ID from DESC INTEGRATION {int_name}")

    lines.append("")

    # --- Resources ---
    lines.append("Resources:")

    for db in config["databases"]:
        db_name = db["name"].upper()
        db_lower = db_name.lower().replace("_", "-")
        prefix = db["s3_prefix"]
        safe_name = db_name.replace("_", "")

        # IAM Role for Lambda
        lines.append(f"  LambdaRole{safe_name}:")
        lines.append("    Type: AWS::IAM::Role")
        lines.append("    Properties:")
        lines.append(f"      RoleName: {naming['lambda_prefix']}-{db_lower}-lambda-role")
        lines.append("      AssumeRolePolicyDocument:")
        lines.append("        Version: '2012-10-17'")
        lines.append("        Statement:")
        lines.append("          - Effect: Allow")
        lines.append("            Principal:")
        lines.append("              Service: lambda.amazonaws.com")
        lines.append("            Action: sts:AssumeRole")
        lines.append("      ManagedPolicyArns:")
        lines.append("        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
        lines.append("      Policies:")
        lines.append(f"        - PolicyName: {naming['lambda_prefix']}-{db_lower}-policy")
        lines.append("          PolicyDocument:")
        lines.append("            Version: '2012-10-17'")
        lines.append("            Statement:")
        lines.append("              - Effect: Allow")
        lines.append("                Action:")
        lines.append("                  - s3:GetObject")
        lines.append("                  - s3:ListBucket")
        lines.append("                Resource:")
        lines.append(f"                  - !Sub 'arn:aws:s3:::${{S3BucketName}}/{prefix}*'")
        lines.append(f"                  - !Sub 'arn:aws:s3:::${{S3BucketName}}'")
        lines.append("              - Effect: Allow")
        lines.append("                Action:")
        lines.append("                  - secretsmanager:GetSecretValue")
        lines.append("                Resource:")
        lines.append(f"                  - !Sub 'arn:aws:secretsmanager:${{AWS::Region}}:${{AWS::AccountId}}:secret:etl/{db_name.lower()}/*'")
        lines.append("              - Effect: Allow")
        lines.append("                Action:")
        lines.append("                  - sns:Publish")
        lines.append("                Resource:")
        lines.append(f"                  - !Ref NotificationTopic{safe_name}")
        lines.append("")

        # Snowflake cross-account role (for storage integration)
        lines.append(f"  SnowflakeRole{safe_name}:")
        lines.append("    Type: AWS::IAM::Role")
        lines.append("    Properties:")
        lines.append(f"      RoleName: {naming['lambda_prefix']}-{db_lower}-snowflake-role")
        lines.append("      AssumeRolePolicyDocument:")
        lines.append("        Version: '2012-10-17'")
        lines.append("        Statement:")
        lines.append("          - Effect: Allow")
        lines.append("            Principal:")
        lines.append(f"              AWS: !Ref SnowflakeIamArn{safe_name}")
        lines.append("            Action: sts:AssumeRole")
        lines.append("            Condition:")
        lines.append("              StringEquals:")
        lines.append(f"                sts:ExternalId: !Ref SnowflakeExternalId{safe_name}")
        lines.append("      Policies:")
        lines.append(f"        - PolicyName: snowflake-s3-{db_lower}")
        lines.append("          PolicyDocument:")
        lines.append("            Version: '2012-10-17'")
        lines.append("            Statement:")
        lines.append("              - Effect: Allow")
        lines.append("                Action:")
        lines.append("                  - s3:GetObject")
        lines.append("                  - s3:GetObjectVersion")
        lines.append("                  - s3:ListBucket")
        lines.append("                  - s3:GetBucketLocation")
        lines.append("                Resource:")
        lines.append(f"                  - !Sub 'arn:aws:s3:::${{S3BucketName}}/{prefix}*'")
        lines.append(f"                  - !Sub 'arn:aws:s3:::${{S3BucketName}}'")
        lines.append("")

        # Secrets Manager secret
        lines.append(f"  Secret{safe_name}:")
        lines.append("    Type: AWS::SecretsManager::Secret")
        lines.append("    Properties:")
        lines.append(f"      Name: etl/{db_name.lower()}/config")
        lines.append(f"      Description: Snowflake credentials for {db_name} loader")
        lines.append("      SecretString: '{\"snowflake_account\": \"PLACEHOLDER\", \"snowflake_user\": \"PLACEHOLDER\"}'")
        lines.append("")

        # Lambda function
        lines.append(f"  LoaderFunction{safe_name}:")
        lines.append("    Type: AWS::Lambda::Function")
        lines.append("    Properties:")
        lines.append(f"      FunctionName: {naming['lambda_prefix']}-{db_lower}-loader")
        lines.append(f"      Runtime: {lam['runtime']}")
        lines.append("      Handler: handler.lambda_handler")
        lines.append(f"      Timeout: {lam['timeout']}")
        lines.append(f"      MemorySize: {lam['memory_mb']}")
        lines.append(f"      Role: !GetAtt LambdaRole{safe_name}.Arn")
        lines.append("      Environment:")
        lines.append("        Variables:")
        lines.append(f"          SECRET_NAME: etl/{db_name.lower()}/config")
        lines.append(f"          S3_PREFIX: '{prefix}'")
        lines.append("          LOG_LEVEL: INFO")
        lines.append(f"          SNS_TOPIC_ARN: !Ref NotificationTopic{safe_name}")
        lines.append("      Code:")
        lines.append("        S3Bucket: !Ref CodeBucketName")
        lines.append("        S3Key: loader.zip")
        if tags:
            lines.append("      Tags:")
            for k, v in tags.items():
                lines.append(f"        - Key: {k}")
                lines.append(f"          Value: '{v}'")
        lines.append("")

        # S3 event permission
        lines.append(f"  S3InvokePermission{safe_name}:")
        lines.append("    Type: AWS::Lambda::Permission")
        lines.append("    Properties:")
        lines.append(f"      FunctionName: !Ref LoaderFunction{safe_name}")
        lines.append("      Action: lambda:InvokeFunction")
        lines.append("      Principal: s3.amazonaws.com")
        lines.append("      SourceArn: !Ref S3BucketArn")
        lines.append("")

        # SNS Notification Topic
        lines.append(f"  NotificationTopic{safe_name}:")
        lines.append("    Type: AWS::SNS::Topic")
        lines.append("    Properties:")
        lines.append(f"      TopicName: {naming['lambda_prefix']}-notifications-{db_lower}")
        lines.append(f"      DisplayName: 'ETL Pipeline - {db_name}'")
        if tags:
            lines.append("      Tags:")
            for k, v in tags.items():
                lines.append(f"        - Key: {k}")
                lines.append(f"          Value: '{v}'")
        lines.append("")

        # Email subscription if configured
        notif_config = config.get("notifications", {})
        if notif_config.get("email"):
            lines.append(f"  EmailSubscription{safe_name}:")
            lines.append("    Type: AWS::SNS::Subscription")
            lines.append("    Properties:")
            lines.append(f"      TopicArn: !Ref NotificationTopic{safe_name}")
            lines.append("      Protocol: email")
            lines.append(f"      Endpoint: '{notif_config['email']}'")
            lines.append("")

    # Note about S3 notifications
    lines.append("  # ─── S3 Event Notifications ───────────────────────────────")
    lines.append("  # S3 bucket notification configuration must be added separately")
    lines.append("  # because the bucket likely already exists. Use the AWS CLI:")
    lines.append("  #")
    for db in config["databases"]:
        db_name = db["name"].upper()
        db_lower = db_name.lower().replace("_", "-")
        prefix = db["s3_prefix"]
        safe_name = db_name.replace("_", "")
        lines.append(f"  # aws s3api put-bucket-notification-configuration \\")
        lines.append(f"  #   --bucket {aws['s3_bucket']} \\")
        lines.append(f"  #   --notification-configuration file://s3-notifications.json")
        lines.append(f"  #")
    lines.append("")

    # --- Parameters for code bucket ---
    # Add to parameters section
    param_insert = [
        "  CodeBucketName:",
        "    Type: String",
        "    Description: S3 bucket containing Lambda deployment packages",
        "",
    ]

    # Insert after Parameters:
    param_idx = lines.index("Parameters:") + 1
    for i, p in enumerate(param_insert):
        lines.insert(param_idx + i, p)

    # --- Outputs ---
    lines.append("Outputs:")
    for db in config["databases"]:
        db_name = db["name"].upper()
        safe_name = db_name.replace("_", "")
        lines.append(f"  LoaderArn{safe_name}:")
        lines.append(f"    Value: !GetAtt LoaderFunction{safe_name}.Arn")
        lines.append(f"    Description: ARN of {db_name} loader Lambda")
        lines.append(f"  SnowflakeRoleArn{safe_name}:")
        lines.append(f"    Value: !GetAtt SnowflakeRole{safe_name}.Arn")
        lines.append(f"    Description: >-")
        lines.append(f"      ARN for Snowflake storage integration {db_name}.")
        lines.append(f"      Use in STORAGE_AWS_ROLE_ARN when creating the integration.")
        lines.append(f"  NotificationTopicArn{safe_name}:")
        lines.append(f"    Value: !Ref NotificationTopic{safe_name}")
        lines.append(f"    Description: SNS topic ARN for {db_name} pipeline notifications")
    lines.append("")

    return "\n".join(lines)


def generate_secrets_bootstrap(config: dict, keys: dict) -> str:
    """Generate script to seed Secrets Manager with credentials."""
    sf = config["snowflake"]
    lines = []
    lines.append("#!/bin/bash")
    lines.append("# Seed Secrets Manager with Snowflake credentials")
    lines.append("# Run this after CloudFormation deploys (creates the secret shells)")
    lines.append("#")
    lines.append("# This script updates the placeholder secrets with real credentials.")
    lines.append("# The private keys were generated during setup — DO NOT commit them.")
    lines.append("")
    lines.append("set -euo pipefail")
    lines.append("")

    for db in config["databases"]:
        db_name = db["name"]
        user = db["service_user"]
        key_data = keys.get(user, {})
        private_path = key_data.get("private_path", f"output/keys/{user.lower()}_private.pem")

        lines.append(f"echo '📦 Updating secret for {db_name}...'")
        lines.append(f"PRIVATE_KEY=$(cat {private_path})")
        lines.append("")
        lines.append(f"aws secretsmanager update-secret \\")
        lines.append(f"  --secret-id etl/{db_name.lower()}/config \\")
        lines.append(f"  --secret-string \"$(cat <<EOF")
        lines.append("{")
        lines.append(f'    "snowflake_account": "{sf["account"]}",')
        lines.append(f'    "snowflake_user": "{user}",')
        lines.append(f'    "snowflake_private_key": "$PRIVATE_KEY",')
        lines.append(f'    "snowflake_role": "{db["role"]}",')
        lines.append(f'    "snowflake_warehouse": "{sf["warehouse"]}",')
        lines.append(f'    "snowflake_database": "{db_name}",')
        lines.append(f'    "snowflake_schema": "{db["schema"]}",')
        lines.append(f'    "snowflake_stage": "{config["naming"]["stage_name"]}",')
        lines.append(f'    "admin_database": "{sf["admin_database"]}",')
        lines.append(f'    "admin_schema": "{sf["admin_schema"]}"')
        lines.append("}")
        lines.append('EOF')
        lines.append(')"')
        lines.append("")

    lines.append("echo '✅ All secrets updated.'")
    lines.append("echo ''")
    lines.append("echo '⚠️  Next steps:'")
    lines.append("echo '  1. Run the Snowflake setup SQL (output/snowflake-setup.sql)'")
    lines.append("echo '  2. Run DESC INTEGRATION for each integration'")
    lines.append("echo '  3. Update CloudFormation parameters with Snowflake IAM ARN + External ID'")
    lines.append("echo '  4. Configure S3 event notifications'")

    return "\n".join(lines)


def generate_teardown_sql(config: dict) -> str:
    """Generate Snowflake teardown SQL."""
    sf = config["snowflake"]
    naming = config["naming"]
    lines = []
    lines.append("-- =============================================================")
    lines.append("-- S3 → Snowflake Loader: TEARDOWN")
    lines.append("-- ")
    lines.append("-- ⚠️  This removes ALL objects created by the setup script.")
    lines.append("-- Run as ACCOUNTADMIN. Review carefully before executing.")
    lines.append("-- =============================================================")
    lines.append("")

    for db in config["databases"]:
        db_name = db["name"].upper()
        schema = db["schema"].upper()
        user = db["service_user"]
        role = db["role"]
        int_name = f"{naming['integration_prefix']}_{db_name}"
        stage_name = naming["stage_name"]

        lines.append(f"-- ─── {db_name} ───")
        lines.append(f"DROP STAGE IF EXISTS {db_name}.{schema}.{stage_name};")
        lines.append(f"DROP STORAGE INTEGRATION IF EXISTS {int_name};")
        lines.append(f"DROP USER IF EXISTS {user};")
        lines.append(f"DROP ROLE IF EXISTS {role};")
        lines.append(f"-- DROP SCHEMA IF EXISTS {db_name}.{schema};  -- Uncomment if safe")
        lines.append("")

    lines.append(f"-- ─── Admin Database ───")
    lines.append(f"DROP TABLE IF EXISTS {sf['admin_database']}.{sf['admin_schema']}.CONTROL_TABLE;")
    lines.append(f"DROP TABLE IF EXISTS {sf['admin_database']}.{sf['admin_schema']}.NOTIFICATIONS;")
    lines.append(f"DROP TABLE IF EXISTS {sf['admin_database']}.{sf['admin_schema']}.LOAD_ERRORS;")
    lines.append(f"DROP TABLE IF EXISTS {sf['admin_database']}.{sf['admin_schema']}.LOAD_HISTORY;")
    lines.append(f"DROP SCHEMA IF EXISTS {sf['admin_database']}.{sf['admin_schema']};")
    lines.append(f"-- DROP DATABASE IF EXISTS {sf['admin_database']};  -- Uncomment if safe")

    return "\n".join(lines)


def generate_s3_notifications(config: dict) -> str:
    """Generate S3 notification configuration JSON."""
    import json as json_mod

    naming = config["naming"]
    notifications = {"LambdaFunctionConfigurations": []}

    for db in config["databases"]:
        db_name = db["name"].upper()
        db_lower = db_name.lower().replace("_", "-")
        prefix = db["s3_prefix"]

        notifications["LambdaFunctionConfigurations"].append({
            "Id": f"{naming['lambda_prefix']}-{db_lower}-trigger",
            "LambdaFunctionArn": f"<ARN of {naming['lambda_prefix']}-{db_lower}-loader>",
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {
                "Key": {
                    "FilterRules": [
                        {"Name": "prefix", "Value": prefix}
                    ]
                }
            }
        })

    return json_mod.dumps(notifications, indent=2)


def main():
    print("🔧 S3 → Snowflake Loader — Generating Deployment Artifacts")
    print("")

    config = load_config()
    print(f"  ✅ Loaded config: {len(config['databases'])} database(s)")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Generate RSA keys
    print("\n📎 Generating RSA key pairs...")
    keys = generate_rsa_keys(config)

    # Generate Snowflake SQL
    print("\n📄 Generating Snowflake setup SQL...")
    sql = generate_snowflake_sql(config, keys)
    (OUTPUT_DIR / "snowflake-setup.sql").write_text(sql)
    print(f"  → output/snowflake-setup.sql")

    # Generate CloudFormation
    print("\n☁️  Generating CloudFormation template...")
    cfn = generate_cloudformation(config)
    (OUTPUT_DIR / "infrastructure.yaml").write_text(cfn)
    print(f"  → output/infrastructure.yaml")

    # Generate secrets bootstrap
    print("\n🔐 Generating secrets bootstrap script...")
    bootstrap = generate_secrets_bootstrap(config, keys)
    bootstrap_path = OUTPUT_DIR / "secrets-bootstrap.sh"
    bootstrap_path.write_text(bootstrap)
    os.chmod(bootstrap_path, 0o755)
    print(f"  → output/secrets-bootstrap.sh")

    # Generate teardown SQL
    print("\n🗑️  Generating teardown SQL...")
    teardown = generate_teardown_sql(config)
    (OUTPUT_DIR / "teardown.sql").write_text(teardown)
    print(f"  → output/teardown.sql")

    # Generate S3 notification config
    print("\n📨 Generating S3 notification config...")
    notif = generate_s3_notifications(config)
    (OUTPUT_DIR / "s3-notifications.json").write_text(notif)
    print(f"  → output/s3-notifications.json")

    print("\n" + "=" * 60)
    print("✅ All artifacts generated in output/")
    print("")
    print("Next steps:")
    print("  1. Review output/snowflake-setup.sql")
    print("     → Give to your Snowflake admin to run")
    print("  2. Package Lambda code:")
    print("     cd lambda/loader && pip install -r requirements.txt -t package/")
    print("     cd package && zip -r ../../output/loader.zip . && cd ..")
    print("     zip -g ../output/loader.zip *.py")
    print("  3. Upload loader.zip to your code S3 bucket")
    print("  4. Deploy output/infrastructure.yaml via CloudFormation")
    print("  5. Run output/secrets-bootstrap.sh to seed credentials")
    print("  6. Run DESC INTEGRATION in Snowflake, update CF params")
    print("  7. Configure S3 notifications (output/s3-notifications.json)")
    print("  8. Test with a sample file!")


if __name__ == "__main__":
    main()
