"""
Load configuration from AWS Secrets Manager.

Each database has its own secret at: etl/<database_name>/config
Secrets are cached for Lambda warm starts to avoid repeated API calls.
"""

import json
import logging
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from snowflake_client import SnowflakeConfig

logger = logging.getLogger(__name__)

# Module-level cache for warm Lambda starts
_cached_config: Optional[SnowflakeConfig] = None
_cached_secret_version: Optional[str] = None


def load_config(secret_name: Optional[str] = None) -> SnowflakeConfig:
    """
    Load Snowflake configuration from Secrets Manager.
    
    Secret name comes from Lambda environment variable SECRET_NAME,
    or can be passed explicitly for testing.
    
    Caches the config for Lambda warm starts.
    """
    global _cached_config, _cached_secret_version

    if secret_name is None:
        secret_name = os.environ.get("SECRET_NAME")
        if not secret_name:
            raise ValueError(
                "SECRET_NAME environment variable not set. "
                "This should be configured in the Lambda's environment."
            )

    # Check cache
    if _cached_config is not None:
        logger.debug(f"Using cached config for {secret_name}")
        return _cached_config

    # Fetch from Secrets Manager
    client = boto3.client("secretsmanager")

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "ResourceNotFoundException":
            raise ValueError(
                f"Secret '{secret_name}' not found in Secrets Manager. "
                f"Run the bootstrap script to create it."
            ) from e
        elif error_code == "AccessDeniedException":
            raise ValueError(
                f"Lambda doesn't have permission to read secret '{secret_name}'. "
                f"Check the IAM role's Secrets Manager policy."
            ) from e
        raise

    secret_data = json.loads(response["SecretString"])
    _cached_secret_version = response.get("VersionId")

    # Build config
    _cached_config = SnowflakeConfig(
        account=secret_data["snowflake_account"],
        user=secret_data["snowflake_user"],
        private_key_pem=secret_data["snowflake_private_key"],
        role=secret_data["snowflake_role"],
        warehouse=secret_data["snowflake_warehouse"],
        database=secret_data["snowflake_database"],
        schema=secret_data["snowflake_schema"],
        stage=secret_data["snowflake_stage"],
        admin_database=secret_data["admin_database"],
        admin_schema=secret_data["admin_schema"],
    )

    logger.info(
        f"Loaded config from {secret_name}: "
        f"account={_cached_config.account} db={_cached_config.database}"
    )

    return _cached_config


def invalidate_cache() -> None:
    """Force re-read from Secrets Manager on next call."""
    global _cached_config, _cached_secret_version
    _cached_config = None
    _cached_secret_version = None
    logger.info("Config cache invalidated")
