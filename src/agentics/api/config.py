from enum import Enum
from typing import List, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    LOCAL = "local"
    PRODUCTION = "production"


class StorageBackend(str, Enum):
    LOCAL = "local"
    S3 = "s3"


class Settings(BaseSettings):
    # Metadata
    API_TITLE: str = "Agentics API"
    API_VERSION: str = "0.1.0"
    ENVIRONMENT: Environment = Environment.LOCAL

    # Security
    API_KEY: str = Field(validation_alias="AGENTICS_API_KEY")
    SESSION_EXPIRY_SECONDS: int = 3600
    MAX_CONCURRENT_EXECUTIONS: int = 5
    DEFAULT_RATE_LIMIT: str = "50/minute"

    # CORS (Cross-Origin Resource Sharing)
    # In production, this should be a list of allowed frontend domains
    CORS_ORIGINS: List[str] = ["*"]

    # Storage Configuration
    STORAGE_BACKEND: StorageBackend = StorageBackend.LOCAL

    TEMP_FILE_PATH: str = "src/agentics/api/temp_files"

    # AWS Credentials (Optional if using LOCAL storage)
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_REGION: str = "us-east-1"
    AWS_BUCKET_NAME: Optional[str] = None

    # Text2SQL Data Paths
    # Defaults point to local repo structure.
    # In Production/Docker, these will be overridden to point to mounted Volumes.
    SQL_BENCHMARKS_FOLDER: str = (
        "src/agentics/api/applications/text2sql/data/benchmarks"
    )
    SQL_DB_PATH: str = "src/agentics/api/applications/text2sql/data/databases"

    # Future: Redis for caching/state
    REDIS_URL: Optional[str] = None

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
