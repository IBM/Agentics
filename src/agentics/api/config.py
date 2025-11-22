from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    API_TITLE: str = "Agentics API"
    API_VERSION: str = "0.1.0"
    API_KEY: str = Field(validation_alias="AGENTICS_API_KEY")

    SESSION_EXPIRY_SECONDS: int = 3600
    MAX_CONCURRENT_EXECUTIONS: int = (
        5  # Max number of simultaneous AG pipelines globally
    )
    DEFAULT_RATE_LIMIT: str = "50/minute"  # Rate limit per IP

    REDIS_URL: str | None = None

    # V2 config style
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
