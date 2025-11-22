from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    API_TITLE: str = "Agentics API"
    API_VERSION: str = "0.1.0"
    # Use validation_alias for Pydantic V2 to explicitly map the env var
    API_KEY: str = Field(validation_alias="AGENTICS_API_KEY")
    SESSION_EXPIRY_SECONDS: int = 3600

    REDIS_URL: str | None = None

    # V2 config style
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
