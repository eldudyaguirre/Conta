from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    APP_NAME: str = "Conta"
    APP_VERSION: str = "0.1.0"

    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 2408

    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "BdTotal"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = ""

    AI_PROVIDER: str = "gemini"
    OPENAI_API_KEY: str | None = None
    OPENAI_MODEL: str = "gpt-6-luna"
    GEMINI_API_KEY: str | None = None
    GEMINI_MODEL: str = "gemini-3.5-flash-lite"

    AI_ENABLED: bool = True
    AI_MAX_TOOL_CALLS: int = 5
    AI_MAX_HISTORY_MESSAGES: int = 20

    SRI_HEADLESS: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()
