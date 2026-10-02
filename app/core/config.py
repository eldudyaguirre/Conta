from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    APP_NAME: str = "Conta"
    APP_VERSION: str = "0.1.0"

    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 2408

    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "Conta"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()