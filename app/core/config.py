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

    SRI_HEADLESS: bool = False
    SRI_BROWSER_CHANNEL: str = "chrome"
    SRI_USER_DATA_DIR: str = "D:/Aplicaciones/Conta/sri_profiles"
    SRI_CHROME_PATH: str = ""
    SRI_CDP_PORT: int = 9222
    SRI_NAVIGATION_TIMEOUT_MS: int = 45000
    SRI_WORKER_POLL_SECONDS: int = 3
    # Cantidad de tareas simultáneas por instancia del worker.
    # Para una arquitectura de 5 PCs, usar 1 en cada PC.
    SRI_WORKER_CONCURRENCY: int = 1
    SRI_WORKER_USER: str = ""
    SRI_WORKER_HEARTBEAT_SECONDS: int = 10
    BUG_REPORT_EMAIL: str = "eldudyaguirre@gmail.com"
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_USE_TLS: bool = True

    # Token interno compartido con el administrador web de TotalCounts.
    # No se expone al navegador.
    TOTALCOUNTS_INTERNAL_TOKEN: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = Settings()
