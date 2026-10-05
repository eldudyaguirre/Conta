import os

# PostgreSQL 16: registrar explícitamente las DLL nativas antes de importar
# SQLAlchemy/psycopg2. Es necesario cuando Conta corre como servicio de Windows.
_POSTGRES_BIN = r"C:\Program Files\PostgreSQL\16\bin"
if os.path.isdir(_POSTGRES_BIN):
    os.add_dll_directory(_POSTGRES_BIN)

from fastapi import FastAPI
from sqlalchemy import text

from app.core.config import settings
from app.database.connection import engine
from app.api.v1.endpoints.clientes import router as clientes_router
from app.api.v1.endpoints.tributario import router as tributario_router
from app.api.v1.endpoints.ai import router as ai_router
from app.api.v1.endpoints.admin import router as admin_router
from app.api.v1.endpoints.admin_sri import router as admin_sri_router
from app.api.v1.endpoints.admin_sri_diagnostico import router as admin_sri_diagnostico_router

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
)

app.include_router(clientes_router, prefix="/api/v1")
app.include_router(tributario_router, prefix="/api/v1")
app.include_router(ai_router, prefix="/api/v1")
app.include_router(admin_router, prefix="/api/v1")
app.include_router(admin_sri_router, prefix="/api/v1")
app.include_router(admin_sri_diagnostico_router, prefix="/api/v1")


@app.get("/")
def root():
    return {
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "ok",
    }


@app.get("/api/v1/health")
def health():
    try:
        with engine.connect() as connection:
            database = connection.execute(
                text("SELECT current_database()")
            ).scalar()

            clientes = connection.execute(
                text("SELECT COUNT(*) FROM clientes")
            ).scalar()

        return {
            "status": "ok",
            "database": database,
            "clientes": clientes,
        }

    except Exception as e:
        return {
            "status": "error",
            "detail": str(e),
        }
