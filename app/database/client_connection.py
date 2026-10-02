import re

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import settings


_engines: dict[str, Engine] = {}
_sessionmakers: dict[str, sessionmaker] = {}


def validar_ruc(ruc: str) -> str:
    """
    Valida que el nombre de la base corresponda a un RUC.
    """
    ruc = str(ruc).strip()

    if not re.fullmatch(r"\d{13}", ruc):
        raise ValueError("El RUC debe contener exactamente 13 dígitos.")

    return ruc


def obtener_engine_cliente(ruc: str) -> Engine:
    """
    Obtiene o crea el Engine correspondiente a la base del cliente.
    La base de datos tiene como nombre el RUC.
    """

    ruc = validar_ruc(ruc)

    if ruc in _engines:
        return _engines[ruc]

    database_url = (
        f"postgresql+psycopg2://"
        f"{settings.DB_USER}:{settings.DB_PASSWORD}"
        f"@{settings.DB_HOST}:{settings.DB_PORT}"
        f"/{ruc}"
    )

    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_size=5,
        max_overflow=10,
    )

    _engines[ruc] = engine

    return engine


def obtener_session_cliente(ruc: str) -> Session:
    """
    Devuelve una sesión SQLAlchemy para la base del cliente.
    """

    ruc = validar_ruc(ruc)

    if ruc not in _sessionmakers:
        engine = obtener_engine_cliente(ruc)

        _sessionmakers[ruc] = sessionmaker(
            bind=engine,
            autocommit=False,
            autoflush=False,
        )

    return _sessionmakers[ruc]()


def probar_conexion_cliente(ruc: str):
    """
    Comprueba que la base del cliente exista y sea accesible.
    """

    ruc = validar_ruc(ruc)
    engine = obtener_engine_cliente(ruc)

    with engine.connect() as connection:

        database = connection.execute(
            text("SELECT current_database()")
        ).scalar()

        version = connection.execute(
            text("SELECT version()")
        ).scalar()

    return {
        "conexion": "ok",
        "base_datos": database,
        "version": version,
    }