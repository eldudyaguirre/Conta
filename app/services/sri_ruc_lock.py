from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast

from app.database.connection import engine

logger = logging.getLogger("conta.sri_ruc_lock")

# Namespace independiente de los locks transaccionales de la cola.
_LOCK_NAMESPACE = 2409
_LOCK_WAIT_SECONDS = 900.0
_LOCK_POLL_SECONDS = 0.25

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


def _try_lock(connection: Any, ruc: str) -> bool:
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT pg_try_advisory_lock(%s, hashtext(%s))",
            (_LOCK_NAMESPACE, ruc),
        )
        row = cursor.fetchone()
        return bool(row and row[0])
    finally:
        cursor.close()


def _unlock_and_close(connection: Any, ruc: str, acquired: bool) -> None:
    invalidate = False
    try:
        if acquired:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT pg_advisory_unlock(%s, hashtext(%s))",
                    (_LOCK_NAMESPACE, ruc),
                )
                row = cursor.fetchone()
                if not row or not row[0]:
                    invalidate = True
                    logger.error("No se confirmó la liberación del lock SRI | ruc=%s", ruc)
            except Exception:
                # No devolver al pool una conexión que pudiera conservar el lock.
                invalidate = True
                logger.exception("Error liberando lock SRI; se invalidará la conexión | ruc=%s", ruc)
            finally:
                cursor.close()
    finally:
        try:
            if invalidate:
                connection.invalidate()
        finally:
            # El cierre físico/invalidación también hace que PostgreSQL libere locks.
            connection.close()


def bloquear_sesion_sri_por_ruc(function: F) -> F:
    """Mantiene un lock PostgreSQL durante toda la sesión SRI de un RUC.

    Se usa en los puntos de entrada que abren navegador/perfil SRI. El lock es
    de sesión y usa una conexión dedicada, no la conexión de las operaciones
    normales de la aplicación. Los distintos RUC pueden trabajar en paralelo.
    """
    @functools.wraps(function)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        ruc_value = kwargs.get("ruc")
        if ruc_value is None and len(args) > 1:
            ruc_value = args[1]
        ruc = str(ruc_value or "").strip()
        if not ruc:
            raise ValueError("Se requiere un RUC para abrir una sesión SRI.")

        connection = await asyncio.to_thread(engine.raw_connection)
        acquired = False
        started = time.monotonic()
        try:
            while True:
                acquired = await asyncio.to_thread(_try_lock, connection, ruc)
                if acquired:
                    break
                if time.monotonic() - started >= _LOCK_WAIT_SECONDS:
                    raise TimeoutError(
                        f"Ya existe otra sesión SRI para el RUC {ruc}; "
                        "se agotó el tiempo de espera."
                    )
                await asyncio.sleep(_LOCK_POLL_SECONDS)

            logger.info("Lock de sesión SRI adquirido | ruc=%s", ruc)
            return await function(*args, **kwargs)
        finally:
            await asyncio.shield(
                asyncio.to_thread(_unlock_and_close, connection, ruc, acquired)
            )

    return cast(F, wrapper)
