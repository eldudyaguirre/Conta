from __future__ import annotations

import os
import socket
import time

# PostgreSQL 16: registrar explícitamente las DLL nativas antes de importar
# el servicio SRI, que termina cargando psycopg2.
_POSTGRES_BIN = r"C:\Program Files\PostgreSQL\16\bin"
if os.path.isdir(_POSTGRES_BIN):
    os.add_dll_directory(_POSTGRES_BIN)

import asyncio
import logging
import smtplib
import traceback
from email.message import EmailMessage

from app.core.config import settings
from app.services.sri_cliente_sync_service import SriClienteSyncService


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | SRI-WORKER | %(levelname)s | %(message)s",
)
logger = logging.getLogger("conta.sri_worker")

_ALERT_COOLDOWN_SECONDS = 300
_ULTIMOS_ALERTAS: dict[str, float] = {}

def _enviar_alerta_error_worker(worker_id: str, usuario: str, trabajo: dict | None, exc: Exception) -> None:
    smtp_user = settings.SMTP_USER.strip()
    smtp_password = settings.SMTP_PASSWORD.strip()
    destino = settings.BUG_REPORT_EMAIL.strip()
    if not smtp_user or not smtp_password or not destino:
        logger.warning("Alerta por correo no enviada: SMTP no configurado.")
        return
    clave = f"{worker_id}|{type(exc).__name__}|{str(exc)}"
    ahora = time.monotonic()
    if ahora - _ULTIMOS_ALERTAS.get(clave, 0.0) < _ALERT_COOLDOWN_SECONDS:
        return
    _ULTIMOS_ALERTAS[clave] = ahora
    job_id = (trabajo or {}).get("job_id", "-")
    cuerpo = (
        "Conta - ERROR AUTOMÁTICO DEL SRI WORKER\n\n"
        f"Worker: {worker_id}\nUsuario: {usuario}\nEquipo: {socket.gethostname()}\n"
        f"Job ID: {job_id}\nRUC: {(trabajo or {}).get("ruc", "-")}\n"
        f"Año: {(trabajo or {}).get("anio", "-")}\nMes: {(trabajo or {}).get("mes", "-")}\n"
        f"Tipo comprobante: {(trabajo or {}).get("tipo_comprobante", "-")}\n"
        f"Operación: {(trabajo or {}).get("operacion", "-")}\n\n"
        f"Error: {type(exc).__name__}: {exc}\n\nTraceback completo:\n{traceback.format_exc()}"
    )
    mensaje = EmailMessage()
    mensaje["From"] = smtp_user
    mensaje["To"] = destino
    mensaje["Subject"] = f"[Conta] Error SRI Worker | {worker_id} | job {job_id}"
    mensaje.set_content(cuerpo)
    try:
        with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=20) as servidor:
            if settings.SMTP_USE_TLS:
                servidor.starttls()
            servidor.login(smtp_user, smtp_password)
            servidor.send_message(mensaje)
        logger.info("Alerta de error enviada a %s.", destino)
    except Exception:
        logger.exception("No se pudo enviar la alerta de error por correo.")


async def _procesar_trabajo(worker_numero: int) -> None:
    usuario_worker = (settings.SRI_WORKER_USER or os.getenv("USERNAME") or socket.gethostname()).strip()
    worker_id = usuario_worker if max(1, settings.SRI_WORKER_CONCURRENCY) == 1 else f"{usuario_worker}-{worker_numero}"
    SriClienteSyncService.registrar_worker(worker_id, usuario_worker)
    ultimo_heartbeat = 0.0
    logger.info("Worker SRI %s iniciado para usuario %s.", worker_id, usuario_worker)

    while True:
        trabajo = None
        try:
            ahora = time.monotonic()
            if ahora - ultimo_heartbeat >= max(5, settings.SRI_WORKER_HEARTBEAT_SECONDS):
                SriClienteSyncService.registrar_worker(worker_id, usuario_worker)
                ultimo_heartbeat = ahora

            trabajo = SriClienteSyncService.obtener_trabajo_pendiente(worker_id)
            if trabajo:
                SriClienteSyncService._iva_debug_log(
                    "WORKER | trabajo reclamado | worker=%s | job_id=%s | ruc=%s | anio=%s | mes=%s | tipo=%s | operacion=%s",
                    worker_id,
                    trabajo["job_id"], trabajo["ruc"], trabajo["anio"], trabajo["mes"],
                    trabajo["tipo_comprobante"], trabajo.get("operacion") or "compras",
                )
                logger.info(
                    "Worker %s tomó trabajo %s: RUC=%s año=%s mes=%s tipo=%s",
                    worker_id,
                    trabajo["job_id"],
                    trabajo["ruc"],
                    trabajo["anio"],
                    trabajo["mes"],
                    trabajo["tipo_comprobante"],
                )
                await SriClienteSyncService._ejecutar_job(
                    trabajo["job_id"],
                    trabajo["ruc"],
                    int(trabajo["anio"]),
                    int(trabajo["mes"]),
                    int(trabajo["tipo_comprobante"]),
                    str(trabajo.get("operacion") or "compras"),
                )
                logger.info("Worker %s terminó trabajo %s.", worker_id, trabajo["job_id"])
            else:
                await asyncio.sleep(max(1, settings.SRI_WORKER_POLL_SECONDS))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Error en el worker SRI %s.", worker_id)
            _enviar_alerta_error_worker(worker_id, usuario_worker, trabajo, exc)
            if trabajo:
                SriClienteSyncService._job_update(
                    trabajo["job_id"],
                    estado="error",
                    mensaje=f"Error inesperado del worker {worker_id}. Revise el log.",
                )
            await asyncio.sleep(5)


async def main() -> None:
    logger.info("Worker SRI interactivo iniciado.")
    logger.info(
        "Arquitectura distribuida: hasta %s tarea(s) simultánea(s) por PC.",
        max(1, settings.SRI_WORKER_CONCURRENCY),
    )
    logger.info(
        "Esperando trabajos. Cada worker abre su propia sesión Chromium cuando existe una tarea."
    )
    SriClienteSyncService._ensure_jobs_table()

    cantidad = max(1, int(settings.SRI_WORKER_CONCURRENCY))
    tareas = [
        asyncio.create_task(_procesar_trabajo(numero))
        for numero in range(1, cantidad + 1)
    ]

    try:
        await asyncio.gather(*tareas)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Worker SRI detenido por el usuario.")
        for tarea in tareas:
            tarea.cancel()
        await asyncio.gather(*tareas, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
