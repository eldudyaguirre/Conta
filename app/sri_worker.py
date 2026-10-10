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
import traceback
import json
from urllib import error as url_error, request as url_request

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
    token = settings.TOTALCOUNTS_INTERNAL_TOKEN.strip()
    base_url = os.getenv("TOTALCOUNTS_URL", "https://totalcounts.com.ec").rstrip("/")
    if not token:
        logger.warning("Alerta no enviada: TOTALCOUNTS_INTERNAL_TOKEN no está configurado.")
        return
    clave = f"{worker_id}|{type(exc).__name__}|{str(exc)}"
    ahora = time.monotonic()
    if ahora - _ULTIMOS_ALERTAS.get(clave, 0.0) < _ALERT_COOLDOWN_SECONDS:
        return
    _ULTIMOS_ALERTAS[clave] = ahora
    trabajo = trabajo or {}
    payload = {
        "worker": worker_id,
        "usuario": usuario,
        "equipo": socket.gethostname(),
        "job_id": trabajo.get("job_id", "-"),
        "ruc": trabajo.get("ruc", "-"),
        "anio": trabajo.get("anio", "-"),
        "mes": trabajo.get("mes", "-"),
        "tipo_comprobante": trabajo.get("tipo_comprobante", "-"),
        "operacion": trabajo.get("operacion", "-"),
        "error_tipo": type(exc).__name__,
        "error_mensaje": str(exc),
        "traceback": traceback.format_exc(),
    }
    req = url_request.Request(
        f"{base_url}/internal/conta/worker-error/",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "X-TotalCounts-Internal": token},
        method="POST",
    )
    try:
        with url_request.urlopen(req, timeout=20) as response:
            if response.status >= 300:
                raise RuntimeError(f"TtCWeb respondió HTTP {response.status}")
        logger.info("Reporte de error enviado a TtCWeb para job %s.", payload["job_id"])
    except url_error.HTTPError as exc_http:
        logger.error("TtCWeb rechazó el reporte de error: HTTP %s.", exc_http.code)
    except Exception:
        logger.exception("No se pudo enviar el reporte de error a TtCWeb.")

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

            trabajo = SriClienteSyncService.obtener_trabajo_pendiente(
                worker_id, solo_pool=settings.SRI_WORKER_POOL_ONLY
            )
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
