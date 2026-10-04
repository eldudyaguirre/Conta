from __future__ import annotations

import asyncio
import logging

from app.core.config import settings
from app.services.sri_cliente_sync_service import SriClienteSyncService


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | SRI-WORKER | %(levelname)s | %(message)s",
)
logger = logging.getLogger("conta.sri_worker")


async def main() -> None:
    logger.info("Worker SRI interactivo iniciado.")
    logger.info("Esperando trabajos. Chromium se abrirá solamente cuando exista una sincronización.")
    SriClienteSyncService._ensure_jobs_table()

    while True:
        trabajo = None
        try:
            trabajo = SriClienteSyncService.obtener_trabajo_pendiente()
            if trabajo:
                logger.info(
                    "Trabajo %s reclamado: RUC=%s año=%s mes=%s tipo=%s",
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
                )
                logger.info("Trabajo %s terminado.", trabajo["job_id"])
            else:
                await asyncio.sleep(max(1, settings.SRI_WORKER_POLL_SECONDS))
        except KeyboardInterrupt:
            logger.info("Worker SRI detenido por el usuario.")
            return
        except Exception:
            logger.exception("Error en el ciclo del worker SRI.")
            if trabajo:
                SriClienteSyncService._job_update(
                    trabajo["job_id"],
                    estado="error",
                    mensaje="Error inesperado del worker SRI. Revise el log.",
                )
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
