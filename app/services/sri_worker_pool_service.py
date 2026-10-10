from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text

from app.database.connection import engine
from app.services.sri_cliente_sync_service import SriClienteSyncService


class SriWorkerPoolService:
    """Operaciones controladas para probar el pool de workers SRI."""

    @classmethod
    def _ensure_pool_schema(cls) -> None:
        SriClienteSyncService._ensure_jobs_table()
        with engine.begin() as db:
            db.execute(text("""
                ALTER TABLE conta_sri_jobs
                ADD COLUMN IF NOT EXISTS grupo_id VARCHAR(64)
            """))
            db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_conta_sri_jobs_grupo
                ON conta_sri_jobs(grupo_id, creado)
            """))

    @classmethod
    def iniciar_prueba(
        cls,
        *,
        anio: int,
        mes: int,
        cantidad: int = 5,
        tipo_comprobante: int = 1,
        operacion: str = "compras",
    ) -> dict[str, Any]:
        """Crea un grupo de trabajos para clientes activos con credenciales.

        No toma clientes que ya tengan un trabajo equivalente pendiente,
        ejecutándose o en CAPTCHA. No modifica la configuración de concurrencia:
        el número de workers se controla por SRI_WORKER_CONCURRENCY.
        """
        cls._ensure_pool_schema()
        grupo_id = uuid.uuid4().hex
        tipo = SriClienteSyncService._tipo(tipo_comprobante)

        with engine.begin() as db:
            clientes = db.execute(text("""
                SELECT
                    c.ruccedcli::text AS ruc,
                    COALESCE(c.nomclient, '') AS cliente
                FROM clientes c
                WHERE COALESCE(c.activo, FALSE) = TRUE
                  AND NULLIF(TRIM(COALESCE(c.clavesri, '')), '') IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM conta_sri_jobs j
                      WHERE TRIM(j.ruc) = TRIM(c.ruccedcli::text)
                        AND j.anio = :anio
                        AND j.mes = :mes
                        AND j.tipo_comprobante = :tipo
                        AND j.operacion = :operacion
                        AND j.estado IN ('pendiente', 'ejecutando', 'captcha')
                  )
                ORDER BY c.ruccedcli
                LIMIT :cantidad
                FOR UPDATE OF c SKIP LOCKED
            """), {
                "anio": anio,
                "mes": mes,
                "tipo": tipo,
                "operacion": operacion,
                "cantidad": cantidad,
            }).mappings().all()

            if not clientes:
                raise ValueError(
                    "No hay clientes activos con clave SRI disponibles para la prueba. "
                    "Puede que ya tengan sincronizaciones equivalentes en curso."
                )

            trabajos: list[dict[str, Any]] = []
            for cliente in clientes:
                job_id = uuid.uuid4().hex
                db.execute(text("""
                    INSERT INTO conta_sri_jobs (
                        job_id, grupo_id, estado, ruc, cliente, anio, mes,
                        tipo_comprobante, operacion, mensaje
                    )
                    VALUES (
                        :job_id, :grupo_id, 'pendiente', :ruc, :cliente, :anio, :mes,
                        :tipo, :operacion,
                        'Prueba de worker pool en cola; esperando un worker disponible.'
                    )
                """), {
                    "job_id": job_id,
                    "grupo_id": grupo_id,
                    "ruc": str(cliente["ruc"]).strip(),
                    "cliente": str(cliente["cliente"] or "").strip(),
                    "anio": anio,
                    "mes": mes,
                    "tipo": tipo,
                    "operacion": operacion,
                })
                trabajos.append({
                    "job_id": job_id,
                    "ruc": str(cliente["ruc"]).strip(),
                    "cliente": str(cliente["cliente"] or "").strip(),
                    "estado": "pendiente",
                })

        return {
            "grupo_id": grupo_id,
            "cantidad_creada": len(trabajos),
            "anio": anio,
            "mes": mes,
            "tipo_comprobante": tipo,
            "operacion": operacion,
            "trabajos": trabajos,
        }

    @classmethod
    def estado_prueba(cls, grupo_id: str) -> dict[str, Any] | None:
        cls._ensure_pool_schema()
        with engine.connect() as db:
            rows = db.execute(text("""
                SELECT
                    job_id, grupo_id, estado, ruc, cliente, anio, mes,
                    tipo_comprobante, operacion, worker, sri, ya_existentes,
                    descargadas, guardadas, errores, paginas, mensaje, detalle,
                    creado, actualizado
                FROM conta_sri_jobs
                WHERE grupo_id = :grupo_id
                ORDER BY creado, ruc
            """), {"grupo_id": grupo_id}).mappings().all()

        if not rows:
            return None

        trabajos = []
        resumen: dict[str, int] = {}
        for row in rows:
            item = dict(row)
            for campo in ("creado", "actualizado"):
                if item.get(campo):
                    item[campo] = item[campo].isoformat()
            estado = str(item.get("estado") or "desconocido")
            resumen[estado] = resumen.get(estado, 0) + 1
            trabajos.append(item)

        return {
            "grupo_id": grupo_id,
            "total": len(trabajos),
            "resumen": resumen,
            "terminados": sum(
                resumen.get(estado, 0)
                for estado in ("finalizado", "error", "cancelado")
            ),
            "trabajos": trabajos,
        }

    @classmethod
    def estado_workers(cls) -> dict[str, Any]:
        cls._ensure_pool_schema()
        with engine.connect() as db:
            rows = db.execute(text("""
                SELECT
                    w.worker_id, w.usuario, w.equipo, w.estado,
                    w.job_actual, w.ultimo_heartbeat,
                    j.ruc AS ruc_actual, j.cliente AS cliente_actual,
                    j.estado AS estado_trabajo, j.mensaje AS mensaje_trabajo
                FROM conta_sri_workers w
                LEFT JOIN conta_sri_jobs j ON j.job_id = w.job_actual
                ORDER BY w.equipo, w.worker_id
            """)).mappings().all()

        workers = []
        for row in rows:
            item = dict(row)
            if item.get("ultimo_heartbeat"):
                item["ultimo_heartbeat"] = item["ultimo_heartbeat"].isoformat()
            workers.append(item)

        return {
            "total": len(workers),
            "activos": sum(1 for w in workers if w.get("estado") == "activo"),
            "workers": workers,
        }
