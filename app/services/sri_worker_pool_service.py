from __future__ import annotations

import re
import uuid
from typing import Any

from sqlalchemy import text

from app.database.connection import engine
from app.services.sri_cliente_sync_service import SriClienteSyncService


class SriWorkerPoolService:
    """Operaciones controladas para probar el pool SRI con RUC explícitos."""

    ESTADOS_ACTIVOS = ("pendiente", "ejecutando", "captcha")

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

    @staticmethod
    def _validar_rucs(rucs: list[str]) -> list[str]:
        normalizados = [str(ruc).strip() for ruc in rucs]
        if not 1 <= len(normalizados) <= 5:
            raise ValueError("Selecciona entre 1 y 5 RUC para la prueba.")
        if any(not re.fullmatch(r"\\d{13}", ruc) for ruc in normalizados):
            raise ValueError("Todos los RUC deben tener exactamente 13 dígitos.")
        if len(set(normalizados)) != len(normalizados):
            raise ValueError("La lista contiene RUC repetidos.")
        return normalizados

    @classmethod
    def _consultar_clientes(
        cls,
        db,
        rucs: list[str],
        *,
        anio: int,
        mes: int,
        tipo: str,
        operacion: str,
    ) -> list[dict[str, Any]]:
        placeholders = ", ".join(f":ruc_{i}" for i in range(len(rucs)))
        params = {
            f"ruc_{i}": ruc for i, ruc in enumerate(rucs)
        }
        params.update({
            "anio": anio,
            "mes": mes,
            "tipo": tipo,
            "operacion": operacion,
        })
        rows = db.execute(text(f"""
            SELECT
                c.ruccedcli::text AS ruc,
                COALESCE(c.nomclient, '') AS cliente,
                COALESCE(c.activo, FALSE) AS activo,
                (NULLIF(TRIM(COALESCE(c.clavesri, '')), '') IS NOT NULL) AS tiene_clave,
                EXISTS (
                    SELECT 1
                    FROM conta_sri_jobs j
                    WHERE TRIM(j.ruc) = TRIM(c.ruccedcli::text)
                      AND j.anio = :anio
                      AND j.mes = :mes
                      AND j.tipo_comprobante = :tipo
                      AND j.operacion = :operacion
                      AND j.estado IN ('pendiente', 'ejecutando', 'captcha')
                ) AS trabajo_en_curso
            FROM clientes c
            WHERE TRIM(c.ruccedcli::text) IN ({placeholders})
            ORDER BY c.ruccedcli
        """), params).mappings().all()

        por_ruc = {str(row["ruc"]).strip(): dict(row) for row in rows}
        resultado = []
        for ruc in rucs:
            row = por_ruc.get(ruc)
            if row is None:
                resultado.append({
                    "ruc": ruc,
                    "cliente": "",
                    "disponible": False,
                    "motivo": "RUC no encontrado en la tabla clientes.",
                })
                continue

            if not row["activo"]:
                motivo = "Cliente inactivo."
            elif not row["tiene_clave"]:
                motivo = "El cliente no tiene clave SRI configurada."
            elif row["trabajo_en_curso"]:
                motivo = "Ya existe una sincronización equivalente en curso."
            else:
                motivo = "Disponible para la prueba."

            resultado.append({
                "ruc": ruc,
                "cliente": str(row["cliente"] or "").strip(),
                "disponible": motivo == "Disponible para la prueba.",
                "motivo": motivo,
            })
        return resultado

    @classmethod
    def previsualizar(
        cls,
        *,
        rucs: list[str],
        anio: int,
        mes: int,
        tipo_comprobante: int = 1,
        operacion: str = "compras",
    ) -> dict[str, Any]:
        """Valida una selección explícita sin encolar ni ejecutar trabajos."""
        cls._ensure_pool_schema()
        rucs = cls._validar_rucs(rucs)
        tipo = SriClienteSyncService._tipo(tipo_comprobante)
        with engine.connect() as db:
            clientes = cls._consultar_clientes(
                db, rucs, anio=anio, mes=mes, tipo=tipo, operacion=operacion
            )
        disponibles = sum(1 for c in clientes if c["disponible"])
        return {
            "modo": "previsualizacion",
            "sin_trabajos_creados": True,
            "cantidad_solicitada": len(rucs),
            "cantidad_disponible": disponibles,
            "anio": anio,
            "mes": mes,
            "tipo_comprobante": tipo,
            "operacion": operacion,
            "clientes": clientes,
        }

    @classmethod
    def iniciar_prueba(
        cls,
        *,
        rucs: list[str],
        anio: int,
        mes: int,
        tipo_comprobante: int = 1,
        operacion: str = "compras",
    ) -> dict[str, Any]:
        """Encola trabajos reales solo para los RUC explícitamente seleccionados."""
        cls._ensure_pool_schema()
        rucs = cls._validar_rucs(rucs)
        grupo_id = uuid.uuid4().hex
        tipo = SriClienteSyncService._tipo(tipo_comprobante)

        with engine.begin() as db:
            # Serializa solicitudes concurrentes para los mismos RUC durante
            # la validación y creación de los trabajos.
            for ruc in sorted(rucs):
                db.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:clave))"),
                    {"clave": f"conta_sri_pool:{ruc}:{anio}:{mes}:{tipo}:{operacion}"},
                )

            clientes = cls._consultar_clientes(
                db, rucs, anio=anio, mes=mes, tipo=tipo, operacion=operacion
            )
            no_disponibles = [c for c in clientes if not c["disponible"]]
            if no_disponibles:
                detalle = "; ".join(
                    f"{c['ruc']}: {c['motivo']}" for c in no_disponibles
                )
                raise ValueError(
                    "No se encoló ningún trabajo porque hay RUC no disponibles: "
                    + detalle
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
                    "ruc": cliente["ruc"],
                    "cliente": cliente["cliente"],
                    "anio": anio,
                    "mes": mes,
                    "tipo": tipo,
                    "operacion": operacion,
                })
                trabajos.append({
                    "job_id": job_id,
                    "ruc": cliente["ruc"],
                    "cliente": cliente["cliente"],
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
