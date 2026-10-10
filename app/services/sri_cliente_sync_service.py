from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import subprocess
import time
import urllib.request
import tempfile
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from sqlalchemy import text

from app.core.config import settings
from app.database.client_connection import obtener_session_cliente
from app.database.connection import engine


logger = logging.getLogger("conta.sri_sync")

# Log de diagnóstico específico para rastrear la clasificación de IVA
# desde el XML del SRI hasta la fila final de comprasnue.
IVA_DEBUG_LOG = Path(__file__).resolve().parents[2] / "logs" / "sri_iva_debug.log"


def _iva_debug_log(message: str, *args: Any) -> None:
    try:
        IVA_DEBUG_LOG.parent.mkdir(parents=True, exist_ok=True)
        text_message = message % args if args else message
        with IVA_DEBUG_LOG.open("a", encoding="utf-8") as fh:
            fh.write(f"{datetime.now().isoformat(timespec='seconds')} | {text_message}\n")
    except Exception:
        # El diagnóstico nunca debe detener una sincronización SRI.
        pass


# Marca de carga del módulo para confirmar qué proceso está ejecutando este archivo.
_iva_debug_log(
    "MODULO CARGADO | archivo=%s | log=%s",
    str(Path(__file__).resolve()),
    str(IVA_DEBUG_LOG),
)


class SriJobCancelado(Exception):
    """Señala que un trabajo SRI fue cancelado por el usuario."""


class SriClienteSyncService:
    JOB_TABLE = "conta_sri_jobs"
    WORKER_TABLE = "conta_sri_workers"

    @staticmethod
    def _iva_debug_log(message: str, *args: Any) -> None:
        _iva_debug_log(message, *args)

    @classmethod
    def _ensure_jobs_table(cls) -> None:
        with engine.begin() as db:
            db.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {cls.JOB_TABLE} (
                    job_id VARCHAR(64) PRIMARY KEY,
                    estado VARCHAR(20) NOT NULL,
                    ruc VARCHAR(13) NOT NULL,
                    cliente TEXT NOT NULL DEFAULT '',
                    anio INTEGER NOT NULL,
                    mes INTEGER NOT NULL,
                    tipo_comprobante VARCHAR(2) NOT NULL,
                    operacion VARCHAR(30) NOT NULL DEFAULT 'compras',
                    worker VARCHAR(150),
                    sri INTEGER NOT NULL DEFAULT 0,
                    ya_existentes INTEGER NOT NULL DEFAULT 0,
                    descargadas INTEGER NOT NULL DEFAULT 0,
                    guardadas INTEGER NOT NULL DEFAULT 0,
                    errores JSONB NOT NULL DEFAULT '[]'::jsonb,
                    paginas INTEGER NOT NULL DEFAULT 0,
                    dias_revisados INTEGER NOT NULL DEFAULT 0,
                    dias_ok INTEGER NOT NULL DEFAULT 0,
                    dias_diferentes INTEGER NOT NULL DEFAULT 0,
                    faltantes INTEGER NOT NULL DEFAULT 0,
                    sobrantes INTEGER NOT NULL DEFAULT 0,
                    mensaje TEXT NOT NULL DEFAULT '',
                    detalle TEXT,
                    creado TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    actualizado TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """))
            db.execute(text(
                f"ALTER TABLE {cls.JOB_TABLE} ADD COLUMN IF NOT EXISTS operacion VARCHAR(30) NOT NULL DEFAULT 'compras'"
            ))
            db.execute(text(
                f"ALTER TABLE {cls.JOB_TABLE} ADD COLUMN IF NOT EXISTS worker VARCHAR(150)"
            ))
            db.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {cls.WORKER_TABLE} (
                    worker_id VARCHAR(150) PRIMARY KEY,
                    usuario VARCHAR(150) NOT NULL,
                    equipo VARCHAR(150) NOT NULL,
                    estado VARCHAR(20) NOT NULL DEFAULT 'activo',
                    job_actual VARCHAR(64),
                    ultimo_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """))
            db.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{cls.WORKER_TABLE}_heartbeat ON {cls.WORKER_TABLE}(ultimo_heartbeat)"
            ))
            for columna in (
                "dias_revisados", "dias_ok", "dias_diferentes", "faltantes", "sobrantes"
            ):
                db.execute(text(
                    f"ALTER TABLE {cls.JOB_TABLE} "
                    f"ADD COLUMN IF NOT EXISTS {columna} INTEGER NOT NULL DEFAULT 0"
                ))
            db.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_{cls.JOB_TABLE}_estado "
                f"ON {cls.JOB_TABLE}(estado, creado)"
            ))

    @classmethod
    def _job_update(cls, job_id: str | None, **values) -> None:
        if not job_id:
            return
        cls._ensure_jobs_table()
        allowed = {
            "estado", "ruc", "cliente", "anio", "mes", "tipo_comprobante", "operacion", "worker",
            "sri", "ya_existentes", "descargadas", "guardadas", "errores",
            "paginas", "dias_revisados", "dias_ok", "dias_diferentes",
            "faltantes", "sobrantes", "mensaje", "detalle"
        }
        sets = []
        params = {"job_id": job_id}
        for key, value in values.items():
            if key not in allowed:
                continue
            if key == "errores":
                value = json.dumps(value, ensure_ascii=False)
            sets.append(f"{key} = :{key}")
            params[key] = value
        if not sets:
            return
        sets.append("actualizado = CURRENT_TIMESTAMP")
        with engine.begin() as db:
            db.execute(text(
                f"UPDATE {cls.JOB_TABLE} SET {', '.join(sets)} WHERE job_id = :job_id"
            ), params)

    @classmethod
    def iniciar_sincronizacion(cls, ruc: str, anio: int, mes: int, tipo_comprobante: int = 1, operacion: str = "compras", dias_objetivo: list[str] | None = None) -> dict[str, Any]:
        # El trabajo se guarda en PostgreSQL para que la API y el worker
        # interactivo compartan la misma cola, incluso en procesos separados.
        cred = cls._credenciales(ruc)
        cls._ensure_jobs_table()
        with engine.begin() as db:
            existente = db.execute(text(f"""
                SELECT job_id, estado
                FROM {cls.JOB_TABLE}
                WHERE ruc = :ruc AND anio = :anio AND mes = :mes
                  AND tipo_comprobante = :tipo
                  AND operacion = :operacion
                  AND estado IN ('pendiente', 'ejecutando', 'captcha')
                ORDER BY creado DESC
                LIMIT 1
            """), {
                "ruc": ruc, "anio": anio, "mes": mes,
                "tipo": cls._tipo(tipo_comprobante), "operacion": operacion,
            }).mappings().first()
            if existente:
                return {
                    "job_id": existente["job_id"],
                    "estado": existente["estado"],
                    "duplicado": True,
                }

            job_id = uuid.uuid4().hex
            db.execute(text(f"""
                INSERT INTO {cls.JOB_TABLE}
                (job_id, estado, ruc, cliente, anio, mes, tipo_comprobante, operacion, mensaje, detalle)
                VALUES
                (:job_id, 'pendiente', :ruc, :cliente, :anio, :mes, :tipo, :operacion, :mensaje, :detalle)
            """), {
                "job_id": job_id,
                "ruc": ruc,
                "cliente": cred["nombre"],
                "anio": anio,
                "mes": mes,
                "tipo": cls._tipo(tipo_comprobante), "operacion": operacion,
                "mensaje": "Sincronización en cola. Esperando al worker SRI interactivo.",
                "detalle": json.dumps({"dias_objetivo": dias_objetivo or []}, ensure_ascii=False) if operacion == "ventas_reparar" else None,
            })
        return {"job_id": job_id, "estado": "pendiente", "duplicado": False}

    @classmethod
    async def _ejecutar_job(cls, job_id: str, ruc: str, anio: int, mes: int, tipo_comprobante: int, operacion: str = "compras") -> None:
        cls._job_update(
            job_id,
            estado="ejecutando",
            mensaje="Worker SRI activo. Iniciando navegador y conexión con el SRI.",
        )
        try:
            if operacion in ("ventas_validar", "ventas_reparar"):
                # Import local para evitar dependencia circular: el validador
                # reutiliza los selectores y parsers del sincronizador SRI.
                from app.services.sri_ventas_validator_service import SriVentasValidatorService

                resultado = await SriVentasValidatorService.sincronizar_mes(
                    ruc, anio, mes, tipo_comprobante, job_id=job_id
                )
            else:
                resultado = await cls.sincronizar_mes(
                    ruc, anio, mes, tipo_comprobante, job_id=job_id, operacion=operacion
                )
            mensaje_final = str(
                resultado.get("mensaje")
                or "Sincronización finalizada."
            )
            resultado_final = dict(resultado)
            resultado_final["estado"] = "finalizado"
            resultado_final["mensaje"] = mensaje_final
            cls._job_update(job_id, **resultado_final)
        except SriJobCancelado as exc:
            cls._job_update(
                job_id,
                estado="cancelado",
                mensaje=str(exc),
            )
        except Exception as exc:
            cls._job_update(
                job_id,
                estado="error",
                mensaje=str(exc),
                detalle=str(exc),
            )

    @classmethod
    def cancelar_sincronizacion(cls, job_id: str) -> dict[str, Any] | None:
        """Solicita la cancelación de un trabajo pendiente o en ejecución."""
        cls._ensure_jobs_table()
        with engine.begin() as db:
            row = db.execute(text(f"""
                SELECT job_id, estado, ruc, cliente, anio, mes, operacion
                FROM {cls.JOB_TABLE}
                WHERE job_id = :job_id
                FOR UPDATE
            """), {"job_id": job_id}).mappings().first()
            if not row:
                return None
            if row["estado"] in ("finalizado", "error", "cancelado"):
                return dict(row)

            db.execute(text(f"""
                UPDATE {cls.JOB_TABLE}
                SET estado = 'cancelado',
                    mensaje = 'Cancelación solicitada por el usuario.',
                    detalle = NULL,
                    actualizado = CURRENT_TIMESTAMP
                WHERE job_id = :job_id
            """), {"job_id": job_id})

            result = dict(row)
            result["estado"] = "cancelado"
            result["mensaje"] = "Cancelación solicitada por el usuario."
            return result

    @classmethod
    def _verificar_cancelacion(cls, job_id: str | None) -> None:
        if not job_id:
            return
        with engine.connect() as db:
            estado = db.execute(text(f"""
                SELECT estado FROM {cls.JOB_TABLE}
                WHERE job_id = :job_id
            """), {"job_id": job_id}).scalar()
        if estado == "cancelado":
            raise SriJobCancelado("Sincronización cancelada por el usuario.")

    @classmethod
    def estado_sincronizacion(cls, job_id: str) -> dict[str, Any] | None:
        cls._ensure_jobs_table()
        with engine.connect() as db:
            row = db.execute(text(f"""
                SELECT job_id, estado, ruc, cliente, anio, mes, tipo_comprobante,
                       sri, ya_existentes, descargadas, guardadas, errores,
                       paginas, mensaje, detalle, creado, actualizado, operacion
                FROM {cls.JOB_TABLE}
                WHERE job_id = :job_id
            """), {"job_id": job_id}).mappings().first()
        if not row:
            return None
        result = dict(row)
        for key in ("creado", "actualizado"):
            if result.get(key):
                result[key] = result[key].isoformat()
        return result

    @classmethod
    def registrar_worker(cls, worker_id: str, usuario: str) -> None:
        """Registra y renueva una estación worker."""
        cls._ensure_jobs_table()
        equipo = socket.gethostname()
        with engine.begin() as db:
            db.execute(text(f"""
                UPDATE {cls.WORKER_TABLE}
                SET estado = 'inactivo'
                WHERE estado = 'activo'
                  AND ultimo_heartbeat < CURRENT_TIMESTAMP - INTERVAL '30 seconds'
            """))
            db.execute(text(f"""
                INSERT INTO {cls.WORKER_TABLE}
                    (worker_id, usuario, equipo, estado, ultimo_heartbeat)
                VALUES
                    (:worker_id, :usuario, :equipo, 'activo', CURRENT_TIMESTAMP)
                ON CONFLICT (worker_id) DO UPDATE SET
                    usuario = EXCLUDED.usuario,
                    equipo = EXCLUDED.equipo,
                    estado = 'activo',
                    ultimo_heartbeat = CURRENT_TIMESTAMP
            """), {"worker_id": worker_id, "usuario": usuario, "equipo": equipo})

    @classmethod
    def obtener_trabajo_pendiente(cls, worker_id: str | None = None) -> dict[str, Any] | None:
        """Reclama atómicamente un trabajo y lo identifica con el worker."""
        cls._ensure_jobs_table()
        with engine.begin() as db:
            row = db.execute(text(f"""
                SELECT job_id, ruc, anio, mes, tipo_comprobante, operacion
                FROM {cls.JOB_TABLE}
                WHERE estado = 'pendiente'
                ORDER BY creado
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            """)).mappings().first()
            if not row:
                return None
            db.execute(text(f"""
                UPDATE {cls.JOB_TABLE}
                SET estado = 'ejecutando',
                    worker = :worker,
                    mensaje = 'Trabajo reclamado por el worker SRI.',
                    actualizado = CURRENT_TIMESTAMP
                WHERE job_id = :job_id
            """), {"job_id": row["job_id"], "worker": worker_id})
            if worker_id:
                db.execute(text(f"""
                    UPDATE {cls.WORKER_TABLE}
                    SET job_actual = :job_id,
                        ultimo_heartbeat = CURRENT_TIMESTAMP,
                        estado = 'activo'
                    WHERE worker_id = :worker
                """), {"job_id": row["job_id"], "worker": worker_id})
        return dict(row)


    """Sincroniza automáticamente comprobantes recibidos del SRI hacia comprasnue.

    El portal del SRI usa reCAPTCHA para la consulta mensual. Conta no intenta
    saltarse ni reutilizar el CAPTCHA. Si el SRI presenta el desafío, Chromium
    puede quedar visible para que el usuario lo resuelva; una vez superado,
    todo el procesamiento es automático.
    """

    LOGIN_URL = "https://srienlinea.sri.gob.ec/sri-en-linea/contribuyente/perfil"
    PORTAL_URL = "https://srienlinea.sri.gob.ec/tuportal-internet/accederAplicacion.jspa?redireccion=60&idGrupo=58"
    RECIBIDOS_URL = "https://srienlinea.sri.gob.ec/comprobantes-electronicos-internet/pages/consultas/recibidos/comprobantesRecibidos.jsf"

    @staticmethod
    def _dec(value: Any) -> Decimal:
        """
        Convierte números provenientes del SRI tolerando formatos locales.
        Acepta, por ejemplo: 1312.00, 1,312.00, 1.312,00,
        1 312,00 y valores con símbolo de moneda.
        """
        if value is None:
            return Decimal("0")
        if isinstance(value, Decimal):
            return value

        texto = str(value).strip()
        if not texto:
            return Decimal("0")

        texto = (
            texto.replace("\xa0", "")
            .replace(" ", "")
            .replace("$", "")
            .replace("€", "")
        )
        texto = "".join(ch for ch in texto if ch.isdigit() or ch in ".,+-")
        if not texto or texto in {"+", "-", ".", ",", "+.", "-.", "+,", "-,"}:
            return Decimal("0")

        try:
            tiene_coma = "," in texto
            tiene_punto = "." in texto

            if tiene_coma and tiene_punto:
                # El último separador normalmente es el decimal.
                if texto.rfind(",") > texto.rfind("."):
                    texto = texto.replace(".", "").replace(",", ".")
                else:
                    texto = texto.replace(",", "")
            elif tiene_coma:
                partes = texto.split(",")
                if len(partes) > 2:
                    texto = "".join(partes)
                elif len(partes) == 2 and len(partes[1]) == 3 and partes[0].isdigit():
                    texto = "".join(partes)
                else:
                    texto = texto.replace(",", ".")
            elif tiene_punto:
                partes = texto.split(".")
                if len(partes) > 2:
                    texto = "".join(partes)

            return Decimal(texto)
        except (InvalidOperation, ValueError):
            logger.warning("No se pudo convertir valor numérico SRI: %r", value)
            return Decimal("0")

    @staticmethod
    def _txt(node: ET.Element | None, tag: str, default: str = "") -> str:
        return ((node.findtext(tag) if node is not None else None) or default).strip()

    @staticmethod
    def _fecha_varchar(value: Any) -> str:
        """Normaliza fechas para los campos VARCHAR de comprasnue."""
        if value is None:
            return ""
        if isinstance(value, datetime):
            return value.strftime("%d/%m/%Y")
        texto = str(value).strip()
        if not texto:
            return ""
        for formato in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(texto[:10], formato).strftime("%d/%m/%Y")
            except ValueError:
                continue
        return texto

    @staticmethod
    def _tipo(tipo: int) -> str:
        return {1: "01", 2: "02", 3: "03", 4: "04", 5: "05", 6: "06", 7: "07"}.get(tipo, f"{tipo:02d}")

    @classmethod
    def _credenciales(cls, ruc: str) -> dict[str, str]:
        with engine.connect() as db:
            row = db.execute(text("""
                SELECT ruccedcli, nomclient, activo, clavesri
                FROM clientes
                WHERE ruccedcli = :ruc
                LIMIT 1
            """), {"ruc": ruc}).mappings().first()

        if not row:
            raise ValueError("El RUC no existe en BdTotal.")
        if not bool(row["activo"]):
            raise ValueError("El cliente no está activo.")
        clave = str(row["clavesri"] or "").strip()
        if not clave:
            raise ValueError("El cliente no tiene clave SRI configurada.")

        return {"ruc": str(row["ruccedcli"]), "nombre": str(row["nomclient"] or ""), "clave": clave}

    @classmethod
    def _parsear_xml(cls, path: Path) -> dict[str, Any]:
        root = ET.parse(path).getroot()
        raw = root.findtext("comprobante")
        if not raw:
            raise ValueError("El XML no contiene comprobante.")
        doc = ET.fromstring(raw)
        it = doc.find("infoTributaria")
        cod_doc = cls._txt(it, "codDoc")

        # Las retenciones recibidas (codDoc 07) no tienen infoFactura.
        # Una retención puede afectar una o varias facturas dentro de
        # docsSustento; cada factura debe recibir la suma de sus propias
        # retenciones IVA y renta.
        if cod_doc == "07":
            info_ret = doc.find("infoCompRetencion")
            if it is None or info_ret is None:
                raise ValueError("La retención no contiene la estructura tributaria esperada.")

            fecha_txt = cls._txt(info_ret, "fechaEmision")
            try:
                fecha = datetime.strptime(fecha_txt, "%d/%m/%Y")
            except ValueError as exc:
                raise ValueError(f"Fecha de retención inválida: {fecha_txt}") from exc

            # En las retenciones reales del SRI, los documentos sustento
            # pueden venir directamente dentro de <impuestos><impuesto>,
            # no necesariamente dentro de <docsSustento>.
            # Agrupamos por numDocSustento para sumar IVA y renta de cada factura.
            documentos_map: dict[str, dict[str, Any]] = {}

            for impuesto in doc.findall("./impuestos/impuesto"):
                num_doc = cls._txt(impuesto, "numDocSustento")
                if not num_doc:
                    continue

                if num_doc not in documentos_map:
                    documentos_map[num_doc] = {
                        "num_doc_sustento": num_doc,
                        "fecha_doc_sustento": cls._txt(
                            impuesto, "fechaEmisionDocSustento"
                        ),
                        "num_aut_doc_sustento": "",
                        "retiva": Decimal("0"),
                        "retrenta": Decimal("0"),
                    }

                codigo = cls._txt(impuesto, "codigo")
                valor = cls._dec(cls._txt(impuesto, "valorRetenido"))

                # codigo 1 = renta; codigo 2 = IVA.
                if codigo == "2":
                    documentos_map[num_doc]["retiva"] += valor
                elif codigo == "1":
                    documentos_map[num_doc]["retrenta"] += valor

            # Compatibilidad con XML que sí utilicen docsSustento.
            for sustento in doc.findall("./docsSustento/docSustento"):
                num_doc = cls._txt(sustento, "numDocSustento")
                if not num_doc:
                    continue

                if num_doc not in documentos_map:
                    documentos_map[num_doc] = {
                        "num_doc_sustento": num_doc,
                        "fecha_doc_sustento": cls._txt(
                            sustento, "fechaEmisionDocSustento"
                        ),
                        "num_aut_doc_sustento": cls._txt(
                            sustento, "numAutDocSustento"
                        ),
                        "retiva": Decimal("0"),
                        "retrenta": Decimal("0"),
                    }

                for retencion in sustento.findall("./retenciones/retencion"):
                    codigo = cls._txt(retencion, "codigo")
                    valor = cls._dec(cls._txt(retencion, "valorRetenido"))
                    if codigo == "2":
                        documentos_map[num_doc]["retiva"] += valor
                    elif codigo == "1":
                        documentos_map[num_doc]["retrenta"] += valor

            documentos = list(documentos_map.values())

            return {
                "ruc": cls._txt(it, "ruc"),
                "razon_social": cls._txt(it, "razonSocial"),
                "cod_doc": cod_doc,
                "numest": cls._txt(it, "estab"),
                "numptoemi": cls._txt(it, "ptoEmi"),
                "numsec": cls._txt(it, "secuencial"),
                "clave_acceso": cls._txt(it, "claveAcceso"),
                "numero_autorizacion": cls._txt(root, "numeroAutorizacion"),
                "fecha_emision": fecha_txt,
                "fecha": fecha,
                "identificacion_sujeto_retenido": cls._txt(
                    info_ret, "identificacionSujetoRetenido"
                ),
                "razon_social_sujeto_retenido": cls._txt(
                    info_ret, "razonSocialSujetoRetenido"
                ),
                "documentos_sustento": documentos,
            }

        inf = doc.find("infoNotaCredito") if cod_doc == "04" else doc.find("infoFactura")
        if it is None or inf is None:
            raise ValueError("El comprobante no contiene la estructura tributaria esperada.")

        fecha_txt = cls._txt(inf, "fechaEmision")
        try:
            fecha = datetime.strptime(fecha_txt, "%d/%m/%Y")
        except ValueError as exc:
            raise ValueError(f"Fecha de emisión inválida: {fecha_txt}") from exc

        bases = {
            "no_objeto": Decimal("0"), "0": Decimal("0"), "5": Decimal("0"),
            "8": Decimal("0"), "12": Decimal("0"), "14": Decimal("0"),
            "15": Decimal("0"), "exenta": Decimal("0"),
        }
        ivas = {k: Decimal("0") for k in ("5", "8", "12", "14", "15")}
        ice = Decimal("0")

        codigo_a_tasa = {
            "0": "0", "2": "12", "3": "14", "4": "15",
            "5": "5", "8": "8",
        }

        # Para compras, el SRI entrega la tarifa real en los impuestos
        # de cada detalle. Esa estructura es la fuente principal.
        impuestos_clasificados = Decimal("0")
        # Un IVA 0% es un impuesto válido aunque no incremente
        # impuestos_clasificados. Solo usamos totalConImpuestos como respaldo
        # cuando no existe ningún impuesto IVA en los detalles.
        impuestos_detalle_encontrados = False

        for impuesto in doc.findall(".//detalle/impuestos/impuesto"):
            codigo = cls._txt(impuesto, "codigo")
            if codigo != "2":
                continue

            impuestos_detalle_encontrados = True

            codigo_pct = cls._txt(impuesto, "codigoPorcentaje")
            tarifa = cls._dec(cls._txt(impuesto, "tarifa"))
            base = cls._dec(cls._txt(impuesto, "baseImponible"))
            valor = cls._dec(cls._txt(impuesto, "valor"))

            _iva_debug_log(
                "XML DETALLE | clave=%s | codigo=%s | codigoPorcentaje=%s | tarifa=%s | baseImponible=%s | valor=%s",
                cls._txt(it, "claveAcceso"), codigo, codigo_pct, tarifa, base, valor,
            )

            tarifa_key = format(tarifa, "f").rstrip("0").rstrip(".")
            if tarifa_key in {"5", "8", "12", "14", "15"}:
                tasa = tarifa_key
            else:
                tasa = codigo_a_tasa.get(codigo_pct)

            _iva_debug_log(
                "XML CLASIFICACION | clave=%s | codigoPorcentaje=%s | tarifa=%s | tasa_resultante=%s | base=%s | valor=%s",
                cls._txt(it, "claveAcceso"), codigo_pct, tarifa, tasa, base, valor,
            )

            if tasa in {"5", "8", "12", "14", "15"}:
                bases[tasa] += base
                ivas[tasa] += valor
                impuestos_clasificados += base
            elif tasa == "0":
                bases["0"] += base
            elif codigo_pct == "6":
                bases["no_objeto"] += base
            elif codigo_pct == "7":
                bases["exenta"] += base
            else:
                bases["no_objeto"] += base

        # Respaldo: si el XML no trae impuestos dentro de los detalles,
        # usamos totalConImpuestos. En los XML normales de compras no se
        # llega aquí, pero permite procesar comprobantes con estructura
        # incompleta.
        if not impuestos_detalle_encontrados:
            totals = inf.find("totalConImpuestos")
            if totals is not None:
                for ti in totals.findall("totalImpuesto"):
                    codigo = cls._txt(ti, "codigo")
                    if codigo == "3":
                        ice += cls._dec(cls._txt(ti, "valor"))
                        continue
                    if codigo != "2":
                        continue

                    codigo_pct = cls._txt(ti, "codigoPorcentaje")
                    tarifa = cls._dec(cls._txt(ti, "tarifa"))
                    base = cls._dec(cls._txt(ti, "baseImponible"))
                    valor = cls._dec(cls._txt(ti, "valor"))

                    tarifa_key = format(tarifa, "f").rstrip("0").rstrip(".")
                    tasa = (
                        tarifa_key
                        if tarifa_key in {"0", "5", "8", "12", "14", "15"}
                        else codigo_a_tasa.get(codigo_pct)
                    )

                    if tasa in {"5", "8", "12", "14", "15"}:
                        bases[tasa] += base
                        ivas[tasa] += valor
                    elif tasa == "0":
                        bases["0"] += base
                    elif codigo_pct == "6":
                        bases["no_objeto"] += base
                    elif codigo_pct == "7":
                        bases["exenta"] += base
                    else:
                        bases["no_objeto"] += base


        logger.warning(
            "SRI COMPRA PARSER | clave=%s | bases=%s | ivas=%s | subtotal=%s",
            cls._txt(it, "claveAcceso"),
            {k: str(v) for k, v in bases.items()},
            {k: str(v) for k, v in ivas.items()},
            cls._txt(inf, "totalSinImpuestos"),
        )
        _iva_debug_log(
            "XML FINAL | clave=%s | bases=%s | ivas=%s | subtotal=%s | total=%s",
            cls._txt(it, "claveAcceso"),
            {k: str(v) for k, v in bases.items()},
            {k: str(v) for k, v in ivas.items()},
            cls._txt(inf, "totalSinImpuestos"),
            cls._txt(inf, "importeTotal"),
        )

        pagos = inf.find("pagos")
        formas = [] if pagos is None else [cls._txt(p, "formaPago") for p in pagos.findall("pago")]

        datos_modificacion = {}
        if cod_doc == "04":
            num_doc_mod = cls._txt(inf, "numDocModificado")
            fecha_doc_mod = cls._txt(inf, "fechaEmisionDocSustento")
            partes = num_doc_mod.split("-")
            datos_modificacion = {
                "numestmod": partes[0] if len(partes) == 3 else "",
                "numptoemimod": partes[1] if len(partes) == 3 else "",
                "numsecmod": partes[2] if len(partes) == 3 else num_doc_mod,
                "fecfac": fecha_doc_mod,
            }

        return {
            "ruc": cls._txt(it, "ruc"),
            "razon_social": cls._txt(it, "razonSocial"),
            "cod_doc": cls._txt(it, "codDoc"),
            "numest": cls._txt(it, "estab"),
            "numptoemi": cls._txt(it, "ptoEmi"),
            "numsec": cls._txt(it, "secuencial"),
            "clave_acceso": cls._txt(it, "claveAcceso"),
            "fecha_emision": fecha_txt,
            "fecha": fecha,
            "fecha_autorizacion": cls._txt(root, "fechaAutorizacion"),
            "bases": bases,
            "ivas": ivas,
            "ice": ice,
            "subtotal": cls._dec(cls._txt(inf, "totalSinImpuestos")),
            "total": cls._dec(cls._txt(inf, "importeTotal")),
            "tipopago": next((x for x in formas if x), ""),
            **datos_modificacion,
        }

    @staticmethod
    def _chrome_executable() -> str:
        configured = str(settings.SRI_CHROME_PATH or "").strip()
        candidates = [
            configured,
            str(Path(os.environ.get("ProgramFiles", "")) / "Google/Chrome/Application/chrome.exe"),
            str(Path(os.environ.get("ProgramFiles(x86)", "")) / "Google/Chrome/Application/chrome.exe"),
            str(Path(os.environ.get("LOCALAPPDATA", "")) / "Google/Chrome/Application/chrome.exe"),
        ]
        for candidate in candidates:
            if candidate and Path(candidate).is_file():
                return candidate
        raise RuntimeError(
            "No se encontró Google Chrome. Configure SRI_CHROME_PATH en .env "
            "con la ruta completa de chrome.exe."
        )

    @staticmethod
    def _puerto_libre(port: int) -> bool:
        """Comprueba si un puerto TCP local está libre."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            return sock.connect_ex(("127.0.0.1", port)) != 0

    @staticmethod
    def _puerto_cdp_disponible(preferido: int) -> int:
        """Obtiene un puerto CDP libre, usando el configurado solo si está disponible."""
        if preferido > 0 and SriClienteSyncService._puerto_libre(preferido):
            return preferido

        # El sistema asigna un puerto efímero libre. Esto evita depender de 9222
        # cuando ya existe otro Chrome de pruebas usando ese puerto.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    @classmethod
    async def _esperar_cdp(cls, port: int, timeout: float = 20.0) -> None:
        url = f"http://127.0.0.1:{port}/json/version"
        limite = time.monotonic() + timeout
        ultimo_error = None
        while time.monotonic() < limite:
            try:
                with urllib.request.urlopen(url, timeout=1.5) as response:
                    if response.status == 200:
                        return
            except Exception as exc:
                ultimo_error = exc
            await asyncio.sleep(0.25)
        raise RuntimeError(
            f"Chrome no abrió el puerto CDP {port} dentro de {timeout:.0f} segundos. "
            f"Último error: {ultimo_error}"
        )

    @staticmethod
    def _cerrar_chrome(chrome_process) -> None:
        """Cierra de forma segura el Chrome SRI que abrió Conta."""
        if chrome_process is None:
            return

        pid = getattr(chrome_process, "pid", None)
        try:
            if chrome_process.poll() is None:
                chrome_process.terminate()
                try:
                    chrome_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    chrome_process.kill()
                    try:
                        chrome_process.wait(timeout=3)
                    except Exception:
                        pass

            # En Windows, si el proceso principal dejó procesos hijos de Chrome
            # abiertos, cerramos únicamente el árbol del PID que Conta inició.
            if pid and chrome_process.poll() is None:
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
        except Exception:
            # El cierre nunca debe convertir una sincronización exitosa en error.
            try:
                if pid:
                    subprocess.run(
                        ["taskkill", "/PID", str(pid), "/T", "/F"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        check=False,
                    )
            except Exception:
                pass

    @classmethod
    async def _login(cls, ruc: str, clave: str):
        p = await async_playwright().start()
        browser = None
        context = None
        chrome_process = None

        profile_root = str(settings.SRI_USER_DATA_DIR or "").strip()
        if not profile_root:
            profile_root = str(Path.cwd() / "sri_profiles")

        profile_dir = Path(profile_root) / f"{ruc}_chrome"
        profile_dir.mkdir(parents=True, exist_ok=True)

        puerto_preferido = int(settings.SRI_CDP_PORT or 9222)
        port = cls._puerto_cdp_disponible(puerto_preferido)
        if port != puerto_preferido:
            logging.getLogger("conta.sri").warning(
                "Puerto CDP %s ocupado; usando puerto disponible %s para RUC %s.",
                puerto_preferido,
                port,
                ruc,
            )

        chrome_path = cls._chrome_executable()
        args = [
            chrome_path,
            f"--user-data-dir={profile_dir}",
            f"--remote-debugging-port={port}",
            "--no-first-run",
            "--no-default-browser-check",
            "--lang=es-EC",
            "--window-size=1366,900",
        ]
        if settings.SRI_HEADLESS:
            args.append("--headless=new")

        try:
            chrome_process = subprocess.Popen(
                args,
                cwd=str(profile_dir),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            await cls._esperar_cdp(port)

            browser = await p.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
            context = browser.contexts[0]
            page = context.pages[0] if context.pages else await context.new_page()

            # No usamos evasión de automatización. Chrome real + CDP es el navegador
            # que ya fue validado manualmente contra reCAPTCHA Enterprise del SRI.

            try:
                await page.goto(
                    cls.PORTAL_URL,
                    wait_until="domcontentloaded",
                    timeout=settings.SRI_NAVIGATION_TIMEOUT_MS,
                )
                if "perfil" not in page.url and "login" not in page.url.lower():
                    return p, browser, context, page, chrome_process
            except PlaywrightTimeoutError:
                if page.url != "about:blank" and "perfil" not in page.url and "login" not in page.url.lower():
                    return p, browser, context, page, chrome_process

            try:
                await page.goto(
                    cls.LOGIN_URL,
                    wait_until="domcontentloaded",
                    timeout=settings.SRI_NAVIGATION_TIMEOUT_MS,
                )
            except PlaywrightTimeoutError as exc:
                try:
                    await page.goto(
                        "https://srienlinea.sri.gob.ec/",
                        wait_until="domcontentloaded",
                        timeout=20000,
                    )
                    await page.goto(
                        cls.LOGIN_URL,
                        wait_until="domcontentloaded",
                        timeout=settings.SRI_NAVIGATION_TIMEOUT_MS,
                    )
                except Exception as retry_exc:
                    raise RuntimeError(
                        "No se pudo abrir el portal del SRI desde Chrome real. "
                        f"Primer intento: {exc}. Reintento: {retry_exc}"
                    ) from retry_exc

            usuario = page.locator(
                'input[name="username"]:visible, #username:visible, #usuario:visible'
            ).first
            password = page.locator("#password:visible").first
            await usuario.wait_for(state="visible", timeout=30000)
            await password.wait_for(state="visible", timeout=10000)
            await usuario.fill(ruc)
            try:
                await page.fill("#ciAdicional", "")
            except Exception:
                pass
            await password.fill(clave)

            login_button = page.locator("#kc-login").first
            await login_button.wait_for(state="visible", timeout=30000)
            await login_button.click(force=True)

            # Keycloak/SRI puede tardar varios segundos en completar el
            # redirect. No basta con esperar 1.5 s: si navegamos al portal
            # demasiado pronto, el portal vuelve a enviarnos al login y luego
            # el formulario de comprobantes nunca llega a construirse.
            login_ok = False
            limite_login = time.monotonic() + 45

            while time.monotonic() < limite_login:
                await page.wait_for_timeout(500)

                url_actual = page.url.lower()
                try:
                    tiene_password = await page.locator("#password:visible").count() > 0
                except Exception:
                    tiene_password = False

                # El login correcto termina en el perfil/portal del SRI,
                # no en /auth/realms/.../login.
                if "auth/realms/" not in url_actual and "kc-login" not in url_actual:
                    if "perfil" in url_actual or "tuportal-internet" in url_actual:
                        login_ok = True
                        break

                # Si Keycloak todavía muestra el campo de contraseña,
                # simplemente seguimos esperando hasta que termine el redirect.
                # No se ejecuta ningún bloque vacío aquí porque Python exige
                # una instrucción dentro del if.
                if tiene_password:
                    continue

                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=1000)
                except Exception:
                    pass

            if not login_ok:
                url_actual = page.url
                texto_login = ""
                try:
                    texto_login = " ".join(
                        (await page.locator("body").inner_text(timeout=3000)).split()
                    )[:1000]
                except Exception:
                    pass

                raise ValueError(
                    "El SRI no completó el inicio de sesión después de 45 segundos. "
                    f"URL={url_actual}; texto={texto_login!r}"
                )

            # Ahora que la sesión está establecida, entramos al portal de
            # comprobantes. Si el SSO vuelve a redirigir al login, no seguimos
            # como si la sesión fuese válida.
            await page.goto(
                cls.PORTAL_URL,
                wait_until="domcontentloaded",
                timeout=60000,
            )

            limite_portal = time.monotonic() + 30
            while time.monotonic() < limite_portal:
                await page.wait_for_timeout(500)
                url_actual = page.url.lower()

                if "auth/realms/" in url_actual and "login" in url_actual:
                    continue

                if (
                    "tuportal-internet" in url_actual
                    or "comprobantes-electronicos-internet" in url_actual
                    or "perfil" in url_actual
                ):
                    return p, browser, context, page, chrome_process

            raise RuntimeError(
                "El SRI autenticó la sesión pero no permitió acceder al portal "
                f"de comprobantes. URL final={page.url}"
            )

        except Exception:
            if browser is not None:
                try:
                    await browser.close()
                except Exception:
                    pass
            elif context is not None:
                try:
                    await context.close()
                except Exception:
                    pass
            cls._cerrar_chrome(chrome_process)
            await p.stop()
            raise

    @staticmethod
    async def _seleccionar(page, selector: str, value: str) -> None:
        """
        Selecciona un control PrimeFaces del SRI tolerando cargas AJAX lentas.

        El SRI puede mostrar primero el documento base y construir los combos
        después mediante JavaScript. No debemos convertir esa ventana de carga
        en un error inmediato de sincronización.
        """
        locator = page.locator(selector).first

        try:
            await locator.wait_for(state="attached", timeout=30000)
        except PlaywrightTimeoutError as exc:
            diagnostico = await SriClienteSyncService._diagnostico_consulta(page)
            raise RuntimeError(
                f"No apareció el campo SRI {selector} después de 30 segundos. "
                + diagnostico
            ) from exc

        try:
            await locator.wait_for(state="visible", timeout=30000)
        except PlaywrightTimeoutError:
            # En algunas respuestas AJAX el <select> está en el DOM pero
            # temporalmente oculto mientras PrimeFaces termina de renderizarlo.
            # Si ya está adjunto, esperamos un poco más antes de fallar.
            for _ in range(20):
                try:
                    if await locator.is_visible():
                        break
                except Exception:
                    pass
                await page.wait_for_timeout(500)
            else:
                diagnostico = await SriClienteSyncService._diagnostico_consulta(page)
                raise RuntimeError(
                    f"El campo SRI {selector} existe pero no llegó a estar visible. "
                    + diagnostico
                )

        await page.select_option(selector, value)

    @staticmethod
    def _tipid_emitido(identificacion: str) -> str:
        valor = str(identificacion or "").strip()
        if valor == "9999999999999":
            return "07"
        if len(valor) == 10:
            return "05"
        if len(valor) == 13:
            return "04"
        return ""

    @classmethod
    def _parsear_factura_emitida_html(cls, html: str) -> dict[str, Any]:
        from bs4 import BeautifulSoup
        import re

        soup = BeautifulSoup(html, "html.parser")
        cab: dict[str, str] = {}
        pares: list[tuple[str, str]] = []

        def _txt(celda) -> str:
            return " ".join(celda.get_text(" ", strip=True).split())

        def _canon_tasa(valor: str) -> str | None:
            match = re.fullmatch(r"(\d+(?:[.,]\d+)?)\s*%?", str(valor or "").strip())
            if not match:
                return None
            tasa = cls._dec(match.group(1))
            return format(tasa, "f").rstrip("0").rstrip(".")

        # El SRI genera una tabla de impuestos por cada línea del detalle.
        # Solo esas tablas alimentan las bases/IVA por tarifa. La tabla de
        # totales del comprobante se ignora para evitar duplicar importes.
        for tabla in soup.find_all("table"):
            tabla_id = tabla.get("id") or ""
            if "tabla-impuestos-detalle-factura" not in tabla_id:
                continue

            filas = tabla.find_all("tr")
            indice_encabezado = None
            encabezados: list[str] = []

            for indice, fila in enumerate(filas):
                textos = [_txt(c) for c in fila.find_all(["td", "th"])]
                textos = [t for t in textos if t]
                normalizados = [t.lower().rstrip(":").strip() for t in textos]

                if len(normalizados) >= 5:
                    requeridos = {"impuesto", "porcentaje", "tarifa", "base imponible", "valor"}
                    if requeridos.issubset(set(normalizados)):
                        indice_encabezado = indice
                        encabezados = normalizados
                        break

            if indice_encabezado is None:
                continue

            pos_impuesto = encabezados.index("impuesto")
            pos_porcentaje = encabezados.index("porcentaje")
            pos_tarifa = encabezados.index("tarifa")
            pos_base = encabezados.index("base imponible")
            pos_valor = encabezados.index("valor")

            for fila in filas[indice_encabezado + 1:]:
                textos = [_txt(c) for c in fila.find_all(["td", "th"])]
                textos = [t for t in textos if t]
                if len(textos) <= max(pos_impuesto, pos_porcentaje, pos_tarifa, pos_base, pos_valor):
                    continue

                if textos[pos_impuesto].upper().strip() != "IVA":
                    continue

                tasa = _canon_tasa(textos[pos_porcentaje]) or _canon_tasa(textos[pos_tarifa])
                if tasa not in {"0", "5", "8", "12", "14", "15"}:
                    continue

                pares.append((f"Base imponible IVA {tasa}%", textos[pos_base]))
                pares.append((f"Valor IVA {tasa}%", textos[pos_valor]))

        # Las demás tablas se usan solo para cabecera y valores generales.
        # Nunca volvemos a interpretar las tablas de impuestos como pares
        # genéricos, porque eso puede mandar una base gravada a baseiva0.
        for tabla in soup.find_all("table"):
            tabla_id = tabla.get("id") or ""
            if "tabla-impuestos-detalle-factura" in tabla_id:
                continue

            for fila in tabla.find_all("tr"):
                textos = [_txt(c) for c in fila.find_all(["td", "th"])]
                textos = [t for t in textos if t]
                if not textos:
                    continue

                if len(textos) == 2:
                    etiqueta, valor = textos[0].rstrip(":"), textos[1]
                    pares.append((etiqueta, valor))
                    cab[etiqueta] = valor
                elif len(textos) == 3:
                    etiqueta, tarifa, valor = textos[0].rstrip(":"), textos[1].strip(), textos[2]
                    tasa = _canon_tasa(tarifa)
                    if tasa:
                        pares.append((f"{etiqueta} {tasa}%", valor))
                    else:
                        pares.append((etiqueta, valor))
                elif len(textos) % 2 == 0:
                    for pos in range(0, len(textos), 2):
                        pares.append((textos[pos].rstrip(":"), textos[pos + 1]))
                # Las tablas de totales de 5 columnas se ignoran aquí.

        def normalizar(s: str) -> str:
            return " ".join(s.lower().replace(":", " ").split())

        def val(nombre: str) -> str:
            objetivo = normalizar(nombre)
            for k, v in cab.items():
                if normalizar(k) == objetivo:
                    return v
            for etiqueta, valor in pares:
                if normalizar(etiqueta) == objetivo:
                    return valor
            return ""

        def buscar_valor_por_etiquetas(etiquetas: list[str]) -> Decimal:
            # Una factura puede tener varias líneas con la misma tarifa.
            # El SRI entrega una fila de impuesto por cada detalle, por lo que
            # NO debemos devolver solo la primera coincidencia.
            objetivos = [normalizar(x) for x in etiquetas]
            total = Decimal("0")
            encontrado = False

            for etiqueta, valor in pares:
                et = normalizar(etiqueta)
                if "%" in valor:
                    continue
                if et in objetivos or any(et.startswith(obj + " ") for obj in objetivos):
                    total += cls._dec(valor)
                    encontrado = True

            return total if encontrado else Decimal("0")

        def buscar_tasa(tipo: str, etiquetas_base: list[str], etiquetas_iva: list[str]) -> tuple[Decimal, Decimal]:
            base = buscar_valor_por_etiquetas(etiquetas_base)
            iva = buscar_valor_por_etiquetas(etiquetas_iva)
            return base, iva

        fecha_txt = val("Fecha Emisión") or val("Fecha de Emisión")
        fecha = datetime.strptime(fecha_txt, "%d/%m/%Y")
        identificacion = val("Identificación Comprador")

        base0 = buscar_valor_por_etiquetas([
            "Base imponible IVA 0%", "Base IVA 0%", "Subtotal 0%", "Subtotal IVA 0%"
        ])
        base_no = buscar_valor_por_etiquetas([
            "Base imponible no objeto de IVA", "Base no objeto", "Subtotal no objeto de IVA"
        ])

        bases_iva: dict[str, Decimal] = {}
        ivas: dict[str, Decimal] = {}
        for tasa in ("5", "8", "12", "14", "15"):
            base, iva = buscar_tasa(
                tasa,
                [f"Base imponible IVA {tasa}%", f"Base IVA {tasa}%", f"Subtotal {tasa}%", f"Subtotal IVA {tasa}%"],
                [f"Valor IVA {tasa}%", f"Importe IVA {tasa}%", f"IVA {tasa}%"],
            )
            bases_iva[tasa] = base
            ivas[tasa] = iva

        # Algunas pantallas muestran solo "Valor IVA" junto a una fila "SUBTOTAL X%".
        # Si existe exactamente una base por tasa y no existe IVA específico, calculamos
        # el IVA de esa tasa para conservar el detalle real del comprobante.
        for tasa, base in bases_iva.items():
            if base and not ivas[tasa]:
                ivas[tasa] = (base * Decimal(tasa) / Decimal("100")).quantize(Decimal("0.01"))

        # Fallback para versiones del SRI que solo muestran "IVA" genérico.
        if not any(ivas.values()):
            iva_generico = buscar_valor_por_etiquetas([
                "Valor IVA", "Importe IVA", "IVA total", "Total IVA", "IVA"
            ])
            tasas_con_base = [t for t, b in bases_iva.items() if b]
            if len(tasas_con_base) == 1 and iva_generico:
                ivas[tasas_con_base[0]] = iva_generico

        baseiva_total = sum(bases_iva.values(), Decimal("0"))
        iva_total = sum(ivas.values(), Decimal("0"))

        # Algunos diseños del SRI no incluyen la tarifa en la etiqueta de la
        # fila: muestran solamente "Subtotal" + monto e "IVA" + monto.
        # En ese caso NO debemos enviar el subtotal a baseiva0. Si existe una
        # única base y un IVA, calculamos la tasa efectiva y la asociamos a la
        # tarifa SRI correspondiente (5/8/12/14/15).
        subtotal_generico = buscar_valor_por_etiquetas([
            "Total Sin impuestos", "Subtotal sin impuestos", "Subtotal"
        ])
        iva_generico = buscar_valor_por_etiquetas([
            "Valor IVA", "Importe IVA", "IVA total", "Total IVA", "IVA"
        ])

        if not baseiva_total and subtotal_generico > 0 and iva_generico > 0:
            tasa_detectada = None
            for tasa in ("5", "8", "12", "14", "15"):
                esperado = (
                    subtotal_generico * Decimal(tasa) / Decimal("100")
                ).quantize(Decimal("0.01"))
                if abs(esperado - iva_generico) <= Decimal("0.02"):
                    tasa_detectada = tasa
                    break

            if tasa_detectada:
                bases_iva[tasa_detectada] = subtotal_generico
                ivas[tasa_detectada] = iva_generico
                baseiva_total = subtotal_generico
                iva_total = iva_generico

        # Compatibilidad con comprobantes donde ya se obtuvo una única tasa
        # mediante el IVA específico pero el subtotal quedó sin etiqueta.
        if not baseiva_total:
            subtotal = subtotal_generico
            if iva_total > 0:
                tasas_con_iva = [t for t, v in ivas.items() if v]
                if len(tasas_con_iva) == 1:
                    bases_iva[tasas_con_iva[0]] = subtotal
                    baseiva_total = subtotal
            # Un subtotal genérico no se considera IVA 0%.
            # baseiva0 solo se llena cuando el SRI identifica explícitamente 0%.

        return {
            "clave_acceso": val("Clave de acceso"),
            "fecha_emision": fecha_txt,
            "fecha": fecha,
            "identificacion": identificacion,
            "razon_social": val("Razón Social Comprador"),
            "establecimiento": val("Establecimiento"),
            "punto_emision": val("Punto de emisión"),
            "secuencial": val("Secuencial"),
            "base_no_objeto": base_no,
            "base_iva0": base0,
            "bases_iva": bases_iva,
            "ivas": ivas,
            "iva_total": sum(ivas.values(), Decimal("0")),
            "tipid": cls._tipid_emitido(identificacion),
        }

    @classmethod
    def _parsear_nota_credito_emitida_html(cls, html: str) -> dict[str, Any]:
        """Parsea una nota de crédito emitida y conserva la factura modificada."""
        factura = cls._parsear_factura_emitida_html(html)

        from bs4 import BeautifulSoup
        import re

        soup = BeautifulSoup(html, "html.parser")
        pares = []

        def txt(celda):
            return " ".join(celda.get_text(" ", strip=True).split())

        for fila in soup.find_all("tr"):
            textos = [txt(c) for c in fila.find_all(["td", "th"])]
            textos = [x for x in textos if x]
            if len(textos) >= 2:
                pares.append((" ".join(textos[:-1]), textos[-1]))

        def buscar(etiquetas):
            for etiqueta, valor in pares:
                normal = etiqueta.lower()
                if any(x in normal for x in etiquetas):
                    return valor
            return ""

        numfac = buscar([
            "número de documento modificado",
            "numero de documento modificado",
            "documento modificado",
            "factura modificada",
            "comprobante modificado",
            "documento que modifica",
        ])
        fecfac = buscar([
            "fecha de emisión documento modificado",
            "fecha de emision documento modificado",
            "fecha documento modificado",
            "fecha del documento modificado",
            "fecha comprobante modificado",
            "fecha documento que modifica",
        ])

        # Si la etiqueta trae texto adicional, extraemos el número de factura.
        match = re.search(r"\b\d{3}-\d{3}-\d{9}\b", str(numfac))
        if match:
            numfac = match.group(0)

        match_fecha = re.search(r"\b\d{2}/\d{2}/\d{4}\b", str(fecfac))
        if match_fecha:
            fecfac = match_fecha.group(0)

        if fecfac:
            try:
                fecfac = datetime.strptime(fecfac, "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                pass

        factura["numfac"] = numfac
        factura["fecfac"] = fecfac
        return factura

    @classmethod
    def _parsear_retencion_emitida_html(cls, html: str) -> dict[str, Any]:
        """Parsea el detalle real de una retención emitida del SRI.

        El detalle del SRI tiene las columnas:
            Nro | Impuesto | Base Imponible | Porcentaje Retenido |
            Valor Retenido | Número Doc. Sustento | Fecha Doc. Sustento

        Las líneas se agrupan por Número Doc. Sustento. Dentro de cada
        documento se relaciona la primera línea IVA con la primera RENTA,
        la segunda IVA con la segunda RENTA, etc. Esto permite guardar
        correctamente retenciones múltiples sobre una misma factura.
        """
        from bs4 import BeautifulSoup
        import re

        soup = BeautifulSoup(html, "html.parser")

        def txt(celda) -> str:
            return " ".join(celda.get_text(" ", strip=True).split())

        def norm(valor: str) -> str:
            return " ".join(
                str(valor or "")
                .lower()
                .replace(":", " ")
                .replace("á", "a").replace("é", "e")
                .replace("í", "i").replace("ó", "o")
                .replace("ú", "u")
                .split()
            )

        # ---------------------------------------------------------------
        # Datos generales del comprobante de retención
        # ---------------------------------------------------------------
        pares = []
        for tabla in soup.find_all("table"):
            for fila in tabla.find_all("tr"):
                textos = [txt(x) for x in fila.find_all(["td", "th"])]
                textos = [x for x in textos if x]
                if len(textos) == 2:
                    pares.append((textos[0], textos[1]))
                elif len(textos) == 3:
                    pares.append((textos[0], textos[-1]))

        def buscar(etiquetas):
            objetivos = [norm(x) for x in etiquetas]
            for etiqueta, valor in pares:
                ne = norm(etiqueta)
                if ne in objetivos or any(ne.startswith(o + " ") for o in objetivos):
                    return valor
            return ""

        fecha_txt = buscar(["Fecha Emisión", "Fecha de Emisión"])
        mf = re.search(r"\d{2}/\d{2}/\d{4}", fecha_txt)
        if mf:
            fecha_txt = mf.group(0)
        if not fecha_txt:
            raise ValueError("La retención no contiene Fecha de Emisión.")
        fecha = datetime.strptime(fecha_txt, "%d/%m/%Y")

        def comp(valor):
            m = re.search(
                r"(\d{3})\s*[- ]\s*(\d{3})\s*[- ]\s*(\d{9})",
                str(valor or ""),
            )
            if not m:
                m = re.search(r"\b(\d{3})(\d{3})(\d{9})\b", str(valor or ""))
            return m.groups() if m else ("", "", "")

        numest = buscar(["Establecimiento"])
        numptoemi = buscar(["Punto de Emisión", "Punto Emisión"])
        numsec = buscar(["Secuencial"])

        ne, np, ns = comp(
            buscar([
                "Número de Comprobante",
                "Numero de Comprobante",
                "Comprobante",
            ])
        )
        numest, numptoemi, numsec = numest or ne, numptoemi or np, numsec or ns

        clave = buscar(["Clave de Acceso", "Clave acceso"])
        autorizacion = buscar([
            "Número de Autorización",
            "Numero de Autorización",
            "Autorización",
        ])
        ruc = buscar([
            "Identificación Sujeto Retenido",
            "Identificacion Sujeto Retenido",
            "Id de Sujeto Retenido",
            "Id Sujeto Retenido",
            "RUC Sujeto Retenido",
        ])
        nombre = buscar([
            "Razón Social Sujeto Retenido",
            "Razon Social Sujeto Retenido",
            "Sujeto Retenido",
            "Proveedor",
        ])

        # ---------------------------------------------------------------
        # Localizar la tabla REAL del detalle de retención.
        # ---------------------------------------------------------------
        tabla_objetivo = soup.find(
            "table",
            id="form-detalle-comprobante-retencion:tabla-impuestos-comprobante-retencion",
        )

        tablas_detalle = [tabla_objetivo] if tabla_objetivo is not None else []

        if not tablas_detalle:
            for tabla in soup.find_all("table"):
                filas_tabla = tabla.find_all("tr")
                for fila in filas_tabla:
                    headers = [norm(txt(x)) for x in fila.find_all(["td", "th"])]
                    joined = " | ".join(headers)
                    if (
                        "impuesto" in joined
                        and "base imponible" in joined
                        and "porcentaje retenido" in joined
                        and "valor retenido" in joined
                        and (
                            "numero doc" in joined
                            or "documento sustento" in joined
                        )
                    ):
                        tablas_detalle.append(tabla)
                        break

        if not tablas_detalle:
            raise ValueError(
                "No se encontró la tabla de impuestos del comprobante de retención."
            )

        # ---------------------------------------------------------------
        # Leer las columnas por NOMBRE, nunca por posición fija.
        # ---------------------------------------------------------------
        filas_por_doc: dict[str, list[dict[str, Any]]] = {}

        for tabla in tablas_detalle:
            filas = tabla.find_all("tr")
            encabezado_idx = None
            idx = None

            for pos, fila in enumerate(filas):
                headers = [norm(txt(x)) for x in fila.find_all(["td", "th"])]
                if not headers:
                    continue

                idx_local = {
                    "impuesto": next(
                        (i for i, x in enumerate(headers) if x == "impuesto"),
                        None,
                    ),
                    "base": next(
                        (i for i, x in enumerate(headers) if "base imponible" in x),
                        None,
                    ),
                    "por": next(
                        (
                            i for i, x in enumerate(headers)
                            if "porcentaje retenido" in x
                            or x == "porcentaje"
                            or "tarifa" in x
                        ),
                        None,
                    ),
                    "valor": next(
                        (
                            i for i, x in enumerate(headers)
                            if "valor retenido" in x
                            or x == "valor"
                        ),
                        None,
                    ),
                    "doc": next(
                        (
                            i for i, x in enumerate(headers)
                            if "numero doc" in x
                            or "numero de doc" in x
                            or "documento sustento" in x
                            or "numdoc" in x
                        ),
                        None,
                    ),
                    "fecha_doc": next(
                        (
                            i for i, x in enumerate(headers)
                            if "fecha doc" in x
                            or "fecha documento" in x
                        ),
                        None,
                    ),
                }

                if all(
                    idx_local[k] is not None
                    for k in ("impuesto", "base", "por", "valor", "doc")
                ):
                    encabezado_idx = pos
                    idx = idx_local
                    break

            if encabezado_idx is None or idx is None:
                continue

            for fila_dato in filas[encabezado_idx + 1:]:
                textos = [txt(x) for x in fila_dato.find_all(["td", "th"])]
                if not textos:
                    continue

                indices_validos = [v for v in idx.values() if v is not None]
                if not indices_validos or max(indices_validos) >= len(textos):
                    continue

                impuesto_norm = norm(textos[idx["impuesto"]])
                es_iva = impuesto_norm == "iva"
                es_renta = impuesto_norm == "renta"
                if not (es_iva or es_renta):
                    continue

                texto_doc = textos[idx["doc"]]
                md = re.search(
                    r"\d{3}\s*[- ]\s*\d{3}\s*[- ]\s*\d{9}|\b\d{15}\b",
                    texto_doc,
                )
                if not md:
                    md = re.search(
                        r"\d{3}\s*[- ]\s*\d{3}\s*[- ]\s*\d{9}|\b\d{15}\b",
                        " ".join(textos),
                    )
                if not md:
                    continue

                numdoc = re.sub(r"\D", "", md.group(0))
                if len(numdoc) != 15:
                    continue

                porcentaje = cls._dec(
                    re.sub(r"[^0-9.,-]", "", textos[idx["por"]])
                )
                valor = cls._dec(textos[idx["valor"]])
                base = cls._dec(textos[idx["base"]])

                fecha_doc = ""
                if idx["fecha_doc"] is not None:
                    fecha_doc = textos[idx["fecha_doc"]]

                filas_por_doc.setdefault(numdoc, []).append({
                    "impuesto": "IVA" if es_iva else "RENTA",
                    "base": base,
                    "porcentaje": porcentaje,
                    "valor": valor,
                    "fecha_doc_sustento": fecha_doc,
                })

        if not filas_por_doc:
            raise ValueError(
                "No se encontraron líneas IVA/RENTA en el detalle SRI."
            )

        # ---------------------------------------------------------------
        # Código histórico de TotalCounts.
        # El HTML del SRI no entrega CodRet; se deriva del % de RENTA.
        # ---------------------------------------------------------------
        codigos_renta = {
            "0": "332",
            "2": "312",
            "3": "3440",
            "10": "303",
        }

        def tasa_texto(valor: Any) -> str:
            dec = cls._dec(valor)
            if dec == dec.to_integral_value():
                return str(int(dec))
            return format(dec, "f").rstrip("0").rstrip(".")

        def codigo_renta(porcentaje: Decimal, tiene_iva: bool) -> str:
            tasa = tasa_texto(porcentaje)
            if tasa == "1":
                return "343" if tiene_iva else "310"
            return codigos_renta.get(tasa, "000")

        # ---------------------------------------------------------------
        # Relación IVA <-> RENTA:
        # mismo Número Doc. Sustento + mismo orden de aparición.
        #
        # Ejemplo:
        #   IVA 30% 47.52
        #   RENTA 2% 21.12
        #
        # produce un solo bloque.
        #
        # Si existen:
        #   IVA 30% 47.52
        #   RENTA 2% 21.12
        #   IVA 70% 10.00
        #   RENTA 1%  5.00
        #
        # produce dos bloques, ambos sobre el mismo documento de sustento.
        # ---------------------------------------------------------------
        bloques = []

        for numdoc, lineas in filas_por_doc.items():
            lineas_iva = [x for x in lineas if x["impuesto"] == "IVA"]
            lineas_renta = [x for x in lineas if x["impuesto"] == "RENTA"]

            cantidad_pares = min(len(lineas_iva), len(lineas_renta))

            for i in range(cantidad_pares):
                iva = lineas_iva[i]
                renta = lineas_renta[i]

                tasa_iva = tasa_texto(iva["porcentaje"])

                bloque = {
                    "num_doc_sustento": numdoc,
                    "codigo_retencion": codigo_renta(
                        renta["porcentaje"],
                        cls._dec(iva["valor"]) > 0,
                    ),
                    "base": renta["base"],
                    "porcentaje": renta["porcentaje"],
                    "retrenta": renta["valor"],
                    "retiva": iva["valor"],
                    "retiva_porcentajes": {
                        tasa_iva: iva["valor"]
                    },
                    "fecha_doc_sustento": renta.get("fecha_doc_sustento") or iva.get(
                        "fecha_doc_sustento"
                    ),
                }
                bloques.append(bloque)

            # Si quedan líneas RENTA sin IVA.
            for renta in lineas_renta[cantidad_pares:]:
                bloques.append({
                    "num_doc_sustento": numdoc,
                    "codigo_retencion": codigo_renta(
                        renta["porcentaje"], False
                    ),
                    "base": renta["base"],
                    "porcentaje": renta["porcentaje"],
                    "retrenta": renta["valor"],
                    "retiva": Decimal("0"),
                    "retiva_porcentajes": {},
                    "fecha_doc_sustento": renta.get("fecha_doc_sustento"),
                })

            # Si quedan líneas IVA sin RENTA.
            for iva in lineas_iva[cantidad_pares:]:
                tasa_iva = tasa_texto(iva["porcentaje"])
                bloques.append({
                    "num_doc_sustento": numdoc,
                    "codigo_retencion": "",
                    "base": Decimal("0"),
                    "porcentaje": Decimal("0"),
                    "retrenta": Decimal("0"),
                    "retiva": iva["valor"],
                    "retiva_porcentajes": {
                        tasa_iva: iva["valor"]
                    },
                    "fecha_doc_sustento": iva.get("fecha_doc_sustento"),
                })

        if not bloques:
            raise ValueError(
                "No se pudieron relacionar las líneas IVA/Renta del detalle SRI."
            )

        cls._iva_debug_log(
            "RETENCION EMITIDA | PARSE DETALLE | retencion=%s-%s-%s | ruc=%s | bloques=%s",
            numest,
            numptoemi,
            numsec,
            ruc,
            len(bloques),
        )
        for bloque in bloques:
            cls._iva_debug_log(
                "RETENCION EMITIDA | BLOQUE | doc=%s | iva=%s | iva_tasas=%s | "
                "codret=%s | base=%s | porret=%s | valret=%s",
                bloque["num_doc_sustento"],
                bloque["retiva"],
                bloque["retiva_porcentajes"],
                bloque["codigo_retencion"],
                bloque["base"],
                bloque["porcentaje"],
                bloque["retrenta"],
            )

        return {
            "numest": numest.strip(),
            "numptoemi": numptoemi.strip(),
            "numsec": numsec.strip(),
            "clave_acceso": clave.strip(),
            "numero_autorizacion": autorizacion.strip() or clave.strip(),
            "fecha_emision": fecha_txt,
            "fecha": fecha,
            "identificacion_sujeto_retenido": ruc.strip(),
            "razon_social_sujeto_retenido": nombre.strip(),
            "documentos_sustento": bloques,
            "retenciones_sustento": bloques,
        }

    @classmethod
    async def _consultar_emitidos(
        cls, page, anio: int, mes: int, tipo_emitido: str = "factura"
    ) -> None:
        """Abre la consulta de comprobantes emitidos.

        La consulta es la misma para facturas y retenciones. La única
        diferencia es el tipo de comprobante seleccionado en el formulario.
        """
        await page.get_by_text("Comprobantes electrónicos emitidos", exact=True).click()
        await page.locator("#frmPrincipal\\:calendarFechaDesde_input").wait_for(
            state="visible", timeout=30000
        )

        if tipo_emitido == "retencion":
            # En emitidos usamos exactamente el mismo formulario de facturas.
            # Para retenciones únicamente cambiamos el combo Tipo de comprobante.
            selects = page.locator("select")
            seleccionado = False

            for idx in range(await selects.count()):
                combo = selects.nth(idx)
                try:
                    opciones = await combo.locator("option").evaluate_all(
                        "(els) => els.map(o => ({value:o.value, text:(o.textContent || '').trim()}))"
                    )
                except Exception:
                    continue

                for opcion in opciones:
                    texto = " ".join(str(opcion["text"]).lower().split())
                    if (
                        "comprobante de retención" in texto
                        or "comprobante de retencion" in texto
                    ):
                        await combo.select_option(value=str(opcion["value"]))
                        seleccionado = True
                        break

                if seleccionado:
                    break

            if not seleccionado:
                raise RuntimeError(
                    "No se encontró la opción 'Comprobante de Retención' "
                    "en el selector Tipo de comprobante de emitidos."
                )
    @classmethod
    async def _consultar_emitidos_dia(cls, page, fecha) -> int:
        """Consulta un día concreto y devuelve el número de filas de resultados."""
        selector_fecha = "#frmPrincipal\\:calendarFechaDesde_input"
        selector_tabla = "#frmPrincipal\\:tablaCompEmitidos_data tr"

        await page.locator(selector_fecha).fill(fecha.strftime("%d/%m/%Y"))

        # Guardamos una referencia al primer resultado para poder esperar el AJAX.
        filas = page.locator(selector_tabla)
        primera_antes = ""
        try:
            if await filas.count():
                primera_antes = (await filas.first.inner_text()).strip()
        except Exception:
            pass

        await page.click("#frmPrincipal\\:btnConsultar")

        # PrimeFaces actualiza la tabla mediante AJAX. Esperamos a que termine
        # sin asumir que siempre habrá resultados.
        for _ in range(30):
            await page.wait_for_timeout(500)
            try:
                cantidad = await filas.count()
                if cantidad == 0:
                    continue
                primera_despues = (await filas.first.inner_text()).strip()
                if not primera_antes or primera_despues != primera_antes:
                    break
            except Exception:
                pass

        await page.wait_for_timeout(1000)
        return await filas.count()

    @classmethod
    async def _obtener_detalle_emitido(cls, page, fila_idx: int) -> str | None:
        fila = page.locator("#frmPrincipal\\:tablaCompEmitidos_data tr").nth(fila_idx)
        enlace = fila.locator("a").first
        if await enlace.count() == 0:
            return None
        await enlace.scroll_into_view_if_needed()
        await enlace.click(force=True)
        for _ in range(60):
            await page.wait_for_timeout(500)
            dialogs = page.locator(".ui-dialog:visible")
            for i in range(await dialogs.count()):
                dialogo = dialogs.nth(i)
                html = await dialogo.inner_html()
                if "Espere por favor" not in html and "Clave de acceso" in html:
                    boton = dialogo.locator(".ui-dialog-titlebar-close")
                    if await boton.count():
                        await boton.click()
                    return html
        return None

    @classmethod
    async def _volver_pagina_1_emitidos(cls, page) -> None:
        """Regresa explícitamente a la página 1 antes de consultar otro día.

        El SRI conserva la página actual del paginador entre consultas AJAX.
        Si un día tuvo varias páginas, el siguiente día puede arrancar desde
        la última página si no hacemos este reset explícito.
        """
        filas = page.locator("#frmPrincipal\\:tablaCompEmitidos_data tr")
        candidatos = [
            ".ui-paginator-first",
            "a.ui-paginator-first",
            "button.ui-paginator-first",
            "[class*='ui-paginator-first']",
        ]

        for selector in candidatos:
            boton = page.locator(selector).first
            if await boton.count() == 0:
                continue

            try:
                clases = (await boton.get_attribute("class") or "").lower()
                aria = (await boton.get_attribute("aria-disabled") or "").lower()
                disabled = await boton.get_attribute("disabled")

                if (
                    disabled is not None
                    or aria == "true"
                    or "ui-state-disabled" in clases
                    or "disabled" in clases
                ):
                    return

                primera_antes = ""
                try:
                    if await filas.count():
                        primera_antes = (await filas.first.inner_text()).strip()
                except Exception:
                    pass

                await boton.click()

                for _ in range(30):
                    await page.wait_for_timeout(300)
                    try:
                        if await filas.count() == 0:
                            continue
                        primera_despues = (await filas.first.inner_text()).strip()
                        if not primera_antes or primera_despues != primera_antes:
                            break
                    except Exception:
                        pass
                return
            except Exception:
                continue

        return

    @classmethod
    async def _procesar_emitidos_ventas(
        cls, page, db, result, job_id, procesadas, anio: int, mes: int,
        texto_tipo: str = "factura", codcomp: str = "18",
        destino: str = "ventas",
    ) -> None:
        """Consulta y procesa comprobantes emitidos de un tipo durante todo el mes."""
        import calendar
        from datetime import date

        ultimo_dia = calendar.monthrange(anio, mes)[1]

        # La sincronización mensual SIEMPRE comienza por el día 1.
        # No usamos MAX(fecfactur) para decidir el día inicial porque tener
        # registros del día 30 no significa que los días 1..29 hayan sido
        # procesados correctamente. Cada factura ya existente se detecta por
        # clave de acceso, por lo que volver a recorrer el mes es seguro.
        dia_inicial = 1

        for dia in range(dia_inicial, ultimo_dia + 1):
            cls._verificar_cancelacion(job_id)
            fecha_consulta = date(anio, mes, dia)
            cls._job_update(
                job_id,
                mensaje=f"Consultando comprobantes emitidos del {fecha_consulta.strftime('%d/%m/%Y')}.",
            )

            cantidad_inicial = await cls._consultar_emitidos_dia(page, fecha_consulta)
            if cantidad_inicial == 0:
                continue

            pagina = 1
            while True:
                cls._verificar_cancelacion(job_id)
                result["paginas"] += 1
                cls._job_update(
                    job_id,
                    mensaje=(
                        f"Procesando {texto_tipo}s emitidas del {fecha_consulta.strftime('%d/%m/%Y')} "
                        f"(página {pagina})."
                    ),
                    paginas=result["paginas"],
                )

                filas = page.locator("#frmPrincipal\\:tablaCompEmitidos_data tr")
                cantidad = await filas.count()
                if cantidad == 0:
                    break

                # Procesamos una copia de los índices actuales. Abrir/cerrar el
                # detalle no debe cambiar la cantidad de filas de la página.
                for idx in range(cantidad):
                    cls._verificar_cancelacion(job_id)
                    try:
                        fila = filas.nth(idx)
                        columnas = await fila.locator("td").all_inner_texts()

                        # El SRI suele devolver: "Factura 001-102-0000260".
                        tipo_texto = (
                            " ".join(columnas[1].strip().split())
                            if len(columnas) >= 2 else ""
                        )
                        tipo_normalizado = tipo_texto.lower()
                        if texto_tipo == "factura":
                            es_tipo = (
                                tipo_texto == "01"
                                or tipo_normalizado.startswith("factura")
                                or " factura " in f" {tipo_normalizado} "
                            )
                        else:
                            es_tipo = (
                                tipo_normalizado.startswith(texto_tipo)
                                or f" {texto_tipo} " in f" {tipo_normalizado} "
                            )
                        if tipo_texto and not es_tipo:
                            continue

                        html = await cls._obtener_detalle_emitido(page, idx)
                        if not html:
                            continue

                        if destino == "ncventas":
                            factura = cls._parsear_nota_credito_emitida_html(html)
                        else:
                            factura = cls._parsear_factura_emitida_html(html)

                        # Seguridad adicional: si el SRI conserva temporalmente
                        # la tabla anterior después de un AJAX, nunca guardamos
                        # una factura de otro día.
                        if factura["fecha"].date() != fecha_consulta:
                            continue

                        clave = factura["clave_acceso"].strip()
                        if not clave:
                            raise ValueError("La factura emitida no contiene clave de acceso.")

                        result["sri"] += 1

                        if clave in procesadas:
                            result["ya_existentes"] += 1
                            continue

                        procesadas.add(clave)

                        if destino == "ncventas":
                            existe = db.execute(text("""
                                SELECT 1 FROM ncventas
                                WHERE TRIM(autorizacion::text) = :clave
                                LIMIT 1
                            """), {"clave": clave}).first()
                        else:
                            existe = db.execute(text("""
                                SELECT 1 FROM ventas
                                WHERE TRIM(autorizacion::text) = :clave
                                LIMIT 1
                            """), {"clave": clave}).first()

                        if existe:
                            result["ya_existentes"] += 1
                            continue

                        if destino == "ncventas":
                            cls._insertar_nota_credito(db, factura)
                        else:
                            cls._insertar_venta(db, factura, codcomp=codcomp)
                        db.commit()
                        result["descargadas"] += 1
                        result["guardadas"] += 1

                        cls._job_update(
                            job_id,
                            sri=result["sri"],
                            guardadas=result["guardadas"],
                            descargadas=result["descargadas"],
                            ya_existentes=result["ya_existentes"],
                            mensaje=f"{texto_tipo.title()} emitida {result['sri']} procesada.",
                        )

                    except Exception as exc:
                        db.rollback()
                        result["errores"].append({
                            "fecha": fecha_consulta.isoformat(),
                            "pagina": pagina,
                            "fila": idx + 1,
                            "detalle": str(exc),
                        })
                        cls._job_update(
                            job_id,
                            errores=result["errores"],
                            mensaje=(
                                f"Error factura emitida {fecha_consulta.strftime('%d/%m/%Y')} "
                                f"fila {idx + 1}: {exc}"
                            ),
                        )

                boton_next = page.locator("[class*='ui-paginator-next']").first
                if await boton_next.count() == 0:
                    break

                clases = (await boton_next.get_attribute("class") or "").lower()
                if "ui-state-disabled" in clases:
                    break

                try:
                    primera = await filas.first.inner_text()
                except Exception:
                    primera = ""

                await boton_next.click()

                # Esperamos el cambio de página.
                for _ in range(30):
                    await page.wait_for_timeout(500)
                    try:
                        if not primera or await filas.first.inner_text() != primera:
                            break
                    except Exception:
                        pass

                pagina += 1

            # Si este día tuvo más de una página, el SRI deja el paginador
            # en la última página. Antes de cambiar al siguiente día debemos
            # regresar SIEMPRE a la página 1 para que la nueva consulta no
            # herede la página anterior.
            await cls._volver_pagina_1_emitidos(page)

    @classmethod
    async def _procesar_emitidos_retenciones(cls, page, db, result, job_id, procesadas, anio: int, mes: int) -> None:
        """Procesa retenciones emitidas y actualiza las compras afectadas."""
        import calendar
        from datetime import date

        ultimo_dia = calendar.monthrange(anio, mes)[1]
        for dia in range(1, ultimo_dia + 1):
            cls._verificar_cancelacion(job_id)
            fecha_consulta = date(anio, mes, dia)
            cls._job_update(job_id, mensaje=f"Consultando retenciones emitidas del {fecha_consulta.strftime('%d/%m/%Y')}.")
            if await cls._consultar_emitidos_dia(page, fecha_consulta) == 0:
                continue

            pagina = 1
            while True:
                cls._verificar_cancelacion(job_id)
                result["paginas"] += 1
                cls._job_update(job_id, paginas=result["paginas"], mensaje=f"Procesando retenciones emitidas del {fecha_consulta.strftime('%d/%m/%Y')} (página {pagina}).")
                filas = page.locator("#frmPrincipal\\:tablaCompEmitidos_data tr")
                cantidad = await filas.count()

                for idx in range(cantidad):
                    cls._verificar_cancelacion(job_id)
                    try:
                        columnas = await filas.nth(idx).locator("td").all_inner_texts()
                        tipo = " ".join(columnas[1].strip().split()).lower() if len(columnas) >= 2 else ""
                        # El SRI puede mostrar el tipo como "07", "Retención"
                        # o "Comprobante de Retención 001-001-000000001".
                        # En este último caso "retención" no está al inicio.
                        tipo_sin_tilde = tipo.replace("retención", "retencion")
                        es_retencion = (
                            tipo == "07"
                            or tipo.startswith("retención")
                            or tipo.startswith("retencion")
                            or " retención " in f" {tipo} "
                            or " retencion " in f" {tipo_sin_tilde} "
                            or "comprobante de retencion" in tipo_sin_tilde
                        )
                        if tipo and not es_retencion:
                            continue

                        html = await cls._obtener_detalle_emitido(page, idx)
                        if not html:
                            continue
                        retencion = cls._parsear_retencion_emitida_html(html)
                        if retencion["fecha"].date() != fecha_consulta:
                            continue

                        clave = retencion["clave_acceso"].strip()
                        if not clave:
                            raise ValueError("La retención emitida no contiene clave de acceso.")
                        if clave in procesadas:
                            result["ya_existentes"] += 1
                            continue
                        procesadas.add(clave)
                        result["sri"] += 1

                        actualizadas = cls._actualizar_retencion_emitida_compras(db, retencion)
                        db.commit()
                        result["descargadas"] += 1
                        result["guardadas"] += actualizadas
                        cls._job_update(
                            job_id, sri=result["sri"], guardadas=result["guardadas"],
                            descargadas=result["descargadas"], ya_existentes=result["ya_existentes"],
                            mensaje=f"Retención emitida {retencion['numest']}-{retencion['numptoemi']}-{retencion['numsec']} procesada.",
                        )
                    except Exception as exc:
                        db.rollback()
                        result["errores"].append({"fecha": fecha_consulta.isoformat(), "pagina": pagina, "fila": idx + 1, "detalle": str(exc)})
                        cls._job_update(job_id, errores=result["errores"], mensaje=f"Error retención emitida {fecha_consulta.strftime('%d/%m/%Y')} fila {idx + 1}: {exc}")

                boton_next = page.locator("[class*='ui-paginator-next']").first
                if await boton_next.count() == 0:
                    break
                clases = (await boton_next.get_attribute("class") or "").lower()
                if "ui-state-disabled" in clases:
                    break
                try:
                    primera = await filas.first.inner_text()
                except Exception:
                    primera = ""
                await boton_next.click()
                for _ in range(30):
                    await page.wait_for_timeout(500)
                    try:
                        if not primera or await filas.first.inner_text() != primera:
                            break
                    except Exception:
                        pass
                pagina += 1

            await cls._volver_pagina_1_emitidos(page)

    @classmethod
    def _actualizar_retencion_emitida_compras(cls, db, retencion: dict[str, Any]) -> int:
        """Actualiza comprasnue con una retención emitida por el SRI.

        La factura se localiza por el Número Doc. Sustento del detalle SRI:
            3 dígitos establecimiento + 3 punto emisión + 9 secuencial
        y por el RUC del sujeto retenido (ruccedprovee).

        Luego se guarda en esa compra:
        - numestret / numptoemiret / numsecret: número de la RETENCIÓN.
        - numautret: clave de acceso de la RETENCIÓN.
        - fecret: fecha de emisión de la RETENCIÓN.
        - IVA: valor de cada línea IVA en retencioniva10/20/30/70/100.
        - RENTA: CodRet, BaseImpRet, PorRet y ValRet.

        La retención actualiza los registros de comprasnue que ya existen.
        No se crean registros nuevos para aplicar una retención.
        """
        import re

        bloques = retencion.get("retenciones_sustento") or retencion.get(
            "documentos_sustento"
        ) or []
        if not bloques:
            raise ValueError("La retención emitida no contiene líneas de sustento.")

        numret = (
            f"{retencion['numest']}-{retencion['numptoemi']}-"
            f"{retencion['numsec']}"
        )
        autret = str(
            retencion.get("numero_autorizacion")
            or retencion.get("clave_acceso")
            or ""
        ).strip()
        fecret = cls._fecha_varchar(retencion["fecha"])
        ruc = str(
            retencion.get("identificacion_sujeto_retenido") or ""
        ).strip()

        iva_fields = {
            "10": "retencioniva10",
            "20": "retencioniva20",
            "30": "retencioniva30",
            "70": "retencioniva70",
            "100": "retencioniva100",
        }

        retencion_fields = (
            "numestret", "numptoemiret", "numsecret", "numautret", "fecret",
            "codret", "baseimpret", "porret", "valret",
            "retencioniva10", "retencioniva20", "retencioniva30",
            "retencioniva70", "retencioniva100",
        )

        def normalizar_numdoc(valor: Any) -> str:
            digitos = re.sub(r"\D", "", str(valor or ""))
            return digitos if len(digitos) == 15 else ""

        def partes_factura(numdoc: str) -> tuple[str, str, str]:
            return numdoc[:3], numdoc[3:6], numdoc[6:]

        def buscar_compras(numdoc: str) -> list[dict[str, Any]]:
            """Busca la factura por Número Doc. Sustento + RUC proveedor."""
            ne, np, ns = partes_factura(numdoc)

            rows = db.execute(text("""
                SELECT *
                FROM comprasnue
                WHERE REPLACE(REPLACE(REPLACE(TRIM(numest::text), '-', ''), ' ', ''), '.', '') = :numest
                  AND REPLACE(REPLACE(REPLACE(TRIM(numptoemi::text), '-', ''), ' ', ''), '.', '') = :numptoemi
                  AND REPLACE(REPLACE(REPLACE(TRIM(numsec::text), '-', ''), ' ', ''), '.', '') = :numsec
                  AND TRIM(ruccedprovee::text) = TRIM(:ruc)
                  AND TRIM(tipcom::text) IN ('01', '03')
                ORDER BY numcompra ASC NULLS LAST
            """), {
                "numest": ne,
                "numptoemi": np,
                "numsec": ns,
                "ruc": ruc,
            }).mappings().all()

            return [dict(row) for row in rows]

        def normalizar_tasa(valor: Any) -> str:
            dec = cls._dec(valor)
            if dec == dec.to_integral_value():
                return str(int(dec))
            return format(dec, "f").rstrip("0").rstrip(".")

        def valores_iva_esperados(bloque: dict[str, Any]) -> dict[str, Decimal]:
            esperados = {
                "10": Decimal("0"),
                "20": Decimal("0"),
                "30": Decimal("0"),
                "70": Decimal("0"),
                "100": Decimal("0"),
            }

            for tasa, valor in (bloque.get("retiva_porcentajes") or {}).items():
                tasa_norm = normalizar_tasa(tasa)
                if tasa_norm in esperados:
                    esperados[tasa_norm] += cls._dec(valor)

            return esperados

        def fila_ya_es_esta_linea(
            row: dict[str, Any],
            bloque: dict[str, Any],
        ) -> bool:
            if str(row.get("numautret") or "").strip() != autret:
                return False

            # La renta es el identificador más fuerte de cada línea
            # cuando la retención ya fue registrada.
            if abs(
                cls._dec(row.get("valret"))
                - cls._dec(bloque.get("retrenta"))
            ) > Decimal("0.0001"):
                return False

            return True

        def clonar_compra(template: dict[str, Any]) -> dict[str, Any]:
            valores = {
                k: v for k, v in template.items()
                if k in columnas_clon and (k not in pk or k == "numcompra")
            }
            valores["numcompra"] = nuevo_numcompra()

            # El clon conserva absolutamente todos los datos de la factura.
            # Solamente se limpian los campos que pertenecen a la retención.
            for campo in retencion_fields:
                if campo in valores:
                    if campo in {
                        "baseimpret", "porret", "valret",
                        "retencioniva10", "retencioniva20",
                        "retencioniva30", "retencioniva70",
                        "retencioniva100",
                    }:
                        valores[campo] = Decimal("0")
                    else:
                        valores[campo] = ""

            nombres = ", ".join(
                f'"{col}"' if col == "año" else col
                for col in valores
            )
            parametros = ", ".join(f":{col}" for col in valores)

            db.execute(
                text(
                    f"INSERT INTO comprasnue ({nombres}) "
                    f"VALUES ({parametros})"
                ),
                valores,
            )

            nuevo = dict(template)
            nuevo.update(valores)
            return nuevo

        def actualizar_fila(
            row: dict[str, Any],
            bloque: dict[str, Any],
        ) -> None:
            iva_values = {
                "10": Decimal("0"),
                "20": Decimal("0"),
                "30": Decimal("0"),
                "70": Decimal("0"),
                "100": Decimal("0"),
            }

            # El porcentaje de IVA determina DIRECTAMENTE el campo destino.
            # Ejemplo: IVA 30.0 -> retencioniva30 = valor retenido.
            for tasa, valor in (bloque.get("retiva_porcentajes") or {}).items():
                tasa_norm = normalizar_tasa(tasa)
                if tasa_norm in iva_values:
                    iva_values[tasa_norm] += cls._dec(valor)

            params = {
                "numcompra": row["numcompra"],

                # Número del comprobante de RETENCIÓN.
                "numestret": str(retencion["numest"]).strip(),
                "numptoemiret": str(retencion["numptoemi"]).strip(),
                "numsecret": str(retencion["numsec"]).strip(),

                # Datos generales de la RETENCIÓN.
                "numautret": autret,
                "fecret": cls._fecha_varchar(fecret),

                # Datos de RENTA.
                "codret": str(bloque.get("codigo_retencion") or "").strip(),
                "baseimpret": cls._dec(bloque.get("base")),
                "porret": cls._dec(bloque.get("porcentaje")),
                "valret": cls._dec(bloque.get("retrenta")),

                # Datos de IVA.
                "retencioniva10": iva_values["10"],
                "retencioniva20": iva_values["20"],
                "retencioniva30": iva_values["30"],
                "retencioniva70": iva_values["70"],
                "retencioniva100": iva_values["100"],
            }

            db.execute(text("""
                UPDATE comprasnue
                SET numestret = :numestret,
                    numptoemiret = :numptoemiret,
                    numsecret = :numsecret,
                    numautret = :numautret,
                    fecret = :fecret,
                    codret = :codret,
                    baseimpret = :baseimpret,
                    porret = :porret,
                    valret = :valret,
                    retencioniva10 = :retencioniva10,
                    retencioniva20 = :retencioniva20,
                    retencioniva30 = :retencioniva30,
                    retencioniva70 = :retencioniva70,
                    retencioniva100 = :retencioniva100
                WHERE numcompra = :numcompra
            """), params)

            cls._iva_debug_log(
                "RETENCION EMITIDA | UPDATE | compra=%s | retencion=%s | "
                "doc_sustento=%s | ruc=%s | "
                "numestret=%s | numptoemiret=%s | numsecret=%s | "
                "codret=%s | base=%s | porret=%s | valret=%s | "
                "iva10=%s | iva20=%s | iva30=%s | iva70=%s | iva100=%s",
                row["numcompra"],
                numret,
                bloque.get("num_doc_sustento"),
                ruc,
                params["numestret"],
                params["numptoemiret"],
                params["numsecret"],
                params["codret"],
                params["baseimpret"],
                params["porret"],
                params["valret"],
                params["retencioniva10"],
                params["retencioniva20"],
                params["retencioniva30"],
                params["retencioniva70"],
                params["retencioniva100"],
            )

        actualizadas = 0
        no_encontradas = []

        # Agrupamos los bloques por documento sustento para que, si una misma
        # factura tiene dos o más rentas, podamos crear los registros
        # adicionales de forma ordenada.
        bloques_por_doc: dict[str, list[dict[str, Any]]] = {}
        for bloque in bloques:
            numdoc = normalizar_numdoc(bloque.get("num_doc_sustento"))
            if not numdoc:
                continue
            bloques_por_doc.setdefault(numdoc, []).append(bloque)

        for numdoc, bloques_doc in bloques_por_doc.items():
            filas = buscar_compras(numdoc)

            if not filas:
                no_encontradas.append(numdoc)
                continue

            filas_trabajo = list(filas)

            for bloque in bloques_doc:
                # Primero buscamos un registro que ya tenga esta misma retención.
                # Esto permite corregir/reemplazar un código anterior sin crear
                # una segunda fila.
                fila_objetivo = next(
                    (
                        row for row in filas_trabajo
                        if fila_ya_es_esta_linea(row, bloque)
                    ),
                    None,
                )

                if fila_objetivo is None:
                    # Las compras nuevas quedan inicialmente con codret=332.
                    # Ese 332 es solo un código provisional. La retención SRI
                    # debe reemplazarlo EN LA MISMA FILA.
                    fila_objetivo = next(
                        (
                            row for row in filas_trabajo
                            if str(row.get("codret") or "").strip() in {"", "332"}
                        ),
                        None,
                    )

                if fila_objetivo is None:
                    # Nunca debemos aplicar una retención a otra compra.
                    # Si el SRI indica un documento sustento/RUC para el que
                    # no existe una fila elegible, se deja sin aplicar.
                    logger.warning(
                        "RETENCION EMITIDA | sin fila elegible | "
                        "retencion=%s | doc_sustento=%s | ruc=%s | "
                        "filas_encontradas=%s",
                        numret,
                        numdoc,
                        ruc,
                        len(filas_trabajo),
                    )
                    no_encontradas.append(
                        f"{numdoc} (sin fila elegible para reemplazo)"
                    )
                    continue

                actualizar_fila(fila_objetivo, bloque)
                actualizadas += 1

        if actualizadas == 0:
            detalle = ", ".join(no_encontradas) or "sin número de factura"
            raise ValueError(
                f"No se encontró en comprasnue ninguna factura para la retención "
                f"emitida {numret}. Documentos de sustento: {detalle}."
            )

        if no_encontradas:
            logger.warning(
                "RETENCION EMITIDA | algunas facturas no fueron encontradas | "
                "retencion=%s | facturas=%s",
                numret,
                ", ".join(no_encontradas),
            )

        logger.info(
            "RETENCION EMITIDA | registrada | retencion=%s | bloques=%s | "
            "registros=%s | ruc=%s",
            numret,
            len(bloques),
            actualizadas,
            ruc,
        )
        return actualizadas

    @classmethod
    def _insertar_nota_credito(cls, db, factura: dict[str, Any]) -> None:
        """Guarda una nota de crédito emitida con la estructura completa de ncventas.

        La autorización de la factura modificada se obtiene desde ventas usando
        el número de factura (numfac). Si SRI no entrega la fecha modificada,
        se usa como respaldo la fecha registrada en ventas.
        """
        b = factura["bases_iva"]
        i = factura["ivas"]

        numnc = (
            f"{factura['establecimiento']}-"
            f"{factura['punto_emision']}-"
            f"{factura['secuencial']}"
        )
        numfac = str(factura.get("numfac") or "").strip()

        # La NC debe enlazarse con la factura original registrada en ventas.
        autfac = ""
        fecfac = factura.get("fecfac") or ""

        if numfac:
            factura_original = db.execute(text("""
                SELECT autorizacion, fecfactur
                FROM ventas
                WHERE TRIM(numfactur::text) = TRIM(:numfac)
                LIMIT 1
            """), {"numfac": numfac}).mappings().first()

            if factura_original:
                autfac = str(factura_original["autorizacion"] or "").strip()
                if not fecfac and factura_original["fecfactur"]:
                    fecfac = factura_original["fecfactur"]
            else:
                logger.warning(
                    "NC EMITIDA | no se encontró factura modificada en ventas | "
                    "numnc=%s | numfac=%s",
                    numnc,
                    numfac,
                )

        values = {
            "numnc": numnc,
            "autorizacion": factura["clave_acceso"],
            "fecnc": factura["fecha"].strftime("%Y-%m-%d"),
            "ruccedcli": factura["identificacion"],
            "nomcli": factura["razon_social"],
            "tipid": factura.get("tipid") or cls._tipid_emitido(factura["identificacion"]),
            "codcomp": "04",
            "numemi": 1,
            "basenoobj": factura["base_no_objeto"],
            "baseiva0": factura["base_iva0"],
            "baseiva12": b["12"],
            "iva": sum(i.values(), Decimal("0")),
            "ice": Decimal("0"),
            "numfac": numfac,
            "autfac": autfac,
            "fecfac": fecfac or None,
            "mes": f"{factura['fecha'].month:02d}",
            "año": str(factura["fecha"].year),
        }

        cols = ", ".join(
            f'"{k}"' if k == "año" else k
            for k in values
        )
        params = ", ".join(f":{k}" for k in values)

        db.execute(
            text(f"INSERT INTO ncventas ({cols}) VALUES ({params})"),
            values,
        )

    @classmethod
    def _insertar_venta(cls, db, factura: dict[str, Any], codcomp: str = "18") -> None:
        b = factura["bases_iva"]
        i = factura["ivas"]
        values = {
            "numfactur": f"{factura['establecimiento']}-{factura['punto_emision']}-{factura['secuencial']}",
            "autorizacion": factura["clave_acceso"],
            "fecfactur": cls._fecha_varchar(factura["fecha"]),
            "ruccedcli": factura["identificacion"],
            "nomcli": factura["razon_social"],
            "tipid": factura["tipid"],
            "codcomp": codcomp,
            "numemi": "1",
            "basenoobj": factura["base_no_objeto"],
            "baseiva0": factura["base_iva0"],
            "baseiva12": b["12"],
            "baseiva5": b["5"],
            "baseiva8": b["8"],
            "baseiva14": b["14"],
            "baseiva15": b["15"],
            "iva": Decimal("0"),
            "iva5": i["5"],
            "iva8": i["8"],
            "iva12": i["12"],
            "iva14": i["14"],
            "iva15": i["15"],
            "ice": Decimal("0"),
            "numret": "",
            "autret": "",
            "fecret": "",
            "retiva": Decimal("0"),
            "retrenta": Decimal("0"),
            "mes": f"{factura['fecha'].month:02d}",
            "año": str(factura["fecha"].year),
            "numasiento": "",
        }
        cols = ", ".join(f'"{k}"' if k == "año" else k for k in values)
        params = ", ".join(f":{k}" for k in values)
        db.execute(text(f"INSERT INTO ventas ({cols}) VALUES ({params})"), values)

    @classmethod
    async def _consultar_recibidos(cls, page, anio: int, mes: int, tipo_comprobante: int) -> None:
        """Consulta recibidos y espera la tabla AJAX, no los enlaces XML.

        El SRI ejecuta dos llamadas al pulsar Consultar: una inicial sin token
        y otra desde rcBuscar() con el token de reCAPTCHA. La segunda llamada
        es la que llena tablaCompRecibidos. Por eso el criterio de éxito es que
        la tabla tenga filas, no que ya existan enlaces .xml en el DOM.
        """
        campos = {
            "ano": str(anio),
            "mes": str(mes),
            "dia": "0",
            "cmbTipoComprobante": str(tipo_comprobante),
        }

        boton_selectores = [
            "#frmPrincipal\\:btnBuscar",
            "button[id$=':btnBuscar']",
            "#frmPrincipal\\:btnConsultarSinRe",
            "input[id$=':btnConsultarSinRe']",
            "button[id$=':btnConsultarSinRe']",
            "input[value*='Consultar']",
            "button:has-text('Consultar')",
        ]

        tabla_selector = "#frmPrincipal\\:tablaCompRecibidos"

        async def preparar_formulario():
            for nombre, value in campos.items():
                selector = f"#frmPrincipal\\:{nombre}"

                if nombre == "cmbTipoComprobante":
                    # El SRI ha cambiado el value interno del combo en algunas
                    # versiones. Para retenciones (07) no dependemos únicamente
                    # de value="7": buscamos también la opción por su texto.
                    combo = page.locator(selector)
                    await combo.wait_for(state="visible", timeout=30000)

                    seleccionado = False
                    try:
                        opciones = await combo.locator("option").evaluate_all(
                            "(els) => els.map(o => ({value:o.value, text:(o.textContent || '').trim()}))"
                        )
                    except Exception:
                        opciones = []

                    for opcion in opciones:
                        texto_opcion = " ".join(str(opcion["text"]).lower().split())
                        valor_opcion = str(opcion["value"]).strip()
                        if valor_opcion == str(value):
                            await combo.select_option(value=valor_opcion)
                            seleccionado = True
                            break

                    if not seleccionado:
                        etiquetas = {
                            "7": ("retencion", "retención", "comprobante de retención"),
                            "4": ("nota de crédito", "nota crédito"),
                            "1": ("factura",),
                        }
                        candidatos = etiquetas.get(str(value), ())
                        for opcion in opciones:
                            texto_opcion = " ".join(str(opcion["text"]).lower().split())
                            if any(candidato in texto_opcion for candidato in candidatos):
                                await combo.select_option(value=str(opcion["value"]))
                                seleccionado = True
                                print(
                                    "SRI tipo comprobante: value=%s seleccionado por texto=%r"
                                    % (opcion["value"], opcion["text"])
                                )
                                break

                    if not seleccionado:
                        raise RuntimeError(
                            "El combo Tipo de Comprobante del SRI no contiene la opción "
                            f"solicitada ({value}). Opciones disponibles: {opciones}"
                        )
                else:
                    await cls._seleccionar(page, selector, value)

            boton = None
            for selector in boton_selectores:
                locator = page.locator(selector).first
                if await locator.count() == 0:
                    continue
                try:
                    await locator.wait_for(state="visible", timeout=5000)
                    boton = locator
                    break
                except PlaywrightTimeoutError:
                    continue

            if boton is None:
                diagnostico = await cls._diagnostico_consulta(page)
                raise RuntimeError(
                    "SRI cargó la pantalla de comprobantes recibidos, pero no apareció "
                    f"el botón Consultar. {diagnostico}"
                )

            return boton

        async def esperar_boton_habilitado(boton, segundos: int = 30) -> bool:
            limite = time.monotonic() + segundos
            while time.monotonic() < limite:
                try:
                    if not await boton.is_disabled():
                        disabled_attr = await boton.get_attribute("disabled")
                        classes = (await boton.get_attribute("class") or "").lower()
                        if disabled_attr is None and "disabled" not in classes:
                            return True
                except Exception:
                    pass
                await page.wait_for_timeout(500)
            return False

        async def contar_filas() -> int:
            """Cuenta filas de datos, ignorando encabezados y paginadores."""
            try:
                return await page.locator(
                    f"{tabla_selector} tbody tr"
                ).count()
            except Exception:
                return 0

        async def esperar_resultado(segundos: int = 45) -> int:
            """Espera a que PrimeFaces termine de pintar tablaCompRecibidos."""
            limite = time.monotonic() + segundos
            ultima = 0

            while time.monotonic() < limite:
                filas = await contar_filas()
                if filas > 0:
                    return filas

                # Algunas versiones del SRI no mantienen tbody de forma estable.
                # El texto de la tabla permite detectar igualmente que llegó el AJAX.
                try:
                    texto = await page.locator(tabla_selector).inner_text(timeout=1000)
                    normalizado = " ".join(texto.split())
                    if (
                        "RUC y Razón social emisor" in normalizado
                        and ("Factura " in normalizado or "Clave de acceso" in normalizado)
                    ):
                        return max(1, await contar_filas())
                except Exception:
                    pass

                await page.wait_for_timeout(500)
                ultima = filas

            return ultima

        # El SRI puede tardar en construir frmPrincipal después de la
        # navegación, e incluso puede responder primero con una página
        # intermedia. Confirmamos el formulario antes de tocar año/mes/día.
        selector_ano = "#frmPrincipal\\:ano"
        ultimo_diagnostico = ""

        for intento_navegacion in range(3):
            try:
                await page.goto(
                    cls.RECIBIDOS_URL,
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
            except PlaywrightTimeoutError:
                # Una navegación que excede el timeout puede haber terminado
                # parcialmente; verificamos el DOM antes de descartarla.
                pass

            try:
                await page.wait_for_load_state("load", timeout=20000)
            except Exception:
                pass

            selector = page.locator(selector_ano).first
            try:
                await selector.wait_for(state="attached", timeout=15000)
                await selector.wait_for(state="visible", timeout=15000)
                break
            except PlaywrightTimeoutError:
                ultimo_diagnostico = await cls._diagnostico_consulta(page)
                logger.warning(
                    "SRI RECIBIDOS | formulario aún no disponible | intento=%s | %s",
                    intento_navegacion + 1,
                    ultimo_diagnostico,
                )
                if intento_navegacion < 2:
                    await page.wait_for_timeout(2000 * (intento_navegacion + 1))
                    continue
                raise RuntimeError(
                    "El SRI no mostró el formulario de comprobantes recibidos "
                    "con el campo de año (#frmPrincipal:ano). "
                    + ultimo_diagnostico
                )

        await page.wait_for_timeout(1000)

        # El flujo oficial del SRI debe ser iniciado por el botón.
        # No ejecutamos rcBuscar() manualmente: hacerlo antes del click puede
        # generar un token reCAPTCHA que expire o quede consumido antes de que
        # el SRI procese la consulta, provocando "Captcha incorrecta".
        boton = await preparar_formulario()

        if not await esperar_boton_habilitado(boton, segundos=30):
            diagnostico = await cls._diagnostico_consulta(page)
            try:
                await page.screenshot(
                    path=str(Path(tempfile.gettempdir()) / f"conta_sri_bloqueado_{anio}_{mes:02d}.png"),
                    full_page=True,
                )
            except Exception:
                pass
            raise RuntimeError(
                "SRI mantuvo el botón Consultar deshabilitado. "
                + diagnostico
            )

        # Ejecutamos el click real sobre el botón oficial. Esto permite que
        # el JavaScript del SRI genere el token reCAPTCHA justo antes de la
        # petición AJAX definitiva.
        try:
            await boton.scroll_into_view_if_needed()
            await boton.click(force=True)
        except Exception as exc:
            diagnostico = await cls._diagnostico_consulta(page)
            raise RuntimeError(
                "No fue posible ejecutar el botón Consultar del SRI. "
                + diagnostico
            ) from exc

        filas = await esperar_resultado(segundos=45)
        if filas <= 0:
            # El SRI puede responder correctamente con una tabla vacía cuando
            # no existen comprobantes para el año/mes/tipo solicitado.
            # Eso NO es un error de sincronización. Antes se lanzaba una
            # excepción aquí y el job quedaba como error aunque la consulta
            # hubiera terminado correctamente.
            try:
                texto = await page.locator("body").inner_text(timeout=3000)
            except Exception:
                texto = ""
            texto_normalizado = " ".join(texto.split()).lower()

            if (
                "no existen datos para los parámetros ingresados" in texto_normalizado
                or "no existen datos para los parametros ingresados" in texto_normalizado
            ):
                print(
                    f"SRI consulta completada sin comprobantes: "
                    f"anio={anio}, mes={mes}, tipo={tipo_comprobante}."
                )
                return 0

            diagnostico = await cls._diagnostico_consulta(page)
            try:
                await page.screenshot(
                    path=str(Path(tempfile.gettempdir()) / f"conta_sri_sin_resultado_{anio}_{mes:02d}.png"),
                    full_page=True,
                )
            except Exception:
                pass
            raise RuntimeError(
                "El SRI ejecutó la consulta pero no llenó tablaCompRecibidos. "
                + diagnostico
            )

        print(f"SRI consulta completada: {filas} filas detectadas en tablaCompRecibidos.")
        return filas
    @staticmethod
    async def _diagnostico_consulta(page) -> str:
        try:
            url = page.url
            title = await page.title()
            body = (await page.locator("body").inner_text(timeout=3000))[:1500]
            body = " ".join(body.split())
            captcha = await page.locator(
                "iframe[src*='recaptcha'], iframe[title*='reCAPTCHA'], "
                "[class*='captcha'], [id*='captcha']"
            ).count()
            boton = await page.locator(
                "#frmPrincipal\\:btnBuscar, "
                "button[id$=':btnBuscar'], "
                "#frmPrincipal\\:btnConsultarSinRe, "
                "input[id$=':btnConsultarSinRe'], button[id$=':btnConsultarSinRe']"
            ).count()
            return (
                f"URL={url}; título={title!r}; botón_consultar={boton}; "
                f"captcha_elementos={captcha}; texto={body!r}"
            )
        except Exception as exc:
            return f"Diagnóstico adicional no disponible: {exc}"

    @classmethod
    async def _siguiente_pagina(cls, page) -> bool:
        candidatos = [
            ".rf-pg-btn.rf-pg-btn-next",
            "input.rf-pg-btn-next",
            "a.rf-pg-btn-next",
            ".ui-paginator-next",
            "a[title*='Siguiente']",
            "button[title*='Siguiente']",
        ]
        for selector in candidatos:
            locator = page.locator(selector).first
            if await locator.count() == 0:
                continue
            try:
                disabled = await locator.get_attribute("disabled")
                classes = (await locator.get_attribute("class") or "").lower()
                aria = (await locator.get_attribute("aria-disabled") or "").lower()
                if disabled is not None or "disabled" in classes or aria == "true":
                    return False
                await locator.click()
                await page.wait_for_timeout(1200)
                return True
            except Exception:
                continue
        return False

    @classmethod
    async def sincronizar_mes(cls, ruc: str, anio: int, mes: int, tipo_comprobante: int = 1, job_id: str | None = None, operacion: str = "compras") -> dict[str, Any]:
        _iva_debug_log(
            "SINCRONIZAR MES | ruc=%s | anio=%s | mes=%s | tipo=%s | operacion=%s | servicio=%s",
            ruc, anio, mes, tipo_comprobante, operacion, str(Path(__file__).resolve()),
        )
        cred = cls._credenciales(ruc)
        p = browser = context = page = chrome_process = None
        result = {
            "ruc": ruc, "cliente": cred["nombre"], "anio": anio, "mes": mes,
            "tipo_comprobante": cls._tipo(tipo_comprobante),
            "sri": 0, "ya_existentes": 0, "descargadas": 0, "guardadas": 0,
            "errores": [], "paginas": 0,
        }
        try:
            cls._job_update(job_id, mensaje="Abriendo sesión del SRI.")
            p, browser, context, page, chrome_process = await cls._login(ruc, cred["clave"])
            cls._job_update(job_id, estado="captcha", mensaje="Consultando comprobantes en el SRI. Si aparece CAPTCHA, resuélvalo en Chromium.")
            if operacion in ("ventas", "notas_credito_emitidas", "retenciones_emitidas"):
                await cls._consultar_emitidos(
                    page,
                    anio,
                    mes,
                    tipo_emitido="retencion" if operacion == "retenciones_emitidas" else "factura",
                )
            else:
                filas_recibidos = await cls._consultar_recibidos(
                    page, anio, mes, tipo_comprobante
                )
                if filas_recibidos == 0:
                    result["mensaje"] = (
                        "El SRI terminó la consulta y no encontró comprobantes "
                        "para los parámetros seleccionados."
                    )
                    cls._job_update(
                        job_id,
                        estado="ejecutando",
                        mensaje=result["mensaje"],
                        sri=0,
                        ya_existentes=0,
                        descargadas=0,
                        guardadas=0,
                        errores=[],
                        paginas=0,
                    )
                    return result
            cls._job_update(job_id, estado="ejecutando", mensaje="Consulta completada. Procesando comprobantes.")
            db = obtener_session_cliente(ruc)
            procesadas: set[str] = set()
            try:
                if operacion == "ventas":
                    await cls._procesar_emitidos_ventas(
                        page, db, result, job_id, procesadas, anio, mes,
                        texto_tipo="factura", codcomp="18",
                    )
                    return result
                if operacion == "notas_credito_emitidas":
                    await cls._procesar_emitidos_ventas(
                        page, db, result, job_id, procesadas, anio, mes,
                        texto_tipo="nota de crédito", codcomp="04",
                        destino="ncventas",
                    )
                    return result
                if operacion == "retenciones_emitidas":
                    await cls._procesar_emitidos_retenciones(
                        page, db, result, job_id, procesadas, anio, mes,
                    )
                    return result
                es_nota_credito_recibida = operacion == "notas_credito_recibidas"
                es_retencion_recibida = operacion == "retenciones_recibidas"
                for pagina in range(1, 1001):
                    result["paginas"] = pagina
                    cls._job_update(job_id, mensaje=f"Procesando página {pagina}.", paginas=pagina)
                    links = page.locator(
                        'a[id*="lnkXml"], a[id$=":lnkXml"], input[id*="lnkXml"], '
                        'button[id*="lnkXml"], a[title*="XML"], a[href*="xml"]'
                    )
                    total_links = await links.count()
                    if total_links == 0:
                        raise RuntimeError("SRI no devolvió comprobantes en la tabla actual.")

                    for idx in range(total_links):
                        factura = None
                        try:
                            # El SRI puede tardar en liberar el AJAX de la fila
                            # anterior. Reconsultamos el locator en cada intento
                            # y esperamos a que el enlace sea realmente visible
                            # antes de hacer click.
                            download = None
                            ultimo_error = None

                            for intento in range(3):
                                try:
                                    enlaces_actuales = page.locator(
                                        'a[id*="lnkXml"], a[id$=":lnkXml"], input[id*="lnkXml"], '
                                        'button[id*="lnkXml"], a[title*="XML"], a[href*="xml"]'
                                    )
                                    if await enlaces_actuales.count() <= idx:
                                        raise RuntimeError(
                                            f"No se encontró el enlace XML de la fila {idx + 1}."
                                        )

                                    enlace_xml = enlaces_actuales.nth(idx)
                                    await enlace_xml.scroll_into_view_if_needed(timeout=10000)
                                    await enlace_xml.wait_for(state="visible", timeout=10000)

                                    async with page.expect_download(timeout=15000) as info:
                                        try:
                                            await enlace_xml.click(timeout=10000)
                                        except Exception:
                                            # En el último intento permitimos el
                                            # click forzado si PrimeFaces dejó un
                                            # overlay momentáneo sobre la fila.
                                            if intento < 2:
                                                raise
                                            await enlace_xml.click(
                                                force=True,
                                                timeout=10000,
                                            )

                                    download = await info.value
                                    break

                                except Exception as exc:
                                    ultimo_error = exc
                                    await page.wait_for_timeout(1500 * (intento + 1))

                            if download is None:
                                raise RuntimeError(
                                    f"No se pudo descargar el XML de la fila {idx + 1} "
                                    f"después de 3 intentos: {ultimo_error}"
                                )

                            with tempfile.TemporaryDirectory(prefix="conta_sri_") as tmp:
                                path = Path(tmp) / download.suggested_filename
                                await download.save_as(str(path))
                                factura = cls._parsear_xml(path)

                            clave = factura["clave_acceso"]
                            result["sri"] += 1
                            cls._job_update(job_id, sri=result["sri"], mensaje=f"Procesando comprobante {result['sri']}.")
                            if not clave:
                                raise ValueError("El XML no contiene clave de acceso.")
                            if clave in procesadas:
                                result["ya_existentes"] += 1
                                continue
                            procesadas.add(clave)

                            if es_retencion_recibida:
                                actualizadas = cls._actualizar_retencion_ventas(db, factura)
                                db.commit()
                                result["descargadas"] += 1
                                result["guardadas"] += actualizadas
                                cls._job_update(
                                    job_id,
                                    guardadas=result["guardadas"],
                                    descargadas=result["descargadas"],
                                    ya_existentes=result["ya_existentes"],
                                    mensaje=(
                                        f"Retención {factura['numest']}-{factura['numptoemi']}-"
                                        f"{factura['numsec']} actualizada en {actualizadas} factura(s)."
                                    ),
                                )
                                continue

                            exists = db.execute(text("""
                                SELECT 1 FROM comprasnue
                                WHERE TRIM(numaut::text) = :clave LIMIT 1
                            """), {"clave": clave}).first()
                            if exists:
                                result["ya_existentes"] += 1
                                continue

                            if es_nota_credito_recibida:
                                cls._insertar_nota_credito_recibida(db, factura)
                            else:
                                cls._insertar(db, factura, tipo_comprobante)
                            db.commit()
                            result["descargadas"] += 1
                            result["guardadas"] += 1
                            cls._job_update(job_id, guardadas=result["guardadas"], descargadas=result["descargadas"], ya_existentes=result["ya_existentes"])
                        except Exception as exc:
                            db.rollback()

                            numero_retencion = ""
                            proveedor_retencion = ""
                            factura_sustento = ""
                            if isinstance(factura, dict):
                                numero_retencion = (
                                    f"{factura.get('numest', '')}-"
                                    f"{factura.get('numptoemi', '')}-"
                                    f"{factura.get('numsec', '')}"
                                ).strip("-")
                                proveedor_retencion = str(
                                    factura.get("razon_social") or ""
                                ).strip()
                                documentos = factura.get("documentos_sustento") or []
                                if documentos:
                                    factura_sustento = ", ".join(
                                        str(d.get("num_doc_sustento") or "").strip()
                                        for d in documentos
                                        if d.get("num_doc_sustento")
                                    )

                            result["errores"].append({
                                "pagina": pagina,
                                "fila": idx + 1,
                                "cliente": result.get("cliente", ""),
                                "num_retencion": numero_retencion,
                                "proveedor_retencion": proveedor_retencion,
                                "factura_sustento": factura_sustento,
                                "detalle": str(exc),
                            })
                            cls._job_update(
                                job_id,
                                errores=result["errores"],
                                mensaje=(
                                    f"Error procesando retención "
                                    f"{numero_retencion or 'desconocida'} "
                                    f"del cliente {result.get('cliente', '')}: {exc}"
                                ),
                            )

                    if not await cls._siguiente_pagina(page):
                        break
            finally:
                db.close()
        finally:
            if browser is not None:
                try:
                    await browser.close()
                except Exception:
                    pass
            elif context is not None:
                try:
                    await context.close()
                except Exception:
                    pass
            cls._cerrar_chrome(chrome_process)
            if p is not None:
                await p.stop()

        result["ok"] = not result["errores"]
        return result

    @classmethod
    def _insertar_recap_venta(cls, db, retencion: dict[str, Any], documento: dict[str, Any]) -> bool:
        """Guarda una retención RECAP como registro especial en ventas.

        El SRI utiliza 999999999999992 como documento de sustento cuando la
        retención no corresponde a una factura individual. En el sistema
        legacy estas retenciones se representan como una fila con numfactur
        = RECAP y conservan la retención para el ATS.
        """
        numero_retencion = (
            f"{retencion['numest']}-{retencion['numptoemi']}-{retencion['numsec']}"
        )
        autorizacion = str(retencion.get("numero_autorizacion") or "").strip()
        fecha = retencion["fecha"].strftime("%Y-%m-%d")

        # En ventas, para un RECAP se registra como cliente/emisor al
        # contribuyente que emitió la retención, no al sujeto retenido.
        ruc_emisor = str(
            retencion.get("ruc")
            or retencion.get("identificacion_emisor")
            or ""
        ).strip()
        nombre_emisor = str(
            retencion.get("razon_social")
            or retencion.get("razon_social_emisor")
            or ""
        ).strip()

        existente = db.execute(text("""
            SELECT 1
            FROM ventas
            WHERE TRIM(numfactur::text) = 'RECAP'
              AND TRIM(numret::text) = TRIM(:numret)
              AND TRIM(autret::text) = TRIM(:autret)
            LIMIT 1
        """), {
            "numret": numero_retencion,
            "autret": autorizacion,
        }).first()

        if existente:
            logger.info(
                "RETENCION RECAP | ya existente | retencion=%s | autorizacion=%s",
                numero_retencion,
                autorizacion,
            )
            return False

        cero = Decimal("0")
        values = {
            "numfactur": "RECAP",
            "autorizacion": autorizacion,
            "fecfactur": cls._fecha_varchar(fecha),
            "ruccedcli": ruc_emisor,
            "nomcli": nombre_emisor,
            "tipid": "4",
            "codcomp": "18",
            "numemi": "0",
            "basenoobj": cero,
            "baseiva0": cero,
            "baseiva5": cero,
            "baseiva8": cero,
            "baseiva12": cero,
            "baseiva14": cero,
            "baseiva15": cero,
            "iva": cero,
            "iva5": cero,
            "iva8": cero,
            "iva12": cero,
            "iva14": cero,
            "iva15": cero,
            "ice": cero,
            "numret": numero_retencion,
            "autret": autorizacion,
            "fecret": cls._fecha_varchar(fecha),
            "retiva": documento.get("retiva", cero),
            "retrenta": documento.get("retrenta", cero),
            "mes": f"{retencion['fecha'].month:02d}",
            "año": str(retencion["fecha"].year),
            "numasiento": cero,
        }

        cols = ", ".join(f'"{k}"' if k == "año" else k for k in values)
        params = ", ".join(f":{k}" for k in values)

        db.execute(
            text(f"INSERT INTO ventas ({cols}) VALUES ({params})"),
            values,
        )

        logger.info(
            "RETENCION RECAP | insertada | retencion=%s | autorizacion=%s | "
            "emisor=%s | ruc=%s | retiva=%s | retrenta=%s",
            numero_retencion,
            autorizacion,
            nombre_emisor,
            ruc_emisor,
            documento.get("retiva", cero),
            documento.get("retrenta", cero),
        )
        return True

    @classmethod
    def _actualizar_retencion_ventas(cls, db, retencion: dict[str, Any]) -> int:
        """Actualiza retenciones recibidas o las registra como RECAP.

        Regla funcional: solo se actualiza una factura si el documento de
        sustento se encuentra en ventas. Si no se encuentra, sin importar
        cuál sea su número, la retención se guarda como RECAP independiente.
        El número especial 999999999999992 también se procesa como RECAP.
        """
        documentos = retencion.get("documentos_sustento") or []
        if not documentos:
            raise ValueError("La retención no contiene documentos de sustento.")

        numero_retencion = (
            f"{retencion['numest']}-{retencion['numptoemi']}-{retencion['numsec']}"
        )
        autorizacion = str(retencion.get("numero_autorizacion") or "").strip()
        fecha = retencion["fecha"].strftime("%Y-%m-%d")
        ruc_sujeto = str(
            retencion.get("identificacion_sujeto_retenido") or ""
        ).strip()

        actualizadas = 0
        procesadas = 0
        no_encontradas: list[str] = []

        for documento in documentos:
            num_doc = str(documento.get("num_doc_sustento") or "").strip()

            # Si el XML no trae número de sustento, tampoco hay factura que
            # actualizar: conservar la retención como RECAP.
            if not num_doc:
                logger.warning(
                    "RETENCION RECIBIDA | sustento vacío; se guardará como RECAP | retencion=%s",
                    numero_retencion,
                )
                if cls._insertar_recap_venta(db, retencion, documento):
                    actualizadas += 1
                procesadas += 1
                continue

            # Normalizamos números con o sin guiones para buscar la factura.
            num_doc_digitos = (
                num_doc.replace("-", "")
                .replace(" ", "")
                .replace(".", "")
            )

            if num_doc_digitos == "999999999999992":
                if cls._insertar_recap_venta(db, retencion, documento):
                    actualizadas += 1
                procesadas += 1
                continue

            if len(num_doc_digitos) == 15 and num_doc_digitos.isdigit():
                num_doc = (
                    f"{num_doc_digitos[:3]}-"
                    f"{num_doc_digitos[3:6]}-"
                    f"{num_doc_digitos[6:]}"
                )

            params = {
                "num_doc": num_doc,
                "num_doc_digitos": num_doc_digitos,
                "ruc_sujeto": ruc_sujeto,
                "numret": numero_retencion,
                "autret": autorizacion,
                "fecret": cls._fecha_varchar(fecha),
                "retiva": documento["retiva"],
                "retrenta": documento["retrenta"],
            }

            # La referencia principal de la retención es numDocSustento.
            # Primero intentamos número + RUC; si el RUC del XML no coincide
            # exactamente con ruccedcli, hacemos un segundo intento solo por
            # número de factura.
            sql_base = """
                UPDATE ventas
                SET numret = :numret,
                    autret = :autret,
                    fecret = :fecret,
                    retiva = :retiva,
                    retrenta = :retrenta
                WHERE REPLACE(REPLACE(REPLACE(TRIM(numfactur::text), '-', ''), ' ', ''), '.', '')
                      = :num_doc_digitos
            """

            result = None
            if ruc_sujeto:
                result = db.execute(text(sql_base + """
                    AND TRIM(ruccedcli::text) = TRIM(:ruc_sujeto)
                """), params)

            if not result or not result.rowcount:
                result = db.execute(text(sql_base), params)

            filas = result.rowcount if result else 0
            if filas:
                actualizadas += filas
                procesadas += 1
                logger.info(
                    "RETENCION RECIBIDA | factura actualizada | "
                    "retencion=%s | numDocSustento=%s | ruc=%s | "
                    "retiva=%s | retrenta=%s | filas=%s",
                    numero_retencion, num_doc, ruc_sujeto,
                    documento["retiva"], documento["retrenta"], filas,
                )
            else:
                no_encontradas.append(num_doc)
                logger.warning(
                    "RETENCION RECIBIDA | factura no encontrada; se guardará como RECAP | "
                    "retencion=%s | numDocSustento=%s | ruc=%s",
                    numero_retencion, num_doc, ruc_sujeto,
                )
                # Acuerdo funcional: si no existe la factura de sustento en
                # ventas, conservar la retención como RECAP en lugar de fallar.
                if cls._insertar_recap_venta(db, retencion, documento):
                    actualizadas += 1
                # Si ya existía el RECAP, también consideramos procesado el caso.
                procesadas += 1

        # Los RECAP existentes no deben convertirse en error ni duplicarse.
        if actualizadas == 0 and not procesadas:
            logger.info(
                "RETENCION RECIBIDA | sin cambios; no había documentos procesables | "
                "retencion=%s",
                numero_retencion,
            )

        if no_encontradas:
            logger.info(
                "RETENCION RECIBIDA | facturas no encontradas convertidas a RECAP | "
                "retencion=%s | facturas=%s",
                numero_retencion, ", ".join(no_encontradas),
            )

        return actualizadas

    @classmethod
    def _insertar_nota_credito_recibida(cls, db, factura: dict[str, Any]) -> None:
        """Guarda una nota de crédito recibida en comprasnue con la estructura legacy."""
        b, i = factura["bases"], factura["ivas"]
        db.execute(text("SELECT pg_advisory_xact_lock(hashtext('conta_comprasnue_numcompra'))"))

        numestmod = str(factura.get("numestmod") or "").strip()
        numptoemimod = str(factura.get("numptoemimod") or "").strip()
        numsecmod = str(factura.get("numsecmod") or "").strip()
        ruc = str(factura.get("ruc") or "").strip()

        numautmod = "9999999999"
        if numestmod and numptoemimod and numsecmod and ruc:
            original = db.execute(text("""
                SELECT numaut
                FROM comprasnue
                WHERE TRIM(numest::text) = TRIM(:numestmod)
                  AND TRIM(numptoemi::text) = TRIM(:numptoemimod)
                  AND TRIM(numsec::text) = TRIM(:numsecmod)
                  AND TRIM(ruccedprovee::text) = TRIM(:ruc)
                  AND TRIM(tipcom::text) IN ('01', '02')
                ORDER BY numcompra DESC NULLS LAST
                LIMIT 1
            """), {
                "numestmod": numestmod,
                "numptoemimod": numptoemimod,
                "numsecmod": numsecmod,
                "ruc": ruc,
            }).scalar()
            if original:
                numautmod = str(original).strip() or "9999999999"

        # numcompra debe provenir del consecutivo de parametros, no de MAX().
        numcompra = db.execute(text("SELECT siguiente_parametro('numcompra')")).scalar()
        if numcompra is None:
            raise RuntimeError("No se pudo obtener el consecutivo numcompra desde parametros.")
        numcompra = str(numcompra).strip()
        if not numcompra:
            raise RuntimeError("El consecutivo numcompra obtenido desde parametros está vacío.")

        values = {
            "numcompra": numcompra,
            "codsus": "01",
            "tipid": "01",
            "ruccedprovee": ruc,
            "tipcom": "04",
            "fecreg": cls._fecha_varchar(factura["fecha"]),
            "numest": factura["numest"],
            "numptoemi": factura["numptoemi"],
            "numsec": factura["numsec"],
            "fecemi": cls._fecha_varchar(factura["fecha_emision"]),
            "numaut": factura["clave_acceso"],
            "baseimpnoobj": b["no_objeto"],
            "baseimpiva0": b["0"],
            "baseimpiva12": b["12"],
            "baseexenta": b["exenta"],
            "montoice": factura["ice"],
            "montoiva": Decimal("0"),
            "retencioniva10": 0, "retencioniva20": 0, "retencioniva30": 0,
            "retencioniva70": 0, "retencioniva100": 0,
            "totbases": sum(b.values(), Decimal("0")),
            "codret": "", "baseimpret": "", "porret": "", "valret": "",
            "numestret": "", "numptoemiret": "", "numsecret": "",
            "numautret": "", "fecret": "", "tipopago": factura["tipopago"],
            "codtipodoc": "01",
            "numestmod": numestmod,
            "numptoemimod": numptoemimod,
            "numsecmod": numsecmod,
            "numautmod": numautmod,
            "mes": f"{factura['fecha'].month:02d}",
            "año": str(factura["fecha"].year),
            "nomprovee": factura["razon_social"],
            "baseimpiva5": b["5"], "baseimpiva8": b["8"],
            "baseimpiva14": b["14"], "baseimpiva15": b["15"],
            "montoiva5": i["5"], "montoiva8": i["8"],
            "montoiva12": i["12"], "montoiva14": i["14"],
            "montoiva15": i["15"],
        }
        cols = ", ".join(f'"{k}"' if k == "año" else k for k in values)
        params = ", ".join(f":{k}" for k in values)
        db.execute(text(f"INSERT INTO comprasnue ({cols}) VALUES ({params})"), values)

    @classmethod
    def _insertar(cls, db, factura: dict[str, Any], tipo_comprobante: int) -> None:
        b, i = factura["bases"], factura["ivas"]
        db.execute(text("SELECT pg_advisory_xact_lock(hashtext('conta_comprasnue_numcompra'))"))
        logger.warning(
            "SRI COMPRA INSERT | clave=%s | base0=%s | base5=%s | base8=%s | base12=%s | base14=%s | base15=%s | iva15=%s",
            factura["clave_acceso"],
            b["0"], b["5"], b["8"], b["12"], b["14"], b["15"], i["15"],
        )
        # Estructura inicial del registro según el procesamiento legacy de VB6.
        totbases = sum(b.values(), Decimal("0"))

        # TipoPago solo se modifica si el total cumple la condición >= 500.
        # En ese caso, si no viene una forma de pago, VB6 coloca "20".
        total = cls._dec(factura.get("total"))
        tipopago_legacy = ""
        if total >= Decimal("500"):
            tipopago_legacy = str(factura.get("tipopago") or "").strip() or "20"

        values = {
            "codsus": "01", "tipid": "01", "ruccedprovee": factura["ruc"],
            "tipcom": cls._tipo(tipo_comprobante), "fecreg": cls._fecha_varchar(factura["fecha"]),
            "numest": factura["numest"], "numptoemi": factura["numptoemi"],
            "numsec": factura["numsec"], "fecemi": cls._fecha_varchar(factura["fecha_emision"]),
            "numaut": factura["clave_acceso"], "baseimpnoobj": b["no_objeto"],
            "baseimpiva0": b["0"], "baseexenta": b["exenta"],
            "baseimpiva5": b["5"], "baseimpiva8": b["8"], "baseimpiva12": b["12"],
            "baseimpiva14": b["14"], "baseimpiva15": b["15"],
            "montoice": factura["ice"], "montoiva": Decimal("0"),
            "montoiva5": i["5"], "montoiva8": i["8"], "montoiva12": i["12"],
            "montoiva14": i["14"], "montoiva15": i["15"],
            "retencioniva10": Decimal("0"), "retencioniva20": Decimal("0"),
            "retencioniva30": Decimal("0"), "retencioniva70": Decimal("0"),
            "retencioniva100": Decimal("0"),
            # Valores iniciales de una compra nueva según VB6.
            "totbases": totbases, "codret": "332", "baseimpret": totbases,
            "porret": Decimal("0"), "valret": Decimal("0.00"),
            "numestret": "", "numptoemiret": "", "numsecret": "",
            "numautret": "", "fecret": "", "tipopago": tipopago_legacy,
            "codtipodoc": "", "numestmod": "", "numptoemimod": "", "numsecmod": "",
            "numautmod": "", "mes": f"{factura['fecha'].month:02d}", "año": str(factura["fecha"].year),
            "nomprovee": factura["razon_social"], "baseimpiva5": b["5"], "baseimpiva8": b["8"],
            "baseimpiva14": b["14"], "baseimpiva15": b["15"], "montoiva5": i["5"],
            "montoiva8": i["8"], "montoiva12": i["12"], "montoiva14": i["14"], "montoiva15": i["15"],
        }
        # numcompra es el consecutivo oficial de comprasnue y debe
        # salir SIEMPRE de parametros mediante siguiente_parametro().
        # No usar MAX(numcompra)+1: el consecutivo de parametros puede estar
        # adelantado o atrasado respecto de los registros existentes y eso
        # provoca que facturas y notas de crédito recibidas puedan terminar
        # compartiendo el mismo numcompra.
        next_num = db.execute(
            text("SELECT siguiente_parametro('numcompra')")
        ).scalar()

        if next_num is None:
            raise RuntimeError(
                "No se pudo obtener el consecutivo numcompra desde parametros."
            )

        next_num = str(next_num).strip()
        if not next_num:
            raise RuntimeError(
                "El consecutivo numcompra obtenido desde parametros está vacío."
            )

        values["numcompra"] = next_num
        cols = ", ".join(f'"{k}"' if k == "año" else k for k in values)
        params = ", ".join(f":{k}" for k in values)
        _iva_debug_log(
            "BD ANTES INSERT | clave=%s | numcompra=%s | baseimpiva0=%s | baseimpiva5=%s | baseimpiva8=%s | baseimpiva12=%s | baseimpiva14=%s | baseimpiva15=%s | montoiva5=%s | montoiva8=%s | montoiva12=%s | montoiva14=%s | montoiva15=%s",
            factura["clave_acceso"], values["numcompra"],
            values["baseimpiva0"], values["baseimpiva5"], values["baseimpiva8"],
            values["baseimpiva12"], values["baseimpiva14"], values["baseimpiva15"],
            values["montoiva5"], values["montoiva8"], values["montoiva12"],
            values["montoiva14"], values["montoiva15"],
        )

        db.execute(text(f"INSERT INTO comprasnue ({cols}) VALUES ({params})"), values)

        # Leemos inmediatamente la fila dentro de la misma transacción para
        # comprobar qué terminó recibiendo realmente PostgreSQL.
        almacenado = db.execute(text("""
            SELECT numcompra, numaut,
                   baseimpiva0, baseimpiva5, baseimpiva8,
                   baseimpiva12, baseimpiva14, baseimpiva15,
                   montoiva5, montoiva8, montoiva12, montoiva14, montoiva15
            FROM comprasnue
            WHERE numcompra = :numcompra
            LIMIT 1
        """), {"numcompra": values["numcompra"]}).mappings().first()

        if almacenado:
            _iva_debug_log(
                "BD DESPUES INSERT | clave=%s | numcompra=%s | baseimpiva0=%s | baseimpiva5=%s | baseimpiva8=%s | baseimpiva12=%s | baseimpiva14=%s | baseimpiva15=%s | montoiva5=%s | montoiva8=%s | montoiva12=%s | montoiva14=%s | montoiva15=%s",
                almacenado["numaut"], almacenado["numcompra"],
                almacenado["baseimpiva0"], almacenado["baseimpiva5"], almacenado["baseimpiva8"],
                almacenado["baseimpiva12"], almacenado["baseimpiva14"], almacenado["baseimpiva15"],
                almacenado["montoiva5"], almacenado["montoiva8"], almacenado["montoiva12"],
                almacenado["montoiva14"], almacenado["montoiva15"],
            )