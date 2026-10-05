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
                    sri INTEGER NOT NULL DEFAULT 0,
                    ya_existentes INTEGER NOT NULL DEFAULT 0,
                    descargadas INTEGER NOT NULL DEFAULT 0,
                    guardadas INTEGER NOT NULL DEFAULT 0,
                    errores JSONB NOT NULL DEFAULT '[]'::jsonb,
                    paginas INTEGER NOT NULL DEFAULT 0,
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
                f"CREATE INDEX IF NOT EXISTS idx_{cls.JOB_TABLE}_estado "
                f"ON {cls.JOB_TABLE}(estado, creado)"
            ))

    @classmethod
    def _job_update(cls, job_id: str | None, **values) -> None:
        if not job_id:
            return
        cls._ensure_jobs_table()
        allowed = {
            "estado", "ruc", "cliente", "anio", "mes", "tipo_comprobante", "operacion",
            "sri", "ya_existentes", "descargadas", "guardadas", "errores",
            "paginas", "mensaje", "detalle"
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
    def iniciar_sincronizacion(cls, ruc: str, anio: int, mes: int, tipo_comprobante: int = 1, operacion: str = "compras") -> dict[str, Any]:
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
                (job_id, estado, ruc, cliente, anio, mes, tipo_comprobante, operacion, mensaje)
                VALUES
                (:job_id, 'pendiente', :ruc, :cliente, :anio, :mes, :tipo, :operacion, :mensaje)
            """), {
                "job_id": job_id,
                "ruc": ruc,
                "cliente": cred["nombre"],
                "anio": anio,
                "mes": mes,
                "tipo": cls._tipo(tipo_comprobante), "operacion": operacion,
                "mensaje": "Sincronización en cola. Esperando al worker SRI interactivo.",
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
            if operacion == "ventas_validar":
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
            cls._job_update(
                job_id,
                **resultado,
                estado="finalizado",
                mensaje="Sincronización finalizada.",
            )
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
    def obtener_trabajo_pendiente(cls) -> dict[str, Any] | None:
        """Reclama atómicamente un trabajo para el worker SRI interactivo."""
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
                    mensaje = 'Trabajo reclamado por el worker SRI interactivo.',
                    actualizado = CURRENT_TIMESTAMP
                WHERE job_id = :job_id
            """), {"job_id": row["job_id"]})
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
        try:
            return Decimal(str(value or "0").strip().replace(",", "."))
        except (InvalidOperation, ValueError):
            return Decimal("0")

    @staticmethod
    def _txt(node: ET.Element | None, tag: str, default: str = "") -> str:
        return ((node.findtext(tag) if node is not None else None) or default).strip()

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
        inf = doc.find("infoFactura")
        if it is None or inf is None:
            raise ValueError("El comprobante no contiene infoTributaria/infoFactura.")

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

        for impuesto in doc.findall(".//detalle/impuestos/impuesto"):
            codigo = cls._txt(impuesto, "codigo")
            if codigo != "2":
                continue

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
        if impuestos_clasificados == 0:
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
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            return sock.connect_ex(("127.0.0.1", port)) != 0

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

        port = int(settings.SRI_CDP_PORT or 9222)
        if not cls._puerto_libre(port):
            await p.stop()
            raise RuntimeError(
                f"El puerto CDP {port} ya está ocupado. Cierre el Chrome SRI de prueba "
                "o configure otro SRI_CDP_PORT en .env."
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

            await page.wait_for_timeout(1500)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass

            if "perfil" in page.url and await page.locator("#password").count():
                raise ValueError("El SRI no aceptó las credenciales del cliente.")

            await page.goto(
                cls.PORTAL_URL,
                wait_until="domcontentloaded",
                timeout=60000,
            )
            return p, browser, context, page, chrome_process

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
        await page.locator(selector).wait_for(state="visible", timeout=15000)
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
                if tasa not in {"5", "8", "12", "14", "15"}:
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
    async def _consultar_emitidos(cls, page, anio: int, mes: int) -> None:
        """Abre la consulta de comprobantes emitidos.
        
        La pantalla del SRI consulta por una fecha exacta, no por todo el mes.
        La iteración diaria se realiza en _procesar_emitidos_ventas().
        """
        await page.get_by_text("Comprobantes electrónicos emitidos", exact=True).click()
        await page.locator("#frmPrincipal\\:calendarFechaDesde_input").wait_for(
            state="visible", timeout=30000
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
    async def _procesar_emitidos_ventas(cls, page, db, result, job_id, procesadas, anio: int, mes: int) -> None:
        """Consulta y procesa todas las fechas del mes de comprobantes emitidos."""
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
                        f"Procesando facturas emitidas del {fecha_consulta.strftime('%d/%m/%Y')} "
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
                        if tipo_texto and not (
                            tipo_texto == "01"
                            or tipo_texto.lower().startswith("factura")
                            or " factura " in f" {tipo_texto.lower()} "
                        ):
                            continue

                        html = await cls._obtener_detalle_emitido(page, idx)
                        if not html:
                            continue

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

                        existe = db.execute(text("""
                            SELECT 1 FROM ventas
                            WHERE TRIM(autorizacion::text) = :clave
                            LIMIT 1
                        """), {"clave": clave}).first()

                        if existe:
                            result["ya_existentes"] += 1
                            continue

                        cls._insertar_venta(db, factura)
                        db.commit()
                        result["descargadas"] += 1
                        result["guardadas"] += 1

                        cls._job_update(
                            job_id,
                            sri=result["sri"],
                            guardadas=result["guardadas"],
                            descargadas=result["descargadas"],
                            ya_existentes=result["ya_existentes"],
                            mensaje=f"Factura emitida {result['sri']} procesada.",
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
    def _insertar_venta(cls, db, factura: dict[str, Any]) -> None:
        b = factura["bases_iva"]
        i = factura["ivas"]
        values = {
            "numfactur": f"{factura['establecimiento']}-{factura['punto_emision']}-{factura['secuencial']}",
            "autorizacion": factura["clave_acceso"],
            "fecfactur": factura["fecha"].strftime("%Y-%m-%d"),
            "ruccedcli": factura["identificacion"],
            "nomcli": factura["razon_social"],
            "tipid": factura["tipid"],
            "codcomp": "18",
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
                await cls._seleccionar(page, f"#frmPrincipal\\:{nombre}", value)

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

        await page.goto(
            cls.RECIBIDOS_URL,
            wait_until="domcontentloaded",
            timeout=30000,
        )
        try:
            await page.wait_for_load_state("load", timeout=20000)
        except Exception:
            pass
        await page.wait_for_timeout(3000)

        # En el SRI la carga del JavaScript de reCAPTCHA puede ocurrir después
        # de DOMContentLoaded. No dependemos de que el botón aparezca habilitado:
        # primero dejamos que el propio rcBuscar inicialice el formulario.
        for intento in range(1, 4):
            boton = await preparar_formulario()

            if await page.evaluate("() => typeof rcBuscar === 'function'"):
                await page.evaluate("""
                    () => {
                        console.log("CONTA: ejecutando rcBuscar() para inicializar SRI");
                        rcBuscar();
                    }
                """)
                await page.wait_for_timeout(3000)

            if await esperar_boton_habilitado(boton, segundos=30):
                print("SRI inicializó el formulario mediante rcBuscar(); Consultar habilitado.")
                break

            # Si el SRI todavía mantiene el botón bloqueado, recargamos esperando
            # el evento load completo. Esto reproduce de forma controlada el F5
            # que manualmente permitió continuar.
            if intento < 3:
                print(
                    f"SRI mantuvo Consultar bloqueado en intento {intento}. "
                    "Recargando y esperando la inicialización completa de reCAPTCHA."
                )
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=30000)
                except PlaywrightTimeoutError:
                    pass
                try:
                    await page.wait_for_load_state("load", timeout=20000)
                except Exception:
                    pass
                await page.wait_for_timeout(5000)
            else:
                diagnostico = await cls._diagnostico_consulta(page)
                try:
                    await page.screenshot(
                        path=str(Path(tempfile.gettempdir()) / f"conta_sri_bloqueado_{anio}_{mes:02d}.png"),
                        full_page=True,
                    )
                except Exception:
                    pass
                raise RuntimeError(
                    "SRI mantuvo el botón Consultar deshabilitado después de "
                    "inicializar rcBuscar() y esperar la carga completa de la página. "
                    + diagnostico
                )

        # Importante: el click genera primero un AJAX sin token. Después,
        # executeRecaptcha() llama a onSubmit() y rcBuscar() genera el AJAX
        # definitivo con g-recaptcha-response. No esperamos XML aquí.
        try:
            await page.evaluate("""
                () => {
                    const boton = document.getElementById('frmPrincipal:btnBuscar');
                    if (!boton || typeof boton.onclick !== 'function') {
                        throw new Error('No se encontró el onclick oficial de frmPrincipal:btnBuscar.');
                    }
                    boton.onclick();
                }
            """)
        except Exception as exc:
            diagnostico = await cls._diagnostico_consulta(page)
            raise RuntimeError(
                "No fue posible ejecutar el flujo oficial de Consultar del SRI. "
                + diagnostico
            ) from exc

        filas = await esperar_resultado(segundos=45)
        if filas <= 0:
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
            if operacion == "ventas":
                await cls._consultar_emitidos(page, anio, mes)
            else:
                await cls._consultar_recibidos(page, anio, mes, tipo_comprobante)
            cls._job_update(job_id, estado="ejecutando", mensaje="Consulta completada. Procesando comprobantes.")
            db = obtener_session_cliente(ruc)
            procesadas: set[str] = set()
            try:
                if operacion == "ventas":
                    await cls._procesar_emitidos_ventas(page, db, result, job_id, procesadas, anio, mes)
                    return result
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
                        try:
                            # Mantener el mismo selector usado para detectar los
                            # enlaces. El SRI puede cambiar el id exacto de lnkXml.
                            enlace_xml = links.nth(idx)
                            async with page.expect_download(timeout=30000) as info:
                                await enlace_xml.click()
                            download = await info.value
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

                            exists = db.execute(text("""
                                SELECT 1 FROM comprasnue
                                WHERE TRIM(numaut::text) = :clave LIMIT 1
                            """), {"clave": clave}).first()
                            if exists:
                                result["ya_existentes"] += 1
                                continue

                            cls._insertar(db, factura, tipo_comprobante)
                            db.commit()
                            result["descargadas"] += 1
                            result["guardadas"] += 1
                            cls._job_update(job_id, guardadas=result["guardadas"], descargadas=result["descargadas"], ya_existentes=result["ya_existentes"])
                        except Exception as exc:
                            db.rollback()
                            result["errores"].append({
                                "pagina": pagina, "fila": idx + 1, "detalle": str(exc)
                            })
                            cls._job_update(job_id, errores=result["errores"], mensaje=f"Error procesando fila {idx + 1}: {exc}")

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
    def _insertar(cls, db, factura: dict[str, Any], tipo_comprobante: int) -> None:
        b, i = factura["bases"], factura["ivas"]
        db.execute(text("SELECT pg_advisory_xact_lock(hashtext('conta_comprasnue_numcompra'))"))
        logger.warning(
            "SRI COMPRA INSERT | clave=%s | base0=%s | base5=%s | base8=%s | base12=%s | base14=%s | base15=%s | iva15=%s",
            factura["clave_acceso"],
            b["0"], b["5"], b["8"], b["12"], b["14"], b["15"], i["15"],
        )
        values = {
            "codsus": "01", "tipid": "01", "ruccedprovee": factura["ruc"],
            "tipcom": cls._tipo(tipo_comprobante), "fecreg": factura["fecha"],
            "numest": factura["numest"], "numptoemi": factura["numptoemi"],
            "numsec": factura["numsec"], "fecemi": factura["fecha_emision"],
            "numaut": factura["clave_acceso"], "baseimpnoobj": b["no_objeto"],
            "baseimpiva0": b["0"], "baseimpiva12": b["12"], "baseexenta": b["exenta"],
            "montoice": factura["ice"], "montoiva": Decimal("0"),
            "retencioniva10": 0, "retencioniva20": 0, "retencioniva30": 0,
            "retencioniva70": 0, "retencioniva100": 0,
            "totbases": sum(b.values(), Decimal("0")), "codret": "", "baseimpret": "",
            "porret": "", "valret": "", "numestret": "", "numptoemiret": "",
            "numsecret": "", "numautret": "", "fecret": "", "tipopago": factura["tipopago"],
            "codtipodoc": "", "numestmod": "", "numptoemimod": "", "numsecmod": "",
            "numautmod": "", "mes": f"{factura['fecha'].month:02d}", "año": str(factura["fecha"].year),
            "nomprovee": factura["razon_social"], "baseimpiva5": b["5"], "baseimpiva8": b["8"],
            "baseimpiva14": b["14"], "baseimpiva15": b["15"], "montoiva5": i["5"],
            "montoiva8": i["8"], "montoiva12": i["12"], "montoiva14": i["14"], "montoiva15": i["15"],
        }
        next_num = db.execute(text("""
            SELECT COALESCE(MAX(CASE
                WHEN TRIM(numcompra::text) ~ '^[0-9]+$'
                THEN TRIM(numcompra::text)::bigint ELSE 0 END), 0) + 1
            FROM comprasnue
        """)).scalar_one()
        values["numcompra"] = str(next_num)
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