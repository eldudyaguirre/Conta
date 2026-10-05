from __future__ import annotations

import asyncio
import json
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


class SriClienteSyncService:
    JOB_TABLE = "conta_sri_jobs"

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
                f"CREATE INDEX IF NOT EXISTS idx_{cls.JOB_TABLE}_estado "
                f"ON {cls.JOB_TABLE}(estado, creado)"
            ))

    @classmethod
    def _job_update(cls, job_id: str | None, **values) -> None:
        if not job_id:
            return
        cls._ensure_jobs_table()
        allowed = {
            "estado", "ruc", "cliente", "anio", "mes", "tipo_comprobante",
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
    def iniciar_sincronizacion(cls, ruc: str, anio: int, mes: int, tipo_comprobante: int = 1) -> dict[str, Any]:
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
                  AND estado IN ('pendiente', 'ejecutando', 'captcha')
                ORDER BY creado DESC
                LIMIT 1
            """), {
                "ruc": ruc, "anio": anio, "mes": mes,
                "tipo": cls._tipo(tipo_comprobante),
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
                (job_id, estado, ruc, cliente, anio, mes, tipo_comprobante, mensaje)
                VALUES
                (:job_id, 'pendiente', :ruc, :cliente, :anio, :mes, :tipo, :mensaje)
            """), {
                "job_id": job_id,
                "ruc": ruc,
                "cliente": cred["nombre"],
                "anio": anio,
                "mes": mes,
                "tipo": cls._tipo(tipo_comprobante),
                "mensaje": "Sincronización en cola. Esperando al worker SRI interactivo.",
            })
        return {"job_id": job_id, "estado": "pendiente", "duplicado": False}

    @classmethod
    async def _ejecutar_job(cls, job_id: str, ruc: str, anio: int, mes: int, tipo_comprobante: int) -> None:
        cls._job_update(
            job_id,
            estado="ejecutando",
            mensaje="Worker SRI activo. Iniciando navegador y conexión con el SRI.",
        )
        try:
            resultado = await cls.sincronizar_mes(
                ruc, anio, mes, tipo_comprobante, job_id=job_id
            )
            cls._job_update(
                job_id,
                **resultado,
                estado="finalizado",
                mensaje="Sincronización finalizada.",
            )
        except Exception as exc:
            cls._job_update(
                job_id,
                estado="error",
                mensaje=str(exc),
                detalle=str(exc),
            )

    @classmethod
    def estado_sincronizacion(cls, job_id: str) -> dict[str, Any] | None:
        cls._ensure_jobs_table()
        with engine.connect() as db:
            row = db.execute(text(f"""
                SELECT job_id, estado, ruc, cliente, anio, mes, tipo_comprobante,
                       sri, ya_existentes, descargadas, guardadas, errores,
                       paginas, mensaje, detalle, creado, actualizado
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
                SELECT job_id, ruc, anio, mes, tipo_comprobante
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

        totals = inf.find("totalConImpuestos")
        if totals is not None:
            for ti in totals.findall("totalImpuesto"):
                codigo = cls._txt(ti, "codigo")
                tarifa = cls._dec(cls._txt(ti, "tarifa"))
                codigo_pct = cls._txt(ti, "codigoPorcentaje")
                base = cls._dec(cls._txt(ti, "baseImponible"))
                valor = cls._dec(cls._txt(ti, "valor"))
                if codigo == "3":
                    ice += valor
                elif codigo == "2":
                    t = format(tarifa, "f").rstrip("0").rstrip(".")
                    if t in bases:
                        bases[t] += base
                        if t in ivas:
                            ivas[t] += valor
                    elif tarifa == 0:
                        bases["0"] += base
                    elif codigo_pct == "6":
                        bases["exenta"] += base
                    else:
                        bases["no_objeto"] += base

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
            if chrome_process is not None:
                try:
                    chrome_process.terminate()
                    chrome_process.wait(timeout=5)
                except Exception:
                    try:
                        chrome_process.kill()
                    except Exception:
                        pass
            await p.stop()
            raise

    @staticmethod
    async def _seleccionar(page, selector: str, value: str) -> None:
        await page.locator(selector).wait_for(state="visible", timeout=15000)
        await page.select_option(selector, value)

    @classmethod
    async def _consultar_recibidos(cls, page, anio: int, mes: int, tipo_comprobante: int) -> None:
        """Consulta comprobantes recibidos usando Chrome real y reCAPTCHA Enterprise del SRI."""
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

        # Primera carga.
        await page.goto(
            cls.RECIBIDOS_URL,
            wait_until="domcontentloaded",
            timeout=30000,
        )

        # El SRI puede cargar la página antes de terminar de inicializar
        # reCAPTCHA Enterprise. En las pruebas manuales, un F5 hizo que el
        # botón pasara de bloqueado a habilitado. Reproducimos ese comportamiento
        # automáticamente, sin intervención del usuario.
        for intento in range(1, 4):
            boton = await preparar_formulario()

            try:
                await boton.scroll_into_view_if_needed(timeout=5000)
            except Exception:
                pass

            if await esperar_boton_habilitado(boton, segundos=20):
                break

            if intento < 3:
                print(
                    f"SRI dejó Consultar bloqueado en el intento {intento}. "
                    "Recargando la página para reinicializar reCAPTCHA Enterprise."
                )
                try:
                    await page.reload(
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                except PlaywrightTimeoutError:
                    # El portal puede tardar en terminar la navegación JSF;
                    # seguimos y dejamos que preparar_formulario compruebe el estado.
                    pass
                await page.wait_for_timeout(2500)
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
                    "SRI mantuvo el botón Consultar deshabilitado incluso después "
                    "de recargar automáticamente la página. " + diagnostico
                )

        try:
            await boton.click(timeout=15000)
        except PlaywrightTimeoutError as exc:
            diagnostico = await cls._diagnostico_consulta(page)
            raise RuntimeError(
                "SRI habilitó el formulario, pero no fue posible ejecutar la consulta. "
                + diagnostico
            ) from exc

        # El SRI ha cambiado varias veces el markup del enlace de descarga XML.
        xml_selector = (
            'a[id*="lnkXml"], '
            'a[id$=":lnkXml"], '
            'input[id*="lnkXml"], '
            'button[id*="lnkXml"], '
            'a[title*="XML"], '
            'a[href*="xml"]'
        )
        links = page.locator(xml_selector)
        try:
            await links.first.wait_for(state="visible", timeout=20000)
            return
        except PlaywrightTimeoutError:
            diagnostico = await cls._diagnostico_consulta(page)
            if settings.SRI_HEADLESS:
                raise RuntimeError(
                    "SRI no devolvió los enlaces XML después de Consultar. "
                    "El navegador está en modo headless; use SRI_HEADLESS=false para la primera prueba. "
                    + diagnostico
                )
            print(
                "SRI todavía no muestra los enlaces XML. "
                "Esperando hasta 120 segundos por la respuesta del portal."
            )
            try:
                await links.first.wait_for(state="visible", timeout=120000)
            except PlaywrightTimeoutError as exc:
                diagnostico = await cls._diagnostico_consulta(page)
                try:
                    await page.screenshot(
                        path=str(Path(tempfile.gettempdir()) / f"conta_sri_resultado_{anio}_{mes:02d}.png"),
                        full_page=True,
                    )
                except Exception:
                    pass
                raise RuntimeError(
                    "SRI no mostró los enlaces XML después de 120 segundos. "
                    "La consulta pudo quedar detenida por CAPTCHA, por un cambio del portal "
                    "o porque la tabla no terminó de renderizar. " + diagnostico
                ) from exc

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
    async def sincronizar_mes(cls, ruc: str, anio: int, mes: int, tipo_comprobante: int = 1, job_id: str | None = None) -> dict[str, Any]:
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
            await cls._consultar_recibidos(page, anio, mes, tipo_comprobante)
            cls._job_update(job_id, estado="ejecutando", mensaje="Consulta completada. Procesando comprobantes.")
            db = obtener_session_cliente(ruc)
            procesadas: set[str] = set()
            try:
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
                            links = page.locator('a[id$=":lnkXml"]')
                            async with page.expect_download(timeout=30000) as info:
                                await links.nth(idx).click()
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
            if chrome_process is not None:
                try:
                    chrome_process.terminate()
                    chrome_process.wait(timeout=5)
                except Exception:
                    try:
                        chrome_process.kill()
                    except Exception:
                        pass
            if p is not None:
                await p.stop()

        result["ok"] = not result["errores"]
        return result

    @classmethod
    def _insertar(cls, db, factura: dict[str, Any], tipo_comprobante: int) -> None:
        b, i = factura["bases"], factura["ivas"]
        db.execute(text("SELECT pg_advisory_xact_lock(hashtext('conta_comprasnue_numcompra'))"))
        values = {
            "codsus": "01", "tipid": "01", "ruccedprovee": factura["ruc"],
            "tipcom": cls._tipo(tipo_comprobante), "fecreg": factura["fecha"],
            "numest": factura["numest"], "numptoemi": factura["numptoemi"],
            "numsec": factura["numsec"], "fecemi": factura["fecha_emision"],
            "numaut": factura["clave_acceso"], "baseimpnoobj": b["no_objeto"],
            "baseimpiva0": b["0"], "baseimpiva12": b["12"], "baseexenta": b["exenta"],
            "montoice": factura["ice"], "montoiva": sum(i.values(), Decimal("0")),
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
        db.execute(text(f"INSERT INTO comprasnue ({cols}) VALUES ({params})"), values)
