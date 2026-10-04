from __future__ import annotations

import tempfile
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

    @classmethod
    async def _login(cls, ruc: str, clave: str):
        p = await async_playwright().start()
        browser = await p.chromium.launch(headless=settings.SRI_HEADLESS)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        try:
            await page.goto(cls.LOGIN_URL, wait_until="commit", timeout=60000)
            usuario = page.locator('input[name="username"]:visible, #username:visible, #usuario:visible').first
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
            try:
                await login_button.scroll_into_view_if_needed(timeout=5000)
            except Exception:
                pass
            await login_button.click(force=True)
            await page.wait_for_timeout(1500)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            if "perfil" in page.url and await page.locator("#password").count():
                raise ValueError("El SRI no aceptó las credenciales del cliente.")
            await page.goto(cls.PORTAL_URL, wait_until="domcontentloaded", timeout=60000)
            return p, browser, page
        except Exception:
            await browser.close()
            await p.stop()
            raise

    @staticmethod
    async def _seleccionar(page, selector: str, value: str) -> None:
        await page.locator(selector).wait_for(state="visible", timeout=15000)
        await page.select_option(selector, value)

    @classmethod
    async def _consultar_recibidos(cls, page, anio: int, mes: int, tipo_comprobante: int) -> None:
        await page.goto(cls.RECIBIDOS_URL, wait_until="domcontentloaded", timeout=30000)
        await cls._seleccionar(page, "#frmPrincipal\\:ano", str(anio))
        await cls._seleccionar(page, "#frmPrincipal\\:mes", str(mes))
        await cls._seleccionar(page, "#frmPrincipal\\:dia", "0")
        await cls._seleccionar(page, "#frmPrincipal\\:cmbTipoComprobante", str(tipo_comprobante))
        await page.locator("#frmPrincipal\\:btnConsultarSinRe").click()

        links = page.locator('a[id$=":lnkXml"]')
        try:
            await links.first.wait_for(state="visible", timeout=15000)
            return
        except PlaywrightTimeoutError:
            if settings.SRI_HEADLESS:
                raise RuntimeError(
                    "SRI presentó reCAPTCHA para la consulta mensual. "
                    "Configure SRI_HEADLESS=False para resolverlo en Chromium y continuar automáticamente."
                )
            print("SRI solicita reCAPTCHA. Resuélvalo en Chromium; Conta continuará automáticamente.")
            await links.first.wait_for(state="visible", timeout=120000)

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
    async def sincronizar_mes(cls, ruc: str, anio: int, mes: int, tipo_comprobante: int = 1) -> dict[str, Any]:
        cred = cls._credenciales(ruc)
        p = browser = page = None
        result = {
            "ruc": ruc, "cliente": cred["nombre"], "anio": anio, "mes": mes,
            "tipo_comprobante": cls._tipo(tipo_comprobante),
            "sri": 0, "ya_existentes": 0, "descargadas": 0, "guardadas": 0,
            "errores": [], "paginas": 0,
        }
        try:
            p, browser, page = await cls._login(ruc, cred["clave"])
            await cls._consultar_recibidos(page, anio, mes, tipo_comprobante)
            db = obtener_session_cliente(ruc)
            procesadas: set[str] = set()
            try:
                for pagina in range(1, 1001):
                    result["paginas"] = pagina
                    links = page.locator('a[id$=":lnkXml"]')
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
                        except Exception as exc:
                            db.rollback()
                            result["errores"].append({
                                "pagina": pagina, "fila": idx + 1, "detalle": str(exc)
                            })

                    if not await cls._siguiente_pagina(page):
                        break
            finally:
                db.close()
        finally:
            if browser is not None:
                await browser.close()
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
