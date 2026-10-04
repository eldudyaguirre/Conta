from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import re
from pathlib import Path
from urllib.parse import parse_qsl, unquote_plus
from xml.etree import ElementTree as ET

from app.core.config import settings
from app.services.sri_cliente_sync_service import SriClienteSyncService

SENSITIVE_FIELDS = {
    "g-recaptcha-response",
    "javax.faces.ViewState",
    "password",
    "j_password",
    "contrasena",
    "clave",
}

SAFE_FIELDS = {
    "javax.faces.source",
    "javax.faces.partial.execute",
    "javax.faces.partial.render",
    "Faces-Request",
    "X-Requested-With",
    "frmPrincipal:ano",
    "frmPrincipal:mes",
    "frmPrincipal:dia",
    "frmPrincipal:cmbTipoComprobante",
}


def safe_post_data(post_data: str) -> str:
    if not post_data:
        return "sin_body"

    parts = []
    for name, value in parse_qsl(post_data, keep_blank_values=True):
        if name in SENSITIVE_FIELDS:
            parts.append(f"{name}=[REDACTED]")
        elif name in SAFE_FIELDS:
            value = unquote_plus(value)
            if len(value) > 200:
                value = value[:200] + "...[TRUNCADO]"
            parts.append(f"{name}={value}")
    return " | ".join(parts) if parts else "sin_campos_relevantes"


def _limpiar_html(html: str) -> str:
    """Convierte el HTML del detalle JSF en texto legible, sin exponer secretos."""
    html = re.sub(r"(?is)<script\b[^>]*>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style\b[^>]*>.*?</style>", " ", html)
    html = html.replace("&nbsp;", " ")
    html = re.sub(r"(?i)<br\s*/?>", "\n", html)
    html = re.sub(r"(?i)</(?:div|p|tr|li|h[1-6])\s*>", "\n", html)
    html = re.sub(r"(?i)</td\s*>", " | ", html)
    html = re.sub(r"<[^>]+>", " ", html)
    html = unquote_plus(html)
    html = re.sub(r"[ \t]+", " ", html)
    html = re.sub(r"\n\s*\n+", "\n", html)
    return html.strip()


def extraer_update_detalle(body: str) -> str | None:
    """Extrae exclusivamente el CDATA del update del panel de detalle JSF."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None

    wanted = {
        "form-detalle-factura:panel-detalle-factura",
        "panel-detalle-factura",
    }

    for update in root.iter():
        tag = update.tag.rsplit("}", 1)[-1]
        if tag != "update":
            continue
        if (update.attrib.get("id") or "") in wanted:
            return "".join(update.itertext())

    return None


def _valor_por_etiqueta(texto: str, etiquetas: tuple[str, ...]) -> str | None:
    """Busca 'Etiqueta valor' dentro del texto visible del detalle."""
    for etiqueta in etiquetas:
        patron = (
            rf"{re.escape(etiqueta)}\s*[:\-]?\s*"
            r"(.{1,180}?)(?=\s+(?:RUC|Número RUC|Clave de acceso|"
            r"Establecimiento|Punto de emisión|Secuencial|Fecha Emisión|"
            r"Razón Social|Nombre Comercial|Tipo de emisión|Total Sin impuestos|"
            r"Total Descuento|Total Propina|$))"
        )
        match = re.search(patron, texto, flags=re.I | re.S)
        if match:
            valor = re.sub(r"\s+", " ", match.group(1)).strip(" |:-")
            if valor:
                return valor
    return None


def extraer_campos_detalle(body: str) -> dict[str, str]:
    """Extrae campos de negocio del HTML contenido en el update JSF."""
    html = extraer_update_detalle(body)
    if not html:
        return {}

    texto = _limpiar_html(html)
    campos: dict[str, str] = {}

    patrones = {
        "ruc_proveedor": (
            r"(?:Número\s+RUC|RUC)\s*[:\-]?\s*(\d{13})",
        ),
        "clave_acceso": (
            r"Clave\s+de\s+acceso\s*[:\-]?\s*(\d{49})",
        ),
        "establecimiento": (
            r"Establecimiento\s*[:\-]?\s*(\d{3})",
        ),
        "punto_emision": (
            r"Punto\s+de\s+emisión\s*[:\-]?\s*(\d{3})",
            r"Punto\s+de\s+emision\s*[:\-]?\s*(\d{3})",
        ),
        "secuencial": (
            r"Secuencial\s*[:\-]?\s*(\d{9})",
        ),
        "fecha_emision": (
            r"Fecha\s+Emisión\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})",
            r"Fecha\s+Emision\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})",
        ),
        "total_sin_impuestos": (
            r"Total\s+Sin\s+impuestos\s*[:\-]?\s*([0-9]+(?:[.,][0-9]+)?)",
        ),
        "total_descuento": (
            r"Total\s+Descuento\s*[:\-]?\s*([0-9]+(?:[.,][0-9]+)?)",
        ),
        "total_propina": (
            r"Total\s+Propina\s*[:\-]?\s*([0-9]+(?:[.,][0-9]+)?)",
        ),
    }

    for nombre, variantes in patrones.items():
        for patron in variantes:
            match = re.search(patron, texto, flags=re.I)
            if match:
                campos[nombre] = match.group(1).strip()
                break

    # En este detalle el texto muestra la etiqueta y el valor inmediatamente
    # después. Se conserva el valor hasta la siguiente etiqueta conocida.
    etiquetas_sociales = (
        "Razón Social", "Razon Social",
        "Nombre Comercial", "Nombre comercial",
    )
    for etiqueta in etiquetas_sociales:
        match = re.search(
            rf"{re.escape(etiqueta)}\s*[:\-]?\s*(.+?)(?=\s+(?:Número RUC|RUC|"
            r"Clave de acceso|Tipo de emisión|Tipo de emision|Establecimiento|"
            r"Punto de emisión|Punto de emision|Secuencial|Fecha Emisión|"
            r"Fecha Emision)\b)",
            texto,
            flags=re.I | re.S,
        )
        if match:
            valor = re.sub(r"\s+", " ", match.group(1)).strip(" |:-")
            if valor:
                if etiqueta.lower().startswith("razón") or etiqueta.lower().startswith("razon"):
                    campos.setdefault("razon_social", valor)
                else:
                    campos.setdefault("nombre_comercial", valor)

    return campos


def resumir_detalle(body: str) -> list[str]:
    """Resumen seguro para diagnóstico; prioriza el panel de detalle real."""
    html = extraer_update_detalle(body)
    if html:
        texto = _limpiar_html(html)
        campos = extraer_campos_detalle(body)
        encontrados = [
            f"{nombre}={valor}"
            for nombre, valor in campos.items()
        ]

        # Mostrar unas líneas del texto visible ayuda a validar campos que aún
        # no tengan parser, sin registrar el body completo.
        if texto:
            muestra = re.sub(r"\s+", " ", texto)
            encontrados.append("texto_detalle=" + muestra[:1200])

        return encontrados

    return ["No se encontró el <update> del panel de detalle en la respuesta JSF."]



async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inicia sesión en SRI y deja Chromium abierto para navegación manual."
    )
    parser.add_argument("ruc", help="RUC de un cliente activo de BdTotal")
    args = parser.parse_args()

    original_headless = settings.SRI_HEADLESS
    settings.SRI_HEADLESS = False

    p = browser = page = None
    started = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"sri_manual_{args.ruc}_{started}.log"

    def log(message: str = "") -> None:
        line = f"[{dt.datetime.now().strftime('%H:%M:%S.%f')[:-3]}] {message}"
        print(line, flush=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")

    try:
        cred = SriClienteSyncService._credenciales(args.ruc)
        log(f"Cliente: {cred['nombre']}")
        log("Iniciando sesión SRI con Chromium visible...")

        p, browser, page = await SriClienteSyncService._login(args.ruc, cred["clave"])

        async def registrar_request(request) -> None:
            try:
                if "sri.gob.ec" not in request.url:
                    return
                log(f"REQUEST {request.method} {request.url}")

                if request.method == "POST" and "comprobantesRecibidos.jsf" in request.url:
                    headers = request.headers
                    log(
                        "POST_HEADERS "
                        f"Faces-Request={headers.get('faces-request', '')} | "
                        f"X-Requested-With={headers.get('x-requested-with', '')}"
                    )
                    log("POST_DATA " + safe_post_data(request.post_data or ""))
            except Exception as exc:
                log(f"REQUEST ERROR {type(exc).__name__}: {exc}")

        async def registrar_response(response) -> None:
            try:
                if "sri.gob.ec" not in response.url:
                    return

                if response.request.method == "POST" or any(
                    x in response.url.lower()
                    for x in ("comprobantesrecibidos", "j_security_check", "recaptcha")
                ):
                    log(
                        f"RESPONSE {response.status} {response.request.method} "
                        f"{response.url} | content_type="
                        f"{response.headers.get('content-type', '')}"
                    )

                    if (
                        response.request.method == "POST"
                        and "comprobantesRecibidos.jsf" in response.url
                    ):
                        try:
                            body = await response.text()
                            keys = len(re.findall(r"\b\d{49}\b", body))
                            panel = "frmPrincipal:panelListaComprobantes" in body
                            detalle = (
                                "form-detalle-factura:panel-detalle-factura" in body
                                or "panel-detalle-factura" in body
                            )
                            captcha = "captcha" in body.lower()
                            log(
                                "POST_RESULT "
                                f"panel={'SI' if panel else 'NO'} | "
                                f"detalle={'SI' if detalle else 'NO'} | "
                                f"claves_49_digitos={keys} | "
                                f"menciona_captcha={'SI' if captcha else 'NO'} | "
                                f"bytes={len(body.encode('utf-8', errors='ignore'))}"
                            )

                            if detalle:
                                log("DETALLE_CAMPOS_INFERIDOS:")
                                for item in resumir_detalle(body):
                                    log("  " + item)
                        except Exception as exc:
                            log(f"POST_RESULT ERROR {type(exc).__name__}: {exc}")
            except Exception as exc:
                log(f"RESPONSE ERROR {type(exc).__name__}: {exc}")

        page.on("request", lambda request: asyncio.create_task(registrar_request(request)))
        page.on("response", lambda response: asyncio.create_task(registrar_response(response)))
        page.on("pageerror", lambda exc: log(f"PAGE_ERROR {str(exc)[:500]}"))

        log("")
        log("==============================================")
        log(" SESIÓN MANUAL SRI - CAPTURA DE DETALLE")
        log("==============================================")
        log("La sesión ya está iniciada.")
        log("1. Entra a Comprobantes Recibidos.")
        log("2. Selecciona año y mes.")
        log("3. Presiona Buscar.")
        log("4. Haz clic en una Clave de Acceso.")
        log("5. Espera a que aparezca el detalle.")
        log("")
        log(f"LOG: {log_path.resolve()}")
        log("Se registran parámetros JSF seguros y un resumen de campos visibles del detalle.")
        log("NO se registran contraseñas, cookies, tokens reCAPTCHA ni valores ViewState.")
        log("")

        await asyncio.to_thread(
            input,
            "Cuando termines las pruebas, presiona ENTER aquí para cerrar Chromium... ",
        )

    finally:
        settings.SRI_HEADLESS = original_headless
        if browser is not None:
            await browser.close()
        if p is not None:
            await p.stop()
        log("Sesión finalizada.")


if __name__ == "__main__":
    asyncio.run(main())
