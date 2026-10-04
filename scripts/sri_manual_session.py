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


def resumir_detalle(body: str) -> list[str]:
    encontrados: list[str] = []
    claves = (
        "claveAcceso", "clave de acceso", "autorizacion", "autorización",
        "ruc", "razon social", "razón social", "proveedor", "fecha",
        "comprobante", "establecimiento", "punto de emisión", "punto de emision",
        "secuencial", "subtotal", "base imponible", "iva", "ice", "total",
        "retención", "retencion",
    )
    texto = re.sub(r"\s+", " ", body, flags=re.S)
    texto_lower = texto.lower()

    for clave in claves:
        pos = texto_lower.find(clave.lower())
        if pos >= 0:
            fragmento = texto[max(0, pos - 120): min(len(texto), pos + 350)]
            fragmento = re.sub(r"\s+", " ", fragmento)
            fragmento = re.sub(r"<[^>]+>", " ", fragmento)
            fragmento = re.sub(r"\s+", " ", fragmento).strip()
            encontrados.append(f"{clave}: {fragmento[:500]}")

    return list(dict.fromkeys(encontrados))


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
