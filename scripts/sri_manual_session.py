from __future__ import annotations

import argparse
import asyncio
import datetime as dt
from pathlib import Path

from app.core.config import settings
from app.services.sri_cliente_sync_service import SriClienteSyncService


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
            except Exception as exc:
                log(f"RESPONSE ERROR {type(exc).__name__}: {exc}")

        page.on("request", lambda request: asyncio.create_task(registrar_request(request)))
        page.on("response", lambda response: asyncio.create_task(registrar_response(response)))
        page.on("pageerror", lambda exc: log(f"PAGE_ERROR {str(exc)[:500]}"))

        log("")
        log("==============================================")
        log(" SESIÓN MANUAL SRI")
        log("==============================================")
        log("La sesión ya está iniciada.")
        log("Ahora tú manejas completamente la ventana de Chromium.")
        log("1. Entra manualmente a Comprobantes Recibidos.")
        log("2. Selecciona año y mes.")
        log("3. Presiona Consultar/Buscar.")
        log("4. Espera a que SRI termine.")
        log("5. Puedes repetir la prueba con otros meses.")
        log("")
        log(f"LOG: {log_path.resolve()}")
        log("Por seguridad, este registro NO guarda contraseñas, cookies, tokens ni contenido de formularios.")
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
