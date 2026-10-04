import asyncio
import os
import platform
import socket
import ssl
import sys
import time
from getpass import getuser

from fastapi import APIRouter, Depends, HTTPException
from playwright.async_api import async_playwright

from app.core.config import settings
from app.security.dependencies import get_admin_user


router = APIRouter(
    prefix="/admin/sri",
    tags=["Administración SRI"],
)

TEST_URLS = [
    ("google", "https://www.google.com/"),
    ("sri_home", "https://srienlinea.sri.gob.ec/"),
    ("sri_perfil", "https://srienlinea.sri.gob.ec/sri-en-linea/contribuyente/perfil"),
]


@router.post("/diagnostico")
async def diagnostico_sri(usuario: dict = Depends(get_admin_user)):
    """
    Diagnóstico de conectividad Chromium desde el mismo proceso de Conta/NSSM.
    No realiza login ni consulta comprobantes.
    """
    p = browser = None

    proxy_vars = {
        key: os.environ.get(key, "")
        for key in (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "no_proxy",
        )
    }

    info = {
        "usuario_admin": usuario["usrname"],
        "windows_user": getuser(),
        "whoami": os.environ.get("USERNAME", ""),
        "userdomain": os.environ.get("USERDOMAIN", ""),
        "userprofile": os.environ.get("USERPROFILE", ""),
        "home": os.environ.get("HOME", ""),
        "localappdata": os.environ.get("LOCALAPPDATA", ""),
        "playwright_browsers_path": os.environ.get("PLAYWRIGHT_BROWSERS_PATH", ""),
        "playwright_version": str(getattr(p, "version", "")),
        "python": sys.executable,
        "python_version": platform.python_version(),
        "sri_headless": settings.SRI_HEADLESS,
        "proxy_environment": proxy_vars,
        "tests": [],
        "network": [],

    }

    try:
        p = await async_playwright().start()
        browser = await p.chromium.launch(
            headless=settings.SRI_HEADLESS,
        )
        info["browser_launched"] = True
        info["browser_name"] = "chromium"

        page = await browser.new_page()

        request_failures = []
        responses = []

        page.on("requestfailed", lambda request: request_failures.append({
            "url": request.url,
            "method": request.method,
            "failure": request.failure,
        }))
        page.on("response", lambda response: responses.append({
            "url": response.url,
            "status": response.status,
        }) if ("sri.gob.ec" in response.url or len(responses) < 20) else None)

        async def network_test(host: str, port: int = 443) -> dict:
            item = {"host": host, "port": port}
            started = time.monotonic()
            try:
                addresses = await asyncio.to_thread(socket.getaddrinfo, host, port, type=socket.SOCK_STREAM)
                item["dns_ok"] = True
                item["addresses"] = sorted({x[4][0] for x in addresses})
            except Exception as exc:
                item["dns_ok"] = False
                item["dns_error"] = f"{type(exc).__name__}: {exc}"
                item["elapsed_ms"] = round((time.monotonic() - started) * 1000)
                return item
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port), timeout=10
                )
                item["tcp_ok"] = True
                writer.close()
                await writer.wait_closed()
            except Exception as exc:
                item["tcp_ok"] = False
                item["tcp_error"] = f"{type(exc).__name__}: {exc}"
                item["elapsed_ms"] = round((time.monotonic() - started) * 1000)
                return item
            try:
                def tls_probe():
                    ctx = ssl.create_default_context()
                    with socket.create_connection((host, port), timeout=10) as raw:
                        with ctx.wrap_socket(raw, server_hostname=host) as tls:
                            return tls.version(), tls.getpeercert()
                version, cert = await asyncio.to_thread(tls_probe)
                item["tls_ok"] = True
                item["tls_version"] = version
                item["certificate_subject"] = str(cert.get("subject", ""))[:500]
            except Exception as exc:
                item["tls_ok"] = False
                item["tls_error"] = f"{type(exc).__name__}: {exc}"
            item["elapsed_ms"] = round((time.monotonic() - started) * 1000)
            return item

        for host in ("srienlinea.sri.gob.ec", "www.google.com"):
            info["network"].append(await network_test(host))
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        for name, url in TEST_URLS:
            test = {
                "name": name,
                "url_requested": url,
            }

            try:
                response = await page.goto(
                    url,
                    wait_until="commit",
                    timeout=20000,
                )
                test["ok"] = True
                test["http_status"] = response.status if response else None
                test["url"] = page.url

                try:
                    test["title"] = await page.title()
                except Exception:
                    pass

            except Exception as exc:
                test["ok"] = False
                test["url"] = page.url
                test["error_type"] = type(exc).__name__
                test["error"] = str(exc)
                test["request_failures"] = request_failures[-20:]
                test["responses"] = responses[-20:]

                try:
                    test["title_after_error"] = await page.title()
                except Exception:
                    pass

            info["tests"].append(test)

        info["browser_network_events"] = {"request_failures": request_failures[-50:], "responses": responses[-50:]}

        info["summary"] = {
            test["name"]: test["ok"]
            for test in info["tests"]
        }

        return info

    except Exception as exc:
        info["diagnostic_error"] = str(exc)
        info["diagnostic_error_type"] = type(exc).__name__
        raise HTTPException(status_code=500, detail=info)
    finally:
        if browser is not None:
            await browser.close()
        if p is not None:
            await p.stop()
