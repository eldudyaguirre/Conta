import os
import platform
import sys
from getpass import getuser
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from playwright.async_api import async_playwright

from app.core.config import settings
from app.security.dependencies import get_admin_user


router = APIRouter(
    prefix="/admin/sri",
    tags=["Administración SRI"],
)


@router.post("/diagnostico")
async def diagnostico_sri(usuario: dict = Depends(get_admin_user)):
    """
    Diagnóstico del acceso SRI desde el mismo proceso de Conta/NSSM.
    No realiza login ni consulta comprobantes.
    """
    p = browser = None

    info = {
        "usuario_admin": usuario["usrname"],
        "windows_user": getuser(),
        "whoami": os.environ.get("USERNAME", ""),
        "userdomain": os.environ.get("USERDOMAIN", ""),
        "userprofile": os.environ.get("USERPROFILE", ""),
        "home": os.environ.get("HOME", ""),
        "localappdata": os.environ.get("LOCALAPPDATA", ""),
        "playwright_browsers_path": os.environ.get("PLAYWRIGHT_BROWSERS_PATH", ""),
        "python": sys.executable,
        "python_version": platform.python_version(),
        "sri_headless": settings.SRI_HEADLESS,
        "login_url": SriClienteSyncDiagnostic.LOGIN_URL,
    }

    try:
        p = await async_playwright().start()
        info["playwright_version"] = p.chromium
        browser = await p.chromium.launch(headless=settings.SRI_HEADLESS)
        info["browser_launched"] = True

        page = await browser.new_page()
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        try:
            response = await page.goto(
                SriClienteSyncDiagnostic.LOGIN_URL,
                wait_until="commit",
                timeout=60000,
            )
            info["goto_ok"] = True
            info["http_status"] = response.status if response else None
            info["url"] = page.url
            info["title"] = await page.title()
        except Exception as exc:
            info["goto_ok"] = False
            info["url"] = page.url
            info["goto_error"] = str(exc)
            info["goto_error_type"] = type(exc).__name__

            try:
                info["page_title_after_error"] = await page.title()
            except Exception:
                pass

            try:
                info["content_length_after_error"] = len(await page.content())
            except Exception:
                pass

            raise HTTPException(
                status_code=502,
                detail=info,
            )

        return info

    except HTTPException:
        raise
    except Exception as exc:
        info["diagnostic_error"] = str(exc)
        info["diagnostic_error_type"] = type(exc).__name__
        raise HTTPException(status_code=500, detail=info)
    finally:
        if browser is not None:
            await browser.close()
        if p is not None:
            await p.stop()


class SriClienteSyncDiagnostic:
    LOGIN_URL = "https://srienlinea.sri.gob.ec/sri-en-linea/contribuyente/perfil"
