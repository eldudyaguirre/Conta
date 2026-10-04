from __future__ import annotations

import argparse
import asyncio
import html
import re
from html.parser import HTMLParser
from typing import Any

import httpx

from app.core.config import settings
from app.services.sri_cliente_sync_service import SriClienteSyncService


class _HiddenInputParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.fields: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "input":
            return
        data = dict(attrs)
        name = data.get("name")
        if name:
            self.fields[name] = data.get("value") or ""


def _form_fields(page_html: str) -> dict[str, str]:
    parser = _HiddenInputParser()
    parser.feed(page_html)
    return parser.fields


def _view_state(page_html: str) -> str:
    patterns = [
        r'name="javax\.faces\.ViewState"[^>]*value="([^"]+)"',
        r'name="javax\.faces\.ViewState"[^>]*value=\'([^\']+)\'',
    ]
    for pattern in patterns:
        match = re.search(pattern, page_html, re.IGNORECASE)
        if match:
            return html.unescape(match.group(1))
    raise RuntimeError("No se encontró javax.faces.ViewState en la página SRI.")


def _resumen_respuesta(texto: str) -> dict[str, Any]:
    lower = texto.lower()
    return {
        "bytes": len(texto.encode("utf-8", errors="ignore")),
        "contiene_viewstate": "javax.faces.ViewState" in texto,
        "contiene_lista_comprobantes": "Lista de comprobantes recibidos" in texto,
        "menciona_recaptcha": "recaptcha" in lower,
        "menciona_captcha": "captcha" in lower,
        "menciona_error": "error" in lower,
        "primeros_300_caracteres": re.sub(r"\\s+", " ", texto[:300]).strip(),
    }


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Comprueba si una sesión autenticada de Playwright SRI puede reutilizarse con HTTPX."
    )
    parser.add_argument("ruc", help="RUC de un cliente activo de BdTotal")
    parser.add_argument("--anio", type=int, required=True)
    parser.add_argument("--mes", type=int, required=True)
    parser.add_argument("--tipo", type=int, default=1)
    args = parser.parse_args()

    if not 1 <= args.mes <= 12:
        raise SystemExit("--mes debe estar entre 1 y 12")
    if not 1 <= args.tipo <= 7:
        raise SystemExit("--tipo debe estar entre 1 y 7")

    # Este script es diagnóstico. La navegación se hace visible porque el
    # diagnóstico anterior demostró que SRI funciona así, mientras que
    # Chromium headless no consigue completar la navegación inicial.
    original_headless = settings.SRI_HEADLESS
    settings.SRI_HEADLESS = False

    p = browser = page = None
    try:
        cred = SriClienteSyncService._credenciales(args.ruc)
        print(f"Cliente: {cred['nombre']}")
        print("1. Iniciando sesión SRI con Playwright visible...")

        p, browser, page = await SriClienteSyncService._login(args.ruc, cred["clave"])
        await page.goto(
            SriClienteSyncService.RECIBIDOS_URL,
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await page.wait_for_selector(
            "#frmPrincipal\\:ano",
            timeout=30000,
        )

        page_html = await page.content()
        view_state = _view_state(page_html)
        cookies = await page.context.cookies()

        cookie_jar = {
            cookie["name"]: cookie["value"]
            for cookie in cookies
            if cookie.get("domain", "").endswith("sri.gob.ec")
        }

        print(f"   Página autenticada: {page.url}")
        print(f"   Cookies SRI transferibles: {len(cookie_jar)}")
        print(f"   ViewState obtenido: {'sí' if view_state else 'no'}")

        print("2. Reutilizando cookies con HTTPX...")
        async with httpx.AsyncClient(
            cookies=cookie_jar,
            follow_redirects=True,
            timeout=30.0,
            headers={
                "User-Agent": await page.evaluate("navigator.userAgent"),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        ) as client:
            response = await client.get(SriClienteSyncService.RECIBIDOS_URL)

            print(f"   GET HTTP: {response.status_code}")
            print(f"   URL final: {response.url}")
            print(f"   Sesión reutilizada: {'sí' if 'comprobantesRecibidos.jsf' in str(response.url) else 'no'}")
            print(f"   ViewState HTTP: {'sí' if 'javax.faces.ViewState' in response.text else 'no'}")
            print(f"   Content-Type: {response.headers.get('content-type', '')}")
            print(f"   Longitud respuesta: {len(response.content)} bytes")
            print(f"   Primeros 500 caracteres: {re.sub(r"\\s+", " ", response.text[:500]).strip()}")

            if 'javax.faces.ViewState' not in response.text:
                # El SRI puede entregar un formulario automático de
                # j_security_check. El navegador lo envía mediante onload.
                # Reproducimos únicamente ese flujo HTTP normal.
                action_match = re.search(
                    r'<form[^>]+action=[\"\']([^\"\']*j_security_check[^\"\']*)[\"\'][^>]*>',
                    response.text,
                    re.IGNORECASE,
                )
                fields = _form_fields(response.text)

                if action_match and fields:
                    action = action_match.group(1)
                    if action.startswith("http"):
                        auth_url = action
                    else:
                        auth_url = str(response.url).rsplit("/", 1)[0] + "/" + action.lstrip("/")

                    print("   SRI devolvió j_security_check: sí")
                    print(f"   Campos del formulario: {len(fields)}")

                    auth_response = await client.post(
                        auth_url,
                        data=fields,
                        headers={
                            "Referer": str(response.url),
                            "Origin": "https://srienlinea.sri.gob.ec",
                        },
                    )

                    print(f"   POST j_security_check: {auth_response.status_code}")
                    print(f"   URL después de j_security_check: {auth_response.url}")
                    print(
                        "   ViewState después de j_security_check: "
                        f"{'sí' if 'javax.faces.ViewState' in auth_response.text else 'no'}"
                    )
                    print(f"   Respuesta final: {len(auth_response.content)} bytes")

                    if 'javax.faces.ViewState' in auth_response.text:
                        http_view_state = _view_state(auth_response.text)
                    else:
                        print()
                        print("DIAGNÓSTICO:")
                        print("HTTPX ejecutó el paso j_security_check, pero todavía no recibió el JSF.")
                        print("No continuamos con la consulta.")
                        return
                else:
                    print()
                    print("DIAGNÓSTICO:")
                    print("HTTPX recibió 200 pero no encontró un formulario j_security_check utilizable.")
                    print("No continuamos con la consulta.")
                    return
            else:
                http_view_state = _view_state(response.text)

            print("3. Enviando POST JSF de prueba sin reCAPTCHA...")
            # No se intenta falsificar ni reutilizar un token reCAPTCHA.
            # El objetivo es comprobar que HTTPX conserva la sesión y llega
            # al endpoint JSF. El SRI puede rechazar esta petición por
            # ausencia de reCAPTCHA, lo cual es precisamente un resultado
            # útil para el diagnóstico.
            data = {
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": "frmPrincipal:btnBuscar",
                "javax.faces.partial.execute": "@all",
                "frmPrincipal:btnBuscar": "frmPrincipal:btnBuscar",
                "frmPrincipal": "frmPrincipal",
                "frmPrincipal:opciones": "ruc",
                "frmPrincipal:ano": str(args.anio),
                "frmPrincipal:mes": str(args.mes),
                "frmPrincipal:dia": "0",
                "frmPrincipal:cmbTipoComprobante": str(args.tipo),
                "g-recaptcha-response": "",
                "javax.faces.ViewState": http_view_state,
            }
            response = await client.post(
                SriClienteSyncService.RECIBIDOS_URL,
                data=data,
                headers={
                    "Accept": "application/xml, text/xml, */*; q=0.01",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Faces-Request": "partial/ajax",
                    "Origin": "https://srienlinea.sri.gob.ec",
                    "Referer": str(response.url),
                    "X-Requested-With": "XMLHttpRequest",
                },
            )

            print(f"   POST HTTP: {response.status_code}")
            print(f"   Content-Type: {response.headers.get('content-type', '')}")
            print(f"   Resumen: {_resumen_respuesta(response.text)}")

        print()
        print("RESULTADO:")
        print("- Playwright autenticó y obtuvo una sesión SRI.")
        print("- Se comprobó si cookies + ViewState funcionan fuera del navegador.")
        print("- El POST se hizo deliberadamente sin token reCAPTCHA; no se intentó bypass.")

    finally:
        settings.SRI_HEADLESS = original_headless
        if browser is not None:
            await browser.close()
        if p is not None:
            await p.stop()


if __name__ == "__main__":
    asyncio.run(main())
