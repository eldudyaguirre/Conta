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


class _TableParser(HTMLParser):
    """Extrae filas/celdas de la tabla HTML incluida dentro del CDATA de SRI."""

    def __init__(self) -> None:
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.current_row: list[str] = []
        self.current_cell: list[str] = []
        self.rows: list[list[str]] = []
        self._table_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attrs_dict = dict(attrs)
        if tag == "table":
            table_id = (attrs_dict.get("id") or "").lower()
            table_class = (attrs_dict.get("class") or "").lower()
            if "tablacomprecibidos" in table_id or "tablacomprecibidos" in table_class:
                self.in_table = True
                self._table_depth = 1
            elif self.in_table:
                self._table_depth += 1
        elif self.in_table and tag == "tr":
            self.in_row = True
            self.current_row = []
        elif self.in_table and self.in_row and tag in {"td", "th"}:
            self.in_cell = True
            self.current_cell = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if not self.in_table:
            return
        if tag in {"td", "th"} and self.in_cell:
            value = re.sub(r"\\s+", " ", "".join(self.current_cell)).strip()
            self.current_row.append(html.unescape(value))
            self.current_cell = []
            self.in_cell = False
        elif tag == "tr" and self.in_row:
            if any(cell.strip() for cell in self.current_row):
                self.rows.append(self.current_row)
            self.current_row = []
            self.in_row = False
        elif tag == "table":
            self._table_depth -= 1
            if self._table_depth <= 0:
                self.in_table = False
                self._table_depth = 0

    def handle_data(self, data: str) -> None:
        if self.in_table and self.in_cell:
            self.current_cell.append(data)


def _extraer_comprobantes(respuesta: str) -> list[list[str]]:
    """Extrae las filas de tablaCompRecibidos desde la respuesta partial-response de JSF."""
    panel_match = re.search(
        r'<update[^>]+id=["\']frmPrincipal:panelListaComprobantes["\'][^>]*>\\s*<!\\[CDATA\\[(.*?)\\]\\]>',
        respuesta,
        re.IGNORECASE | re.DOTALL,
    )
    if not panel_match:
        return []

    panel_html = html.unescape(panel_match.group(1))
    parser = _TableParser()
    parser.feed(panel_html)
    return parser.rows


def _normalizar_comprobantes(filas: list[list[str]]) -> list[dict[str, str]]:
    """Convierte la tabla SRI en registros con nombres de columnas estables."""
    if not filas:
        return []

    # SRI puede devolver cabeceras con una o varias filas. Tomamos la primera
    # fila que tenga nombres de columnas reconocibles.
    header_index = next(
        (
            i for i, fila in enumerate(filas)
            if any("RUC" in celda.upper() for celda in fila)
            and any("CLAVE" in celda.upper() or "AUTORIZ" in celda.upper() for celda in fila)
        ),
        None,
    )
    if header_index is None:
        return []

    headers = [re.sub(r"\\s+", " ", x).strip() for x in filas[header_index]]
    datos: list[dict[str, str]] = []
    for fila in filas[header_index + 1:]:
        if len(fila) < 2:
            continue
        registro = {
            headers[i] if i < len(headers) and headers[i] else f"columna_{i + 1}": fila[i]
            for i in range(min(len(headers), len(fila)))
        }
        # Descarta filas de paginación/controles sin una clave de acceso.
        texto = " ".join(fila)
        if re.search(r"\\b\\d{49}\\b", texto):
            datos.append(registro)
    return datos


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

        # Ejecutar primero la consulta REAL dentro del navegador.
        # Playwright deja que el JavaScript de SRI genere y envíe
        # reCAPTCHA Enterprise de forma legítima; no se reutiliza ni
        # se fabrica ningún token.
        print("2. Ejecutando consulta real dentro de Playwright...")
        await page.locator("#frmPrincipal\\:ano").select_option(str(args.anio))

        # SRI no usa necesariamente 01..12 como value del combo de meses.
        # Descubrimos el option real y seleccionamos por value o label.
        mes_locator = page.locator("#frmPrincipal\\:mes")
        opciones_mes = await mes_locator.locator("option").evaluate_all(
            """els => els.map(e => ({value: e.value, text: (e.textContent || '').trim()}))"""
        )
        print(f"   Opciones de mes disponibles: {opciones_mes}")

        mes_num = args.mes
        mes_nombres = [
            "", "enero", "febrero", "marzo", "abril", "mayo", "junio",
            "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
        ]
        candidatos = {
            f"{mes_num:02d}",
            str(mes_num),
            mes_nombres[mes_num],
            mes_nombres[mes_num].capitalize(),
        }

        opcion_mes = next(
            (
                o for o in opciones_mes
                if str(o["value"]).strip() in candidatos
                or str(o["text"]).strip().lower() in {x.lower() for x in candidatos}
                or str(o["text"]).strip().lower().startswith(mes_nombres[mes_num])
            ),
            None,
        )
        if not opcion_mes:
            raise RuntimeError(
                f"No se encontró el mes {mes_num} en el combo SRI. "
                f"Opciones: {opciones_mes}"
            )

        print(f"   Mes seleccionado: {opcion_mes}")
        await mes_locator.select_option(value=opcion_mes["value"])

        await page.locator("#frmPrincipal\\:dia").select_option("0")
        await page.locator("#frmPrincipal\\:cmbTipoComprobante").select_option(str(args.tipo))

        # El botón Consultar ejecuta executeRecaptcha(...) y luego PrimeFaces.ab(...)
        # desde su propio onclick. Al hacer click queda temporalmente disabled
        # mientras Google/SRI genera el token. Por eso NO debemos intentar
        # hacer un segundo click sobre el mismo botón.
        print("   Ejecutando Consultar y esperando la respuesta AJAX real...")

        respuestas = []

        def es_post_sri(response):
            return (
                "comprobantesRecibidos.jsf" in response.url
                and response.request.method == "POST"
            )

        page.on("response", lambda response: respuestas.append(response) if es_post_sri(response) else None)

        # Diagnóstico del flujo reCAPTCHA: solo registramos URLs/tipos,
        # nunca tokens ni cookies.
        recaptcha_requests = []
        page.on(
            "request",
            lambda request: recaptcha_requests.append(request.url)
            if any(x in request.url.lower() for x in ("recaptcha", "gstatic.com/recaptcha"))
            else None,
        )
        page_errors = []
        page.on("pageerror", lambda exc: page_errors.append(str(exc)[:300]))

        boton = page.locator("#frmPrincipal\\:btnBuscar")
        print("   onclick Consultar:", (await boton.get_attribute("onclick") or "")[:500])
        print("   disabled antes:", await boton.is_disabled())

        # SRI inicializa reCAPTCHA Enterprise mediante JavaScript propio.
        # Esperamos explícitamente a que el cliente exista antes del click.
        try:
            await page.wait_for_function(
                """() => typeof grecaptcha !== 'undefined' &&
                         grecaptcha.enterprise &&
                         typeof grecaptcha.enterprise.execute === 'function'""",
                timeout=30000,
            )
            print("   reCAPTCHA Enterprise API: disponible")
        except Exception:
            print("   reCAPTCHA Enterprise API: NO disponible")
            print("   Scripts reCAPTCHA cargados:")
            for url in await page.locator("script[src]").evaluate_all(
                """els => els.map(e => e.src).filter(u => u.toLowerCase().includes('recaptcha'))"""
            ):
                print(f"      {url}")

        # Comprobar si la página de SRI expone su inicializador.
        estado_recaptcha = await page.evaluate(
            """() => ({
                grecaptcha: typeof grecaptcha !== 'undefined',
                enterprise: typeof grecaptcha !== 'undefined' && !!grecaptcha.enterprise,
                execute: typeof grecaptcha !== 'undefined' && !!grecaptcha.enterprise &&
                         typeof grecaptcha.enterprise.execute === 'function',
                sriExecute: typeof executeRecaptcha === 'function'
            })"""
        )
        print(f"   Estado JS reCAPTCHA: {estado_recaptcha}")

        # Dejamos un margen para que sri-reCAPTCHAEnterprise.js cree el cliente.
        await page.wait_for_timeout(5000)

        await boton.click()

        # Esperamos a que termine el flujo de reCAPTCHA + PrimeFaces.
        await page.wait_for_timeout(30000)

        print(f"   Peticiones relacionadas con reCAPTCHA: {len(recaptcha_requests)}")
        if recaptcha_requests:
            dominios = sorted({re.sub(r"^https?://([^/]+).*", r"\\1", u) for u in recaptcha_requests})
            print(f"   Dominios reCAPTCHA detectados: {dominios}")
        print(f"   Errores JavaScript de página: {len(page_errors)}")
        if page_errors:
            print(f"   Primer error JS: {page_errors[0]}")
        print("   disabled después:", await boton.is_disabled())

        # Diagnóstico seguro del campo generado por reCAPTCHA. Nunca mostramos
        # el token, solo si existe y su longitud.
        try:
            captcha_fields = await page.locator(
                'textarea[name="g-recaptcha-response"], input[name="g-recaptcha-response"]'
            ).evaluate_all(
                """els => els.map(e => ({
                    tag: e.tagName,
                    name: e.name,
                    valueLength: (e.value || '').length,
                    visible: !!(e.offsetWidth || e.offsetHeight || e.getClientRects().length)
                }))"""
            )
            print(f"   Campos g-recaptcha-response en DOM: {captcha_fields}")
        except Exception as exc:
            print(f"   No se pudo inspeccionar g-recaptcha-response: {type(exc).__name__}")

        post_sri = [
            response for response in respuestas
            if response.status == 200
        ]

        print(f"   POST SRI detectados: {len(post_sri)}")
        for idx, response in enumerate(post_sri, start=1):
            try:
                cuerpo_diag = await response.text()
                post_data = response.request.post_data or ""
                captcha_match = re.search(
                    r"(?:^|&)g-recaptcha-response=([^&]*)",
                    post_data,
                    re.IGNORECASE,
                )
                captcha_value = captcha_match.group(1) if captcha_match else ""
                captcha_len = len(captcha_value)
                print(
                    f"      POST {idx}: bytes={len(cuerpo_diag.encode('utf-8', errors='ignore'))}, "
                    f"panel={'sí' if 'id="frmPrincipal:panelListaComprobantes"' in cuerpo_diag else 'no'}, "
                    f"captcha={'sí' if 'captcha' in cuerpo_diag.lower() else 'no'}, "
                    f"token_recaptcha={'presente' if captcha_len else 'vacío'}, "
                    f"token_len={captcha_len}"
                )
            except Exception as exc:
                print(f"      POST {idx}: no se pudo leer respuesta ({type(exc).__name__})")

        if not post_sri:
            boton = page.locator("#frmPrincipal\\:btnBuscar")
            print(
                "   Estado Consultar: "
                f"disabled={await boton.is_disabled()}"
            )
            print("   No se recibió POST AJAX después de esperar 15 segundos.")
            print("   URL actual:", page.url)
            raise RuntimeError(
                "SRI no produjo la petición AJAX de consulta después de executeRecaptcha."
            )

        # Leer las respuestas de forma asíncrona y guardar el cuerpo una sola vez.
        # No usamos next() con await dentro de una expresión: response.text()
        # es asíncrono y debe resolverse mediante un bucle normal.
        respuestas_leidas: list[tuple[Any, str]] = []
        for response in post_sri:
            try:
                cuerpo = await response.text()
            except Exception as exc:
                print(
                    f"      POST: no se pudo leer el cuerpo "
                    f"({type(exc).__name__}: {exc})"
                )
                continue
            respuestas_leidas.append((response, cuerpo))

        # Un panel en el XML parcial NO significa que la consulta haya tenido
        # éxito. SRI puede devolver el panel con un mensaje de CAPTCHA/error.
        # Consideramos éxito solamente si hay al menos una clave de acceso
        # de 49 dígitos en la respuesta.
        respuesta_real = None
        cuerpo_real = ""

        for response, cuerpo in reversed(respuestas_leidas):
            if re.search(r"\b\d{49}\b", cuerpo):
                respuesta_real = response
                cuerpo_real = cuerpo
                break

        if respuesta_real is not None:
            print(f"   POST consulta REAL HTTP: {respuesta_real.status}")
            print(
                f"   Content-Type: "
                f"{respuesta_real.headers.get('content-type', '')}"
            )
            print(f"   Resumen respuesta real: {_resumen_respuesta(cuerpo_real)}")

            filas = _extraer_comprobantes(cuerpo_real)
            registros = _normalizar_comprobantes(filas)

            print("   Tabla válida de comprobantes: sí")
            print(f"   Filas de tabla extraídas: {len(filas)}")
            print(f"   Comprobantes reconocidos: {len(registros)}")

            if registros:
                print("   Encabezados:", list(registros[0].keys()))
                print("   Primeros comprobantes:")
                for i, registro in enumerate(registros[:10], start=1):
                    print(f"      {i}. {registro}")
                print(
                    "   RESULTADO: la consulta real del navegador funciona "
                    "y la tabla fue extraída."
                )
            else:
                print(
                    "   La respuesta contiene claves de acceso, pero el parser "
                    "todavía no pudo convertirlas en filas."
                )
        else:
            # No hubo una respuesta con comprobantes. Mostramos la respuesta
            # más relevante para diagnóstico, sin exponer tokens ni cookies.
            if respuestas_leidas:
                respuesta_real, cuerpo_real = respuestas_leidas[-1]
                print(f"   POST consulta REAL HTTP: {respuesta_real.status}")
                print(
                    f"   Content-Type: "
                    f"{respuesta_real.headers.get('content-type', '')}"
                )
                print(f"   Resumen respuesta real: {_resumen_respuesta(cuerpo_real)}")
                tiene_panel = 'id="frmPrincipal:panelListaComprobantes"' in cuerpo_real
                print(
                    f"   PanelListaComprobantes presente: "
                    f"{'sí' if tiene_panel else 'no'}"
                )
                print(
                    f"   Clave de acceso de 49 dígitos: "
                    f"{'sí' if re.search(r'\b\d{49}\b', cuerpo_real) else 'no'}"
                )
                print(
                    "   RESULTADO: SRI no devolvió comprobantes válidos "
                    "en esta consulta."
                )
                print(
                    "   Respuesta resumida:",
                    re.sub(r"\s+", " ", cuerpo_real[:2500]).strip(),
                )
            else:
                print(
                    "   RESULTADO: no hubo respuestas SRI que pudieran "
                    "ser analizadas."
                )

        cookies = await page.context.cookies()

        cookie_jar = {
            cookie["name"]: cookie["value"]
            for cookie in cookies
            if cookie.get("domain", "").endswith("sri.gob.ec")
        }

        print(f"   Página autenticada: {page.url}")
        print(f"   Cookies SRI transferibles: {len(cookie_jar)}")
        print(f"   ViewState obtenido: {'sí' if view_state else 'no'}")

        print("3. Reutilizando cookies con HTTPX para comparar la sesión...")
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

            print("4. Enviando POST JSF de preparación sin reCAPTCHA...")
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

            print(f"   POST preparación HTTP: {response.status_code}")
            print(f"   Resumen: {_resumen_respuesta(response.text)}")

            # El primer POST puede devolver un ViewState nuevo. Lo extraemos
            # sin mostrar su valor.
            partial_view_states = re.findall(
                r'<update[^>]+id=["\']javax\.faces\.ViewState["\'][^>]*><!\[CDATA\[(.*?)\]\]>',
                response.text,
                re.IGNORECASE | re.DOTALL,
            )
            if partial_view_states:
                http_view_state = html.unescape(partial_view_states[-1]).strip()
                print("   ViewState actualizado desde respuesta AJAX: sí")
            elif "javax.faces.ViewState" in response.text:
                try:
                    http_view_state = _view_state(response.text)
                    print("   ViewState actualizado desde respuesta AJAX: sí")
                except RuntimeError:
                    print("   ViewState actualizado desde respuesta AJAX: no")
                    return
            else:
                print("   ViewState actualizado desde respuesta AJAX: no")
                return

            print("5. Enviando segundo POST JSF (consulta real) sin reCAPTCHA...")
            data = {
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": "frmPrincipal:j_idt36",
                "javax.faces.partial.execute": "@all",
                "javax.faces.partial.render": "frmPrincipal:panelListaComprobantes frmPrincipal:tablaCompRecibidos",
                "frmPrincipal:j_idt36": "frmPrincipal:j_idt36",
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

            print(f"   POST consulta HTTP: {response.status_code}")
            print(f"   Content-Type: {response.headers.get('content-type', '')}")
            print(f"   Resumen: {_resumen_respuesta(response.text)}")
            filas_http = _extraer_comprobantes(response.text)
            registros_http = _normalizar_comprobantes(filas_http)
            print(f"   Filas de tabla extraídas por HTTPX: {len(filas_http)}")
            print(f"   Comprobantes reconocidos por HTTPX: {len(registros_http)}")
            print(f"   Tamaño respuesta consulta: {len(response.content)} bytes")

            if registros_http:
                print("   RESULTADO HTTPX: la tabla fue extraída sin reCAPTCHA.")
            else:
                print("   RESULTADO HTTPX: no se obtuvieron comprobantes; la respuesta no contiene")
                print("   una tabla válida de comprobantes. Esto confirma que el token reCAPTCHA")
                print("   generado por el navegador forma parte del flujo necesario.")


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
