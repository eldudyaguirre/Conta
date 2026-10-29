from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
import re
import shutil
import tempfile
import threading
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.core.config import settings
from app.database.connection import engine
from app.services.sri_cliente_sync_service import SriClienteSyncService


logger = logging.getLogger("conta.sri_certificados_ruc")


CERTIFICADO_URL = (
    "https://srienlinea.sri.gob.ec/"
    "sri-catastro-tributario-web-internet/pages/certificado/"
    "opciones-certificado.jsf?&contextoMPT="
    "https://srienlinea.sri.gob.ec/tuportal-internet&pathMPT=RUC"
    "&actualMPT=Certificados%20&linkMPT=%2Fsri-catastro-tributario-web-internet"
    "%2Fpages%2Fcertificado%2Fopciones-certificado.jsf%3F&esFavorito=S"
)
RUC_SELECTOR = 'img[src*="RUC.svg"], img[src*="ruc.svg"], a[href*="RUC"], a[href*="ruc"]'


class SriCertificadoRucService:
    _running = False
    _thread: threading.Thread | None = None
    _single_status: dict[str, dict[str, Any]] = {}
    _single_lock = threading.Lock()
    _status: dict[str, Any] = {
        "estado": "detenido",
        "total": 0,
        "procesados": 0,
        "exitos": 0,
        "errores": 0,
        "ruc_actual": "",
        "ultimo_error": "",
    }

    @classmethod
    def _clientes_activos(cls) -> list[dict[str, str]]:
        with engine.connect() as db:
            rows = db.execute(text("""
                SELECT ruccedcli, nomclient
                FROM clientes
                WHERE activo = TRUE
                  AND LENGTH(TRIM(ruccedcli::text)) = 13
                ORDER BY ruccedcli
            """)).mappings().all()
        return [
            {
                "ruc": str(row["ruccedcli"]).strip(),
                "nombre": str(row["nomclient"] or "").strip(),
            }
            for row in rows
            if str(row["ruccedcli"] or "").strip()
        ]

    @classmethod
    def iniciar_uno(cls, ruc: str) -> dict[str, Any]:
        """Inicia la obtención de un certificado para un único RUC."""
        ruc = re.sub(r"\D", "", str(ruc))
        if len(ruc) != 13:
            raise ValueError("El RUC debe contener exactamente 13 dígitos.")

        with cls._single_lock:
            actual = cls._single_status.get(ruc, {})
            if actual.get("estado") in {"iniciando", "ejecutando"}:
                return {
                    "ruc": ruc,
                    "estado": actual["estado"],
                    "mensaje": "Ya existe una solicitud activa para este RUC.",
                }
            cls._single_status[ruc] = {
                "ruc": ruc,
                "estado": "iniciando",
                "mensaje": "Conta recibió la solicitud y está preparando el navegador.",
                "error": "",
            }

        thread = threading.Thread(
            target=cls._procesar_uno,
            args=(ruc,),
            name=f"Conta-Certificado-RUC-{ruc}",
            daemon=True,
        )
        thread.start()
        logger.info("CERTIFICADO RUC INDIVIDUAL | solicitud aceptada | ruc=%s", ruc)
        return {
            "ruc": ruc,
            "estado": "iniciando",
            "mensaje": "Solicitud recibida por Conta. Se consultará el estado hasta confirmar el PDF.",
        }

    @classmethod
    def estado_uno(cls, ruc: str) -> dict[str, Any]:
        ruc = re.sub(r"\D", "", str(ruc))
        with cls._single_lock:
            estado = cls._single_status.get(ruc)
            return dict(estado) if estado else {
                "ruc": ruc,
                "estado": "no_iniciado",
                "mensaje": "No hay una solicitud registrada para este RUC.",
                "error": "",
            }

    @classmethod
    def _procesar_uno(cls, ruc: str) -> None:
        with cls._single_lock:
            cls._single_status[ruc].update({
                "estado": "ejecutando",
                "mensaje": "Conectando al SRI y solicitando el certificado PDF.",
            })
        try:
            resultado = asyncio.run(cls.descargar_cliente(ruc))
            with cls._single_lock:
                cls._single_status[ruc].update({
                    "estado": "completado",
                    "mensaje": "Certificado de RUC obtenido y guardado correctamente.",
                    "resultado": resultado,
                    "error": "",
                })
            logger.info("CERTIFICADO RUC INDIVIDUAL OK | ruc=%s | resultado=%s", ruc, resultado)
        except Exception as exc:
            mensaje = f"{type(exc).__name__}: {exc}"
            with cls._single_lock:
                cls._single_status[ruc].update({
                    "estado": "error",
                    "mensaje": "No se pudo obtener o guardar el Certificado de RUC.",
                    "error": mensaje,
                })
            logger.exception("CERTIFICADO RUC INDIVIDUAL ERROR | ruc=%s", ruc)

    @classmethod
    def iniciar_todos(cls) -> dict[str, Any]:
        if cls._running:
            return {
                "estado": "ejecutando",
                "mensaje": "Ya existe una descarga masiva de Certificados de RUC en ejecución.",
            }

        # No consultar la base de datos dentro de la petición HTTP.
        # La solicitud debe responder inmediatamente; toda la carga queda
        # en el hilo de trabajo, incluyendo la consulta de clientes activos.
        cls._running = True
        cls._status = {
            "estado": "iniciando",
            "total": 0,
            "procesados": 0,
            "exitos": 0,
            "errores": 0,
            "ruc_actual": "",
            "ultimo_error": "",
        }
        cls._thread = threading.Thread(
            target=cls._iniciar_en_hilo,
            name="Conta-Certificados-RUC",
            daemon=True,
        )
        cls._thread.start()
        logger.info("CERTIFICADOS RUC | solicitud aceptada | inicializando proceso")
        return {
            "estado": "iniciado",
            "total": 0,
            "mensaje": "La descarga masiva de Certificados de RUC fue iniciada.",
        }

    @classmethod
    def estado(cls) -> dict[str, Any]:
        return dict(cls._status)

    @classmethod
    def _iniciar_en_hilo(cls) -> None:
        """Carga los clientes y arranca el procesamiento fuera de la petición HTTP."""
        try:
            logger.info("CERTIFICADOS RUC | consultando clientes activos")
            clientes = cls._clientes_activos()
            if not clientes:
                cls._status.update({
                    "estado": "finalizado",
                    "total": 0,
                    "procesados": 0,
                    "exitos": 0,
                    "errores": 0,
                    "ruc_actual": "",
                    "ultimo_error": "",
                })
                logger.info("CERTIFICADOS RUC | no existen clientes activos")
                return

            cls._status.update({
                "estado": "ejecutando",
                "total": len(clientes),
            })
            logger.info(
                "CERTIFICADOS RUC | clientes activos cargados | total=%s",
                len(clientes),
            )
            asyncio.run(cls._procesar_todos(clientes))
        except Exception as exc:
            cls._status.update({
                "estado": "error",
                "ultimo_error": f"{type(exc).__name__}: {exc}",
            })
            logger.exception("CERTIFICADOS RUC | error fatal al iniciar proceso")
        finally:
            cls._running = False if cls._status.get("estado") != "ejecutando" else cls._running
            if cls._status.get("estado") in {"iniciando", "error", "finalizado"}:
                cls._thread = None

    @classmethod
    def _ejecutar_en_hilo(cls, clientes: list[dict[str, str]]) -> None:
        """Ejecuta el proceso fuera del event loop de la petición HTTP."""
        try:
            asyncio.run(cls._procesar_todos(clientes))
        except Exception:
            logger.exception("CERTIFICADOS RUC | error fatal del proceso masivo")
            cls._running = False
            cls._thread = None

    @classmethod
    async def _procesar_todos(cls, clientes: list[dict[str, str]]) -> None:
        exitos = 0
        errores = 0
        try:
            for cliente in clientes:
                ruc = cliente["ruc"]
                cls._status["ruc_actual"] = ruc
                logger.info("CERTIFICADO RUC | iniciando | ruc=%s | cliente=%s", ruc, cliente["nombre"])
                try:
                    await cls.descargar_cliente(ruc)
                    exitos += 1
                    cls._status["exitos"] = exitos
                    cls._status["procesados"] = exitos + errores
                    logger.info(
                        "CERTIFICADO RUC OK | ruc=%s | cliente=%s",
                        ruc,
                        cliente["nombre"],
                    )
                except Exception as exc:
                    errores += 1
                    cls._status["errores"] = errores
                    cls._status["procesados"] = exitos + errores
                    cls._status["ultimo_error"] = f"{ruc}: {type(exc).__name__}: {exc}"
                    logger.exception(
                        "CERTIFICADO RUC ERROR | ruc=%s | cliente=%s",
                        ruc,
                        cliente["nombre"],
                    )
            logger.info(
                "CERTIFICADOS RUC FINALIZADOS | total=%s | exitos=%s | errores=%s",
                len(clientes), exitos, errores,
            )
            cls._status.update({
                "estado": "finalizado",
                "procesados": len(clientes),
                "exitos": exitos,
                "errores": errores,
                "ruc_actual": "",
            })
        finally:
            cls._running = False
            cls._thread = None
            if cls._status.get("estado") == "ejecutando":
                cls._status["estado"] = "finalizado"

    @classmethod
    async def descargar_cliente(cls, ruc: str) -> dict[str, Any]:
        logger.info("CERTIFICADO RUC | credenciales | ruc=%s", ruc)
        cred = SriClienteSyncService._credenciales(ruc)

        p = browser = context = page = chrome_process = None
        temp_path: Path | None = None
        download_dir: Path | None = None

        try:
            logger.info("CERTIFICADO RUC | login SRI | ruc=%s", ruc)
            p, browser, context, page, chrome_process = await SriClienteSyncService._login(ruc, cred["clave"])
            logger.info("CERTIFICADO RUC | login OK | ruc=%s | url=%s", ruc, page.url)

            logger.info("CERTIFICADO RUC | abriendo certificados | ruc=%s", ruc)

            async def _abrir_certificados() -> None:
                await page.goto(
                    CERTIFICADO_URL,
                    wait_until="domcontentloaded",
                    timeout=settings.SRI_NAVIGATION_TIMEOUT_MS,
                )
                logger.info(
                    "CERTIFICADO RUC | página certificados | ruc=%s | url=%s | title=%s",
                    ruc,
                    page.url,
                    await page.title(),
                )

            await _abrir_certificados()

            logger.info("CERTIFICADO RUC | buscando botón RUC | ruc=%s", ruc)

            async def _buscar_boton_ruc():
                # SRI cambia ocasionalmente el marcado JSF; buscar también en iframes.
                selectores = [
                    RUC_SELECTOR,
                    'a[href*="RUC" i]',
                    'a[href*="ruc" i]',
                    'button:has-text("RUC")',
                    '[role="button"]:has-text("RUC")',
                    'input[value*="RUC" i]',
                    'img[alt*="RUC" i]',
                    'img[title*="RUC" i]',
                    '[aria-label*="RUC" i]',
                    '[title*="RUC" i]',
                ]
                objetivos = [page] + [frame for frame in page.frames if frame != page.main_frame]
                for objetivo in objetivos:
                    for selector in selectores:
                        try:
                            locator = objetivo.locator(selector).first
                            if await locator.count() and await locator.is_visible():
                                logger.info(
                                    "CERTIFICADO RUC | opción encontrada por selector | ruc=%s | frame=%s | selector=%s",
                                    ruc, getattr(objetivo, "url", page.url), selector,
                                )
                                return locator
                        except Exception:
                            continue
                    for patron in (
                        re.compile(r"^\s*RUC\s*$", re.IGNORECASE),
                        re.compile(r"^\s*Certificado\s+de\s+RUC\s*$", re.IGNORECASE),
                        re.compile(r"^\s*RUC\s*[:\-]?\s*$", re.IGNORECASE),
                    ):
                        try:
                            locator = objetivo.get_by_text(patron).first
                            if await locator.count() and await locator.is_visible():
                                logger.info(
                                    "CERTIFICADO RUC | opción encontrada por texto | ruc=%s | frame=%s",
                                    ruc, getattr(objetivo, "url", page.url),
                                )
                                return locator
                        except Exception:
                            continue
                return None
            ruc_button = await _buscar_boton_ruc()
            if ruc_button is None:
                # El SRI puede devolver una página intermedia después del login.
                # Reabrimos la página de certificados una sola vez.
                logger.warning(
                    "CERTIFICADO RUC | botón RUC no encontrado | ruc=%s | url=%s",
                    ruc,
                    page.url,
                )
                await _abrir_certificados()
                ruc_button = await _buscar_boton_ruc()

            if ruc_button is None:
                diagnostico = []
                for objetivo in [page] + [frame for frame in page.frames if frame != page.main_frame]:
                    try:
                        texto = " ".join((await objetivo.locator("body").inner_text(timeout=1500)).split())
                        diagnostico.append(
                            f"frame_url={getattr(objetivo, 'url', page.url)!r}, texto={texto[:700]!r}"
                        )
                    except Exception as diag_exc:
                        diagnostico.append(
                            f"frame_url={getattr(objetivo, 'url', page.url)!r}, lectura={type(diag_exc).__name__}"
                        )
                raise RuntimeError(
                    "El SRI abrió la página de certificados, pero no apareció la opción RUC. "
                    f"URL={page.url!r}; título={await page.title()!r}; "
                    + " | ".join(diagnostico)
                )

            logger.info("CERTIFICADO RUC | haciendo clic RUC | ruc=%s", ruc)

            # Diagnosticar la respuesta del SRI sin registrar cuerpos ni datos de sesión.
            # En Chrome conectado por CDP, habilitar descargas explícitamente para esta pestaña.
            respuestas_sri: list[dict[str, Any]] = []

            async def _registrar_respuesta_sri(resp) -> None:
                try:
                    if "srienlinea.sri.gob.ec" not in resp.url:
                        return
                    headers = resp.headers
                    detalle = {
                        "status": resp.status,
                        "method": resp.request.method,
                        "url": resp.url[:240],
                        "content_type": headers.get("content-type", ""),
                        "content_disposition": headers.get("content-disposition", ""),
                    }
                    respuestas_sri.append(detalle)
                    if len(respuestas_sri) > 20:
                        del respuestas_sri[:-20]
                    logger.info(
                        "CERTIFICADO RUC | respuesta SRI | ruc=%s | status=%s | method=%s | "
                        "content_type=%s | content_disposition=%s | url=%s",
                        ruc, detalle["status"], detalle["method"],
                        detalle["content_type"], detalle["content_disposition"], detalle["url"],
                    )
                except Exception:
                    logger.debug("No se pudo registrar una respuesta de red del SRI", exc_info=True)

            # Chrome conectado por CDP guarda el archivo descargado en disco.
            # Usamos un directorio exclusivo por RUC para leer el archivo real
            # de Chromium sin mezclarlo con descargas previas ni otros procesos.
            download_dir = Path(tempfile.mkdtemp(prefix=f"conta_ruc_{ruc}_"))
            cdp_session = None
            page.on("response", _registrar_respuesta_sri)
            try:
                try:
                    cdp_session = await context.new_cdp_session(page)
                    await cdp_session.send(
                        "Page.setDownloadBehavior",
                        {
                            "behavior": "allow",
                            "downloadPath": str(download_dir),
                        },
                    )
                    logger.info(
                        "CERTIFICADO RUC | descargas habilitadas por CDP | ruc=%s",
                        ruc,
                    )
                except Exception as cdp_exc:
                    logger.warning(
                        "CERTIFICADO RUC | no se pudo configurar descarga por CDP | ruc=%s | error=%s: %s",
                        ruc, type(cdp_exc).__name__, cdp_exc,
                    )
                    if cdp_session is not None:
                        try:
                            await cdp_session.detach()
                        except Exception:
                            pass
                        cdp_session = None

                async with page.expect_download(timeout=60000) as descarga_pendiente:
                    await ruc_button.click()
                descarga = await descarga_pendiente.value
            except Exception as exc:
                page.remove_listener("response", _registrar_respuesta_sri)
                if cdp_session is not None:
                    try:
                        await cdp_session.detach()
                    except Exception:
                        pass
                resumen_respuestas = respuestas_sri[-10:]
                raise RuntimeError(
                    "El SRI no inició una descarga de certificado en 60 segundos. "
                    f"URL actual={page.url!r}; título={await page.title()!r}; "
                    f"últimas respuestas HTTP del SRI={resumen_respuestas!r}. "
                    "No se realizó otra consulta automáticamente para evitar consumir intentos del SRI."
                ) from exc
            else:
                page.remove_listener("response", _registrar_respuesta_sri)
                if cdp_session is not None:
                    try:
                        await cdp_session.detach()
                    except Exception:
                        logger.debug("No se pudo cerrar la sesión CDP de diagnóstico", exc_info=True)

            nombre_descarga = Path(
                descarga.suggested_filename or f"certificado_ruc_{ruc}.pdf"
            ).name
            fallo_descarga = await descarga.failure()
            logger.info(
                "CERTIFICADO RUC | descarga iniciada por Chromium | ruc=%s | nombre=%s | failure=%s",
                ruc, nombre_descarga, fallo_descarga,
            )
            if fallo_descarga:
                raise RuntimeError(
                    f"Chromium informó un fallo al descargar el certificado: {fallo_descarga}"
                )

            contenido = b""
            origen_contenido = ""
            # Con connect_over_cdp, Playwright puede recibir el evento de descarga
            # pero save_as() no siempre recupera el cuerpo. Primero leemos el
            # archivo que Chrome guardó en el downloadPath de CDP.
            if download_dir is not None and download_dir.is_dir():
                limite = asyncio.get_running_loop().time() + 10
                while asyncio.get_running_loop().time() < limite:
                    try:
                        ruta_esperada = download_dir / nombre_descarga
                        if ruta_esperada.is_file():
                            candidato = ruta_esperada
                        else:
                            candidatos = [
                                ruta for ruta in download_dir.iterdir()
                                if ruta.is_file()
                                and not ruta.name.lower().endswith(".crdownload")
                            ]
                            candidato = max(
                                candidatos,
                                key=lambda ruta: ruta.stat().st_mtime,
                                default=None,
                            )
                        if candidato is not None:
                            datos_chrome = await asyncio.to_thread(candidato.read_bytes)
                            if datos_chrome:
                                contenido = datos_chrome
                                origen_contenido = f"archivo Chromium: {candidato.name}"
                                if contenido.startswith(b"%PDF"):
                                    break
                    except OSError:
                        logger.debug(
                            "Esperando a que Chromium termine de escribir el PDF | ruc=%s",
                            ruc,
                            exc_info=True,
                        )
                    await asyncio.sleep(0.25)

            # Respaldo: pedir a Playwright que guarde su copia administrada.
            # Se usa también si el archivo directo de Chrome está vacío o no es PDF.
            if not contenido.startswith(b"%PDF"):
                with tempfile.NamedTemporaryFile(
                    prefix=f"ruc_{ruc}_",
                    suffix=".pdf",
                    delete=False,
                ) as tmp:
                    temp_path = Path(tmp.name)
                try:
                    await descarga.save_as(str(temp_path))
                    contenido_playwright = await asyncio.to_thread(temp_path.read_bytes)
                    if contenido_playwright.startswith(b"%PDF") or len(contenido_playwright) > len(contenido):
                        contenido = contenido_playwright
                        origen_contenido = "copia administrada por Playwright"
                except Exception as exc:
                    logger.warning(
                        "CERTIFICADO RUC | respaldo save_as falló | ruc=%s | error=%s: %s",
                        ruc, type(exc).__name__, exc,
                    )

            logger.info(
                "CERTIFICADO RUC | descarga leída | ruc=%s | bytes=%s | origen=%s",
                ruc, len(contenido), origen_contenido or "sin archivo",
            )

            if not contenido.startswith(b"%PDF"):
                preview = contenido[:200].decode("utf-8", errors="replace")
                cabecera_hex = contenido[:24].hex()
                raise ValueError(
                    f"El SRI no devolvió un PDF válido para el RUC {ruc}. "
                    f"bytes={len(contenido)}; origen={origen_contenido or 'desconocido'}; "
                    f"cabecera_hex={cabecera_hex!r}; preview={preview!r}; "
                    f"últimas respuestas HTTP del SRI={respuestas_sri[-5:]!r}"
                )

            logger.info(
                "CERTIFICADO RUC | PDF recibido | ruc=%s | bytes=%s",
                ruc,
                len(contenido),
            )

            logger.info("CERTIFICADO RUC | subiendo a TtCWeb | ruc=%s | bytes=%s", ruc, len(contenido))
            respuesta = cls._subir_a_ttcweb(
                ruc=ruc,
                nombre=cred["nombre"],
                contenido=contenido,
            )

            logger.info("CERTIFICADO RUC | guardado en TtCWeb | ruc=%s | respuesta=%s", ruc, respuesta)
            return {
                "ruc": ruc,
                "nombre": cred["nombre"],
                "tamano": len(contenido),
                **respuesta,
            }

        finally:
            if temp_path:
                try:
                    temp_path.unlink(missing_ok=True)
                except Exception:
                    pass

            try:
                SriClienteSyncService._cerrar_chrome(chrome_process)
                if p is not None:
                    await p.stop()
            except Exception:
                logger.exception(
                    "No se pudo cerrar correctamente Chrome | ruc=%s",
                    ruc,
                )

            # Elimina el directorio de descargas después de cerrar Chrome para
            # no competir con el proceso si el SRI dejó una descarga pendiente.
            if download_dir is not None:
                try:
                    await asyncio.to_thread(shutil.rmtree, download_dir, True)
                except Exception:
                    logger.debug(
                        "No se pudo limpiar el directorio temporal de descarga | ruc=%s",
                        ruc,
                        exc_info=True,
                    )

    @classmethod
    def _subir_a_ttcweb(
        cls,
        ruc: str,
        nombre: str,
        contenido: bytes,
    ) -> dict[str, Any]:
        token = str(settings.TOTALCOUNTS_INTERNAL_TOKEN or "").strip()
        if not token:
            raise RuntimeError(
                "TOTALCOUNTS_INTERNAL_TOKEN no está configurado en Conta."
            )

        # Para comunicación servidor-a-servidor usamos la URL interna.
        # Evita Cloudflare, que protege la URL pública de totalcounts.com.ec.
        base_url = getattr(
            settings,
            "TOTALCOUNTS_INTERNAL_URL",
            "http://127.0.0.1:8001",
        ).strip().rstrip("/")
        if not base_url:
            raise RuntimeError(
                "TOTALCOUNTS_URL no está configurado en Conta."
            )

        boundary = f"----ContaRuc{uuid.uuid4().hex}"
        sha256 = hashlib.sha256(contenido).hexdigest()

        def campo(nombre_campo: str, valor: str) -> bytes:
            return (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{nombre_campo}"\r\n\r\n'
                f"{valor}\r\n"
            ).encode("utf-8")

        cuerpo = bytearray()
        cuerpo.extend(campo("ruc", ruc))
        cuerpo.extend(campo("nombre", nombre))
        cuerpo.extend(
            (
                f"--{boundary}\r\n"
                'Content-Disposition: form-data; name="certificado_ruc"; '
                'filename="certificado_ruc.pdf"\r\n'
                "Content-Type: application/pdf\r\n\r\n"
            ).encode("utf-8")
        )
        cuerpo.extend(contenido)
        cuerpo.extend(b"\r\n")
        cuerpo.extend(f"--{boundary}--\r\n".encode("utf-8"))

        req = urllib.request.Request(
            f"{base_url}/internal/conta/certificado-ruc/",
            data=bytes(cuerpo),
            headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Accept": "application/json",
                "X-TotalCounts-Internal": token,
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                import json

                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"TtCWeb rechazó el Certificado de RUC ({exc.code}): {body}"
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(
                f"No se pudo conectar con TtCWeb: {exc.reason}"
            ) from exc
