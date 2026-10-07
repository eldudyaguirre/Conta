from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
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
RUC_SELECTOR = 'img[src*="RUC.svg"]'


class SriCertificadoRucService:
    _running = False
    _thread: threading.Thread | None = None
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

        try:
            logger.info("CERTIFICADO RUC | login SRI | ruc=%s", ruc)
            p, browser, context, page, chrome_process = await SriClienteSyncService._login(ruc, cred["clave"])
            logger.info("CERTIFICADO RUC | login OK | ruc=%s | url=%s", ruc, page.url)

            logger.info("CERTIFICADO RUC | abriendo certificados | ruc=%s", ruc)
            await page.goto(
                CERTIFICADO_URL,
                wait_until="domcontentloaded",
                timeout=settings.SRI_NAVIGATION_TIMEOUT_MS,
            )

            logger.info("CERTIFICADO RUC | buscando botón RUC | ruc=%s", ruc)
            ruc_button = page.locator(RUC_SELECTOR).first
            await ruc_button.wait_for(
                state="visible",
                timeout=settings.SRI_NAVIGATION_TIMEOUT_MS,
            )

            logger.info("CERTIFICADO RUC | haciendo clic RUC | ruc=%s", ruc)
            async with page.expect_download(timeout=60000) as download_info:
                await ruc_button.click()

            download = await download_info.value
            logger.info("CERTIFICADO RUC | descarga recibida | ruc=%s | archivo=%s", ruc, download.suggested_filename)

            with tempfile.NamedTemporaryFile(
                prefix=f"ruc_{ruc}_",
                suffix=".pdf",
                delete=False,
            ) as tmp:
                temp_path = Path(tmp.name)

            await download.save_as(str(temp_path))

            contenido = temp_path.read_bytes()
            if not contenido.startswith(b"%PDF"):
                raise ValueError(
                    f"El SRI no devolvió un PDF válido para el RUC {ruc}."
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

        base_url = getattr(settings, "TOTALCOUNTS_URL", "").strip().rstrip("/")
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
