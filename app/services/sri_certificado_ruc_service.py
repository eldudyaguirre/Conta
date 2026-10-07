from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
import tempfile
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

        clientes = cls._clientes_activos()
        if not clientes:
            return {
                "estado": "finalizado",
                "total": 0,
                "mensaje": "No existen clientes activos.",
            }

        cls._running = True
        asyncio.create_task(cls._procesar_todos(clientes))
        return {
            "estado": "iniciado",
            "total": len(clientes),
            "mensaje": "La descarga masiva de Certificados de RUC fue iniciada.",
        }

    @classmethod
    async def _procesar_todos(cls, clientes: list[dict[str, str]]) -> None:
        exitos = 0
        errores = 0
        try:
            for cliente in clientes:
                ruc = cliente["ruc"]
                try:
                    await cls.descargar_cliente(ruc)
                    exitos += 1
                    logger.info(
                        "CERTIFICADO RUC OK | ruc=%s | cliente=%s",
                        ruc,
                        cliente["nombre"],
                    )
                except Exception:
                    errores += 1
                    logger.exception(
                        "CERTIFICADO RUC ERROR | ruc=%s | cliente=%s",
                        ruc,
                        cliente["nombre"],
                    )
            logger.info(
                "CERTIFICADOS RUC FINALIZADOS | total=%s | exitos=%s | errores=%s",
                len(clientes),
                exitos,
                errores,
            )
        finally:
            cls._running = False

    @classmethod
    async def descargar_cliente(cls, ruc: str) -> dict[str, Any]:
        cred = SriClienteSyncService._credenciales(ruc)

        p = browser = context = page = chrome_process = None
        temp_path: Path | None = None

        try:
            p, browser, context, page, chrome_process = (
                await SriClienteSyncService._login(ruc, cred["clave"])
            )

            await page.goto(
                CERTIFICADO_URL,
                wait_until="domcontentloaded",
                timeout=settings.SRI_NAVIGATION_TIMEOUT_MS,
            )

            ruc_button = page.locator(RUC_SELECTOR).first
            await ruc_button.wait_for(
                state="visible",
                timeout=settings.SRI_NAVIGATION_TIMEOUT_MS,
            )

            async with page.expect_download(timeout=60000) as download_info:
                await ruc_button.click()

            download = await download_info.value

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

            respuesta = cls._subir_a_ttcweb(
                ruc=ruc,
                nombre=cred["nombre"],
                contenido=contenido,
            )

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
