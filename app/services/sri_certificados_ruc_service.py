from __future__ import annotations

import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Any

import requests
from sqlalchemy import text

from app.core.config import settings
from app.database.connection import engine
from app.services.sri_cliente_sync_service import SriClienteSyncService


logger = logging.getLogger("conta.sri_certificados_ruc")


class SriCertificadosRucService:
    CERTIFICADO_URL = (
        "https://srienlinea.sri.gob.ec/"
        "sri-catastro-tributario-web-internet/pages/certificado/"
        "opciones-certificado.jsf?&contextoMPT="
        "https://srienlinea.sri.gob.ec/tuportal-internet&pathMPT=RUC"
        "&actualMPT=Certificados%20&linkMPT=%2Fsri-catastro-tributario-web-internet"
        "%2Fpages%2Fcertificado%2Fopciones-certificado.jsf%3F&esFavorito=S"
    )
    RUC_SELECTOR = 'img[src*="RUC.svg"]'
    TOTALCOUNTS_UPLOAD_PATH = "/internal/conta/certificado-ruc/"

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
    def _subir_a_ttcweb(cls, ruc: str, nombre: str, pdf: bytes) -> dict[str, Any]:
        base_url = str(settings.TOTALCOUNTS_URL or "").strip().rstrip("/")
        token = str(settings.TOTALCOUNTS_INTERNAL_TOKEN or "").strip()

        if not base_url:
            raise RuntimeError("TOTALCOUNTS_URL no está configurado en Conta.")
        if not token:
            raise RuntimeError("TOTALCOUNTS_INTERNAL_TOKEN no está configurado en Conta.")

        url = f"{base_url}{cls.TOTALCOUNTS_UPLOAD_PATH}"
        response = requests.post(
            url,
            headers={"X-TotalCounts-Internal": token},
            data={"ruc": ruc, "nombre": nombre},
            files={
                "certificado_ruc": (
                    "certificado_ruc.pdf",
                    pdf,
                    "application/pdf",
                )
            },
            timeout=120,
        )

        if not response.ok:
            raise RuntimeError(
                f"TtCWeb respondió HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )

        try:
            payload = response.json()
        except ValueError:
            payload = {}

        if not payload.get("ok"):
            raise RuntimeError(
                str(payload.get("error") or "TtCWeb no confirmó el guardado del certificado.")
            )

        return payload

    @classmethod
    async def descargar_cliente(cls, ruc: str, nombre: str = "") -> dict[str, Any]:
        cred = SriClienteSyncService._credenciales(ruc)
        p = browser = context = page = chrome_process = None

        try:
            p, browser, context, page, chrome_process = await SriClienteSyncService._login(
                ruc,
                cred["clave"],
            )

            await page.goto(
                cls.CERTIFICADO_URL,
                wait_until="domcontentloaded",
                timeout=settings.SRI_NAVIGATION_TIMEOUT_MS,
            )

            boton = page.locator(cls.RUC_SELECTOR).first
            await boton.wait_for(state="visible", timeout=30000)

            async with page.expect_download(timeout=60000) as download_info:
                await boton.click(force=True)

            download = await download_info.value

            if download.suggested_filename and not download.suggested_filename.lower().endswith(".pdf"):
                raise RuntimeError(
                    f"El SRI devolvió un archivo inesperado: {download.suggested_filename}"
                )

            with tempfile.TemporaryDirectory(prefix=f"ruc_{ruc}_") as temp_dir:
                destino = Path(temp_dir) / "certificado_ruc.pdf"
                await download.save_as(str(destino))
                pdf = destino.read_bytes()

            if not pdf.startswith(b"%PDF"):
                raise RuntimeError("El archivo descargado por SRI no parece ser un PDF válido.")

            resultado = await asyncio.to_thread(
                cls._subir_a_ttcweb,
                ruc,
                nombre or cred["nombre"],
                pdf,
            )

            return {
                "ok": True,
                "ruc": ruc,
                "nombre": nombre or cred["nombre"],
                "tamano": len(pdf),
                "ruta_relativa": resultado.get(
                    "ruta_relativa",
                    f"{ruc}/documentos/ruc/certificado_ruc.pdf",
                ),
            }

        finally:
            if browser is not None:
                try:
                    await browser.close()
                except Exception:
                    pass
            elif context is not None:
                try:
                    await context.close()
                except Exception:
                    pass

            SriClienteSyncService._cerrar_chrome(chrome_process)

            if p is not None:
                try:
                    await p.stop()
                except Exception:
                    pass

    @classmethod
    async def descargar_todos_activos(cls) -> dict[str, Any]:
        clientes = cls._clientes_activos()
        resultados: list[dict[str, Any]] = []
        exitosos = 0
        errores = 0

        for cliente in clientes:
            ruc = cliente["ruc"]
            try:
                resultado = await cls.descargar_cliente(ruc, cliente["nombre"])
                resultados.append(resultado)
                exitosos += 1
                logger.info(
                    "CERTIFICADO RUC OK | ruc=%s | tamano=%s",
                    ruc,
                    resultado["tamano"],
                )
            except Exception as exc:
                errores += 1
                error = {
                    "ok": False,
                    "ruc": ruc,
                    "nombre": cliente["nombre"],
                    "error": str(exc),
                }
                resultados.append(error)
                logger.exception("CERTIFICADO RUC ERROR | ruc=%s", ruc)

        return {
            "ok": errores == 0,
            "total": len(clientes),
            "exitosos": exitosos,
            "errores": errores,
            "resultados": resultados,
        }
