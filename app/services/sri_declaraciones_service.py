from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
import tempfile
import threading
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.services.sri_cliente_sync_service import SriClienteSyncService


logger = logging.getLogger("conta.sri_declaraciones")

SRI_DECLARACIONES_PAGE = (
    "https://srienlinea.sri.gob.ec/"
    "sri-eyr-consulta-web-internet/pages/consulta/obligacion/lista-obligaciones.jsf"
)
SRI_DECLARACIONES_URL = SRI_DECLARACIONES_PAGE + "?" + urllib.parse.urlencode({
    "contextoMPT": "https://srienlinea.sri.gob.ec/tuportal-internet",
    "pathMPT": "Declaraciones / Declaración de impuestos",
    "actualMPT": "Consulta de declaraciones y comprobantes de pago ",
    "linkMPT": "/sri-eyr-consulta-web-internet/pages/consulta/obligacion/lista-obligaciones.jsf?",
    "esFavorito": "S",
})
TIMEOUT_MS = 60_000
OBLIGACIONES = {
    "iva": {"data_rk": "1", "titulo": "IVA"},
    "renta_natural": {"data_rk": "14", "titulo": "RENTA NATURALES"},
    "renta_sociedades": {"data_rk": "101", "titulo": "RENTA SOCIEDADES"},
}
MESES = {
    "enero": "01",
    "febrero": "02",
    "marzo": "03",
    "abril": "04",
    "mayo": "05",
    "junio": "06",
    "julio": "07",
    "agosto": "08",
    "septiembre": "09",
    "setiembre": "09",
    "octubre": "10",
    "noviembre": "11",
    "diciembre": "12",
}


def _sin_acentos(valor: str) -> str:
    return "".join(
        caracter for caracter in unicodedata.normalize("NFKD", str(valor or ""))
        if not unicodedata.combining(caracter)
    ).lower().strip()


def _obligacion_renta_por_tipo_contribuyente(tipo_contribuyente: str) -> str:
    """Mapea el tipo BDTotal a la obligación real de Renta que ofrece el SRI."""
    tipo = _sin_acentos(tipo_contribuyente)
    if "juridic" in tipo or "sociedad" in tipo:
        return "renta_sociedades"
    if "natural" in tipo:
        return "renta_natural"
    raise ValueError(
        "No se pudo determinar si el cliente es persona natural o jurídica "
        f"a partir de tipo_contribuyente={tipo_contribuyente!r} en BdTotal.clientes."
    )


class SriDeclaracionesService:
    """Sincroniza declaraciones ya presentadas en SRI; nunca presenta formularios."""

    _lock = threading.Lock()
    _trabajos: dict[str, dict[str, Any]] = {}

    @classmethod
    def _clave_trabajo(cls, ruc: str, anio: int, tipo: str = "ambos") -> str:
        return f"{ruc}:{anio}:{tipo}"

    @classmethod
    def iniciar(cls, ruc: str, anio: int, tipo: str = "ambos") -> dict[str, Any]:
        ruc = re.sub(r"\D", "", str(ruc))
        anio = int(anio)
        tipo = str(tipo or "ambos").strip().lower()
        if len(ruc) != 13:
            raise ValueError("El RUC debe contener exactamente 13 dígitos.")
        if anio < 2011 or anio > 2100:
            raise ValueError("El año fiscal debe estar entre 2011 y 2100.")
        if tipo not in {"iva", "renta", "ambos"}:
            raise ValueError("Tipo de declaración inválido; use iva o renta.")

        key = cls._clave_trabajo(ruc, anio, tipo)
        renta_anio = anio if tipo == "renta" else anio - 1
        if tipo == "iva":
            mensaje = f"Preparando consulta de declaraciones IVA del año fiscal {anio}."
        elif tipo == "renta":
            mensaje = f"Preparando consulta de declaraciones de Renta Naturales del ejercicio {anio}."
        else:
            mensaje = f"Preparando consulta: IVA {anio} y Renta del ejercicio {anio - 1}."

        with cls._lock:
            actual = cls._trabajos.get(key, {})
            if actual.get("estado") in {"iniciando", "ejecutando"}:
                return {
                    **actual,
                    "mensaje": "Ya existe una sincronización activa para este RUC, año y tipo.",
                }
            cls._trabajos[key] = {
                "ruc": ruc,
                "anio": anio,
                "tipo": tipo,
                "iva_anio": anio if tipo in {"iva", "ambos"} else None,
                "renta_anio": renta_anio if tipo in {"renta", "ambos"} else None,
                "estado": "iniciando",
                "mensaje": mensaje,
                "encontradas": 0,
                "ya_existian": 0,
                "descargadas": 0,
                "errores": 0,
                "total": 0,
                "detalle_errores": [],
            }

        worker = threading.Thread(
            target=cls._trabajo,
            args=(ruc, anio, tipo),
            name=f"Conta-Declaraciones-SRI-{tipo}-{ruc}-{anio}",
            daemon=True,
        )
        worker.start()
        logger.info(
            "SINCRONIZACIÓN DECLARACIONES | solicitud aceptada | ruc=%s | tipo=%s | año=%s",
            ruc, tipo, anio,
        )
        return cls.estado(ruc, anio, tipo)

    @classmethod
    def estado(cls, ruc: str, anio: int, tipo: str = "ambos") -> dict[str, Any]:
        ruc = re.sub(r"\D", "", str(ruc))
        tipo = str(tipo or "ambos").strip().lower()
        key = cls._clave_trabajo(ruc, int(anio), tipo)
        renta_anio = int(anio) if tipo == "renta" else int(anio) - 1
        with cls._lock:
            actual = cls._trabajos.get(key)
            if actual:
                return dict(actual)
        return {
            "ruc": ruc,
            "anio": int(anio),
            "tipo": tipo,
            "iva_anio": int(anio) if tipo in {"iva", "ambos"} else None,
            "renta_anio": renta_anio if tipo in {"renta", "ambos"} else None,
            "estado": "no_iniciado",
            "mensaje": f"No se ha iniciado sincronización de {tipo} para este RUC y año.",
            "encontradas": 0,
            "ya_existian": 0,
            "descargadas": 0,
            "errores": 0,
            "total": 0,
            "detalle_errores": [],
        }

    @classmethod
    def _actualizar(cls, ruc: str, anio: int, tipo: str = "ambos", **campos: Any) -> None:
        key = cls._clave_trabajo(ruc, anio, tipo)
        with cls._lock:
            if key in cls._trabajos:
                cls._trabajos[key].update(campos)

    @classmethod
    def _incrementar(cls, ruc: str, anio: int, campo: str, cantidad: int = 1, tipo: str = "ambos") -> None:
        key = cls._clave_trabajo(ruc, anio, tipo)
        with cls._lock:
            if key in cls._trabajos:
                cls._trabajos[key][campo] = int(cls._trabajos[key].get(campo, 0)) + cantidad

    @classmethod
    def _agregar_error(cls, ruc: str, anio: int, tipo: str, mensaje: str) -> None:
        key = cls._clave_trabajo(ruc, anio, tipo)
        with cls._lock:
            if key in cls._trabajos:
                cls._trabajos[key]["errores"] = int(cls._trabajos[key].get("errores", 0)) + 1
                errores = cls._trabajos[key].setdefault("detalle_errores", [])
                if len(errores) < 30:
                    errores.append(mensaje)

    @classmethod
    def _trabajo(cls, ruc: str, anio: int, tipo: str) -> None:
        try:
            mensaje_inicio = (
                f"Conectando al SRI para declaraciones de IVA del año {anio}."
                if tipo == "iva" else
                f"Conectando al SRI para declaraciones de Renta Naturales del ejercicio {anio}."
                if tipo == "renta" else
                f"Conectando al SRI. IVA {anio}; Renta del ejercicio {anio - 1}."
            )
            cls._actualizar(ruc, anio, tipo, estado="ejecutando", mensaje=mensaje_inicio)
            resultado = asyncio.run(cls._procesar(ruc, anio, tipo))
            errores = int(resultado.get("errores", 0))
            descargas = int(resultado.get("descargadas", 0))
            existentes = int(resultado.get("ya_existian", 0))
            encontradas = int(resultado.get("encontradas", 0))
            mensaje = (
                f"Finalizado. Declaraciones encontradas: {encontradas}; "
                f"descargadas: {descargas}; ya existentes: {existentes}; errores: {errores}."
            )
            cls._actualizar(
                ruc, anio, tipo,
                estado="completado" if errores == 0 else "error",
                mensaje=mensaje,
                encontradas=encontradas,
                descargadas=descargas,
                ya_existian=existentes,
                errores=errores,
                total=encontradas,
                detalle_errores=resultado.get("detalle_errores", []),
            )
            logger.info(
                "SINCRONIZACIÓN DECLARACIONES | finalizada | ruc=%s | tipo=%s | año=%s | encontradas=%s | descargadas=%s | existentes=%s | errores=%s",
                ruc, tipo, anio, encontradas, descargas, existentes, errores,
            )
        except Exception as exc:
            logger.exception("SINCRONIZACIÓN DECLARACIONES | error general | ruc=%s | tipo=%s | año=%s", ruc, tipo, anio)
            cls._agregar_error(ruc, anio, tipo, f"{type(exc).__name__}: {exc}")
            cls._actualizar(
                ruc, anio, tipo, estado="error",
                mensaje=f"No se pudo completar la sincronización: {type(exc).__name__}: {exc}",
            )

    @classmethod
    async def _procesar(cls, ruc: str, anio: int, tipo_solicitado: str) -> dict[str, Any]:
        cred = await asyncio.to_thread(SriClienteSyncService._credenciales, ruc)
        p = browser = context = page = chrome_process = profile_dir = None
        download_dir: Path | None = None
        resultado = {
            "encontradas": 0,
            "descargadas": 0,
            "ya_existian": 0,
            "errores": 0,
            "detalle_errores": [],
        }
        try:
            logger.info("DECLARACIONES SRI | iniciando sesión | ruc=%s", ruc)
            (
                p, browser, context, page, chrome_process, profile_dir
            ) = await SriClienteSyncService._login(ruc, cred["clave"])
            page.set_default_timeout(TIMEOUT_MS)

            download_dir = Path(tempfile.mkdtemp(prefix=f"conta_declaraciones_{ruc}_"))
            try:
                cdp = await context.new_cdp_session(page)
                await cdp.send(
                    "Page.setDownloadBehavior",
                    {"behavior": "allow", "downloadPath": str(download_dir)},
                )
            except Exception:
                logger.warning(
                    "DECLARACIONES SRI | no se pudo configurar CDP; se usará Playwright save_as | ruc=%s",
                    ruc, exc_info=True,
                )
                cdp = None

            existing = await asyncio.to_thread(cls._listar_existentes_ttcweb, ruc)
            existing_keys = [str(item.get("key", "")) for item in existing]
            existing_names = [str(item.get("nombre", "")) for item in existing]

            # BdTotal.clientes.tipo_contribuyente determina qué opción de Renta
            # se consulta: RENTA NATURALES (data-rk=14) o RENTA SOCIEDADES (101).
            obligacion_renta = None
            if tipo_solicitado in {"renta", "ambos"}:
                obligacion_renta = _obligacion_renta_por_tipo_contribuyente(
                    cred.get("tipo_contribuyente", "")
                )
                logger.info(
                    "DECLARACIONES SRI | clasificación tributaria | ruc=%s | tipo_contribuyente=%s | obligación=%s",
                    ruc, cred.get("tipo_contribuyente", ""), obligacion_renta,
                )

            if tipo_solicitado == "iva":
                obligaciones = [("iva", anio)]
            elif tipo_solicitado == "renta":
                obligaciones = [(obligacion_renta, anio)]
            else:
                obligaciones = [("iva", anio), (obligacion_renta, anio - 1)]

            for tipo_declaracion, ejercicio in obligaciones:
                if tipo_declaracion == "iva":
                    descripcion = "IVA"
                elif tipo_declaracion == "renta_sociedades":
                    descripcion = "Renta Sociedades"
                else:
                    descripcion = "Renta Naturales"
                cls._actualizar(
                    ruc, anio, tipo_solicitado,
                    mensaje=f"Consultando {descripcion}, período fiscal {ejercicio}.",
                )
                try:
                    registros = await cls._consultar_obligacion(page, tipo_declaracion, ejercicio)
                except Exception as exc:
                    resultado["errores"] += 1
                    error_txt = f"{descripcion} {ejercicio}: {type(exc).__name__}: {exc}"
                    resultado["detalle_errores"].append(error_txt)
                    logger.exception("DECLARACIONES SRI | fallo consultando obligación | ruc=%s | tipo_declaracion=%s | ejercicio=%s", ruc, tipo_declaracion, ejercicio)
                    continue

                resultado["encontradas"] += len(registros)
                cls._actualizar(
                    ruc, anio, tipo_solicitado,
                    encontradas=resultado["encontradas"],
                    total=resultado["encontradas"],
                    mensaje=f"{descripcion} {ejercicio}: {len(registros)} declaración(es) localizada(s).",
                )

                # Las filas del SRI se paginan en el navegador. Volvemos a la primera
                # página antes de descargar y avanzamos en el mismo orden capturado.
                pagina_actual = 0
                if registros:
                    paginador = page.locator('[id="formPresentada:tblConsultaDeclaracion_paginator_bottom"]')
                    primera = paginador.locator(".ui-paginator-first").first
                    if await primera.count() and "ui-state-disabled" not in (await primera.get_attribute("class") or ""):
                        await cls._cambiar_pagina(page, ".ui-paginator-first")

                for registro in registros:
                    while pagina_actual < int(registro.get("pagina", 0)):
                        paginador = page.locator('[id="formPresentada:tblConsultaDeclaracion_paginator_bottom"]')
                        siguiente = paginador.locator(".ui-paginator-next").first
                        if await siguiente.count() == 0 or "ui-state-disabled" in (await siguiente.get_attribute("class") or ""):
                            break
                        await cls._cambiar_pagina(page, ".ui-paginator-next")
                        pagina_actual += 1

                    serie = registro["serie"]
                    if not serie:
                        resultado["errores"] += 1
                        resultado["detalle_errores"].append(
                            f"{descripcion} {ejercicio}: una fila no contiene número de serie."
                        )
                        continue

                    patron_serie = re.compile(rf"(?<!\d){re.escape(serie)}(?!\d)")
                    ya_guardada = any(
                        patron_serie.search(clave) for clave in (existing_keys + existing_names)
                    )
                    if ya_guardada:
                        resultado["ya_existian"] += 1
                        cls._actualizar(
                            ruc, anio, tipo_solicitado,
                            ya_existian=resultado["ya_existian"],
                            mensaje=f"{descripcion} {ejercicio}: {serie} ya está guardada; se omite.",
                        )
                        continue

                    try:
                        contenido, nombre_sri = await cls._descargar_declaracion(
                            page=page,
                            download_dir=download_dir,
                            registro=registro,
                            ruc=ruc,
                            tipo_declaracion=tipo_declaracion,
                        )
                        nombre_archivo = f"Declaracion_{serie}.pdf"
                        respuesta = await asyncio.to_thread(
                            cls._guardar_en_ttcweb,
                            ruc,
                            "iva" if tipo_declaracion == "iva" else "renta",
                            ejercicio,
                            registro["mes"],
                            nombre_archivo,
                            contenido,
                        )
                        key_guardada = str(respuesta.get("key", ""))
                        existing_keys.append(key_guardada)
                        existing_names.append(nombre_archivo)
                        if respuesta.get("existia"):
                            resultado["ya_existian"] += 1
                            cls._actualizar(ruc, anio, tipo_solicitado, ya_existian=resultado["ya_existian"])
                        else:
                            resultado["descargadas"] += 1
                            cls._actualizar(ruc, anio, tipo_solicitado, descargadas=resultado["descargadas"])
                        cls._actualizar(
                            ruc, anio, tipo_solicitado,
                            mensaje=f"{descripcion} {ejercicio}: procesada declaración {serie}.",
                        )
                        logger.info(
                            "DECLARACIONES SRI | documento guardado | ruc=%s | tipo_declaracion=%s | ejercicio=%s | serie=%s | archivo_sri=%s | key=%s",
                            ruc, tipo_declaracion, ejercicio, serie, nombre_sri, key_guardada,
                        )
                    except Exception as exc:
                        resultado["errores"] += 1
                        error_txt = f"{descripcion} {ejercicio}, serie {serie}: {type(exc).__name__}: {exc}"
                        resultado["detalle_errores"].append(error_txt)
                        cls._actualizar(ruc, anio, tipo_solicitado, errores=resultado["errores"])
                        logger.exception(
                            "DECLARACIONES SRI | fallo descargando declaración | ruc=%s | tipo_declaracion=%s | ejercicio=%s | serie=%s",
                            ruc, tipo_declaracion, ejercicio, serie,
                        )

            return resultado
        finally:
            try:
                if browser is not None:
                    await browser.close()
            except Exception:
                logger.debug("No se pudo cerrar la conexión Playwright de declaraciones.", exc_info=True)
            try:
                SriClienteSyncService._cerrar_chrome(chrome_process, profile_dir)
                if p is not None:
                    await p.stop()
                SriClienteSyncService._eliminar_perfil_chrome(profile_dir)
            except Exception:
                logger.exception("No se pudo cerrar correctamente Chrome de declaraciones | ruc=%s", ruc)
            if download_dir is not None:
                try:
                    await asyncio.to_thread(shutil.rmtree, download_dir, True)
                except Exception:
                    logger.debug("No se pudo limpiar la carpeta temporal de declaraciones.", exc_info=True)

    @staticmethod
    async def _cambiar_pagina(page, selector: str) -> None:
        """Pulsa un control del paginador y espera a que el SRI cambie la página."""
        paginador = page.locator('[id="formPresentada:tblConsultaDeclaracion_paginator_bottom"]')
        control = paginador.locator(selector).first
        if await control.count() == 0:
            raise RuntimeError("No se encontró el control de paginación de las declaraciones.")
        clase = await control.get_attribute("class") or ""
        if "ui-state-disabled" in clase:
            return
        etiqueta = paginador.locator(".ui-paginator-current").first
        anterior = (await etiqueta.inner_text()).strip() if await etiqueta.count() else ""
        await control.click()
        if anterior:
            await page.wait_for_function(
                """(args) => {
                    const el = document.querySelector(args.selector);
                    return !!el && el.textContent.trim() !== args.previous;
                }""",
                arg={
                    "selector": '[id="formPresentada:tblConsultaDeclaracion_paginator_bottom"] .ui-paginator-current',
                    "previous": anterior,
                },
                timeout=TIMEOUT_MS,
            )
        else:
            await page.wait_for_timeout(700)

    @classmethod
    async def _consultar_obligacion(cls, page, tipo: str, anio: int) -> list[dict[str, Any]]:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError

        if tipo not in OBLIGACIONES:
            raise ValueError("Tipo de obligación no admitido.")
        obligacion = OBLIGACIONES[tipo]
        await page.goto(
            SRI_DECLARACIONES_URL,
            wait_until="domcontentloaded",
            timeout=TIMEOUT_MS,
        )
        await page.wait_for_selector(
            '[id="formPresentada:tblGrupoObligacionSeleccion_data"] tr[data-rk]',
            state="visible",
            timeout=TIMEOUT_MS,
        )

        fila_obligacion = page.locator(
            f'[id="formPresentada:tblGrupoObligacionSeleccion_data"] tr[data-rk="{obligacion["data_rk"]}"]'
        )
        if await fila_obligacion.count() != 1:
            raise RuntimeError(f"No apareció la opción {obligacion['titulo']} en el SRI.")

        check = fila_obligacion.locator(".ui-chkbox-box").first
        if await check.count() == 0:
            raise RuntimeError(f"No se encontró la casilla para {obligacion['titulo']}.")
        # El SRI conserva a veces la selección anterior. Solo pulsamos la casilla
        # cuando la obligación todavía no está seleccionada, para evitar desmarcarla.
        clases_fila = await fila_obligacion.get_attribute("class") or ""
        aria_seleccionada = (await fila_obligacion.get_attribute("aria-selected") or "").lower() == "true"
        clases_casilla = await check.get_attribute("class") or ""
        ya_seleccionada = (
            aria_seleccionada
            or "ui-state-highlight" in clases_fila
            or "ui-state-active" in clases_casilla
        )
        if not ya_seleccionada:
            await check.click(timeout=15000)
        logger.info(
            "DECLARACIONES SRI | obligación seleccionada | tipo=%s | data-rk=%s | ya_seleccionada=%s",
            tipo, obligacion["data_rk"], ya_seleccionada,
        )

        await page.locator('[id="formPresentada:btnBuscarGrupoObligacion"]').click(timeout=15000)
        await page.locator('[id="formPresentada:dlgPeriodoSeleccion"]').wait_for(
            state="visible", timeout=TIMEOUT_MS
        )

        year_select = page.locator('[id="formPresentada:somAnioFiscal_input"]')
        opciones = await year_select.locator("option").evaluate_all(
            "(items) => items.map((item) => item.value)"
        )
        if str(anio) not in opciones:
            raise ValueError(f"El SRI no ofrece el período fiscal {anio} para {obligacion['titulo']}.")

        valor_actual = await year_select.input_value()
        if valor_actual != str(anio):
            await page.locator('[id="formPresentada:somAnioFiscal_label"]').click()
            opcion = page.locator(
                f'[id="formPresentada:somAnioFiscal_panel"] li.ui-selectonemenu-item[data-label="{anio}"]'
            )
            try:
                await opcion.wait_for(state="visible", timeout=10000)
            except PlaywrightTimeoutError:
                await page.locator('[id="formPresentada:somAnioFiscal_label"]').click()
                await opcion.wait_for(state="visible", timeout=10000)
            await opcion.click()

        await page.locator('[id="formPresentada:btnAceptarPeriodoSeleccion"]').click(timeout=15000)
        await page.locator('[id="formPresentada:tblConsultaDeclaracion_data"]').wait_for(
            state="attached", timeout=TIMEOUT_MS
        )
        await page.wait_for_timeout(800)

        registros: list[dict[str, Any]] = []
        pagina = 0
        paginas_vistas: set[str] = set()
        while True:
            filas = page.locator('[id="formPresentada:tblConsultaDeclaracion_data"] tr[data-ri]')
            count = await filas.count()
            for idx in range(count):
                fila = filas.nth(idx)
                celdas = [
                    " ".join((texto or "").split())
                    for texto in await fila.locator("td").all_inner_texts()
                ]
                if len(celdas) < 9:
                    continue

                descripcion = celdas[1]
                periodo = celdas[2]
                norma_desc = _sin_acentos(descripcion)
                norma_periodo = _sin_acentos(periodo)
                if tipo == "iva" and not (
                    "declaracion" in norma_desc and "iva" in norma_desc
                ):
                    continue
                if tipo == "renta_natural" and not (
                    "renta" in norma_desc and "naturales" in norma_desc
                    and "herencias" not in norma_desc
                ):
                    continue
                if tipo == "renta_sociedades" and not (
                    "renta" in norma_desc and "sociedades" in norma_desc
                ):
                    continue

                periodo_anio_match = re.search(r"\b(20\d{2})\b", norma_periodo)
                periodo_anio = int(periodo_anio_match.group(1)) if periodo_anio_match else anio
                if periodo_anio != anio:
                    continue

                mes = "00"
                if tipo == "iva":
                    # El SRI presenta IVA mensual por nombre de mes y el IVA
                    # semestral como PRIMER/SEGUNDO SEMESTRE. Guardamos estos
                    # últimos como S1/S2 para no confundirlos con meses.
                    if "semestre" in norma_periodo or "semestral" in norma_desc:
                        if re.search(r"\b(primer|primero|1er|primer semestre)\b", norma_periodo):
                            mes = "S1"
                        elif re.search(r"\b(segundo|segunda|2do)\b", norma_periodo):
                            mes = "S2"
                        else:
                            logger.warning(
                                "No se pudo reconocer el semestre fiscal | período=%s | descripción=%s",
                                periodo, descripcion,
                            )
                            continue
                    else:
                        mes = next(
                            (numero for nombre, numero in MESES.items() if re.search(rf"\b{nombre}\b", norma_periodo)),
                            "",
                        )
                        if not mes:
                            logger.warning("No se pudo reconocer el mes fiscal | período=%s", periodo)
                            continue

                serie = re.sub(r"\D", "", celdas[4])
                if not serie:
                    continue
                registros.append({
                    "fila": int(await fila.get_attribute("data-ri") or idx),
                    "pagina": pagina,
                    "serie": serie,
                    "formulario": celdas[0],
                    "descripcion": descripcion,
                    "periodo": periodo,
                    "mes": mes,
                    "estado_pago": celdas[7],
                    "estado_declaracion": celdas[8],
                })

            paginas = page.locator('[id="formPresentada:tblConsultaDeclaracion_paginator_bottom"]')
            next_page = paginas.locator(".ui-paginator-next").first
            if await next_page.count() == 0 or "ui-state-disabled" in (await next_page.get_attribute("class") or ""):
                break
            before = ",".join(r["serie"] for r in registros)
            if before in paginas_vistas or pagina > 20:
                break
            paginas_vistas.add(before)
            await cls._cambiar_pagina(page, ".ui-paginator-next")
            pagina += 1

        # No devolver duplicados si el SRI repite una fila al paginar.
        unicos: dict[str, dict[str, Any]] = {}
        for registro in registros:
            unicos.setdefault(registro["serie"], registro)
        return list(unicos.values())

    @classmethod
    async def _descargar_declaracion(
        cls, page, download_dir: Path, registro: dict[str, Any], ruc: str,
        tipo_declaracion: str,
    ) -> tuple[bytes, str]:
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError

        fila = page.locator(
            f'[id="formPresentada:tblConsultaDeclaracion_data"] tr[data-ri="{registro["fila"]}"]'
        )
        # Para IVA usamos «Declaración completa», porque la perfilada puede
        # presentar inconsistencias. Para Renta se conserva el comportamiento previo.
        boton_titulo = "Declaración completa" if tipo_declaracion == "iva" else "Declaración perfilada"
        boton = fila.locator(f'button[title="{boton_titulo}"]').first
        if await boton.count() == 0:
            raise RuntimeError(
                f"No se encontró el botón «{boton_titulo}» para la serie {registro['serie']}."
            )
        logger.info(
            "DECLARACIONES SRI | descargando PDF | ruc=%s | serie=%s | botón=%s | fila=%s | página=%s",
            ruc, registro["serie"], boton_titulo, registro["fila"], registro.get("pagina", 0),
        )

        async with page.expect_download(timeout=TIMEOUT_MS) as espera:
            await boton.click(timeout=20000)
        descarga = await espera.value
        fallo = await descarga.failure()
        if fallo:
            raise RuntimeError(f"Chrome no pudo descargar el documento: {fallo}")

        nombre_sri = Path(descarga.suggested_filename or f"Declaracion_{registro['serie']}.pdf").name
        destino_sri = download_dir / nombre_sri
        contenido = b""

        limite = asyncio.get_running_loop().time() + 12
        while asyncio.get_running_loop().time() < limite:
            try:
                if destino_sri.is_file() and not destino_sri.name.lower().endswith(".crdownload"):
                    candidato = await asyncio.to_thread(destino_sri.read_bytes)
                    if candidato.startswith(b"%PDF"):
                        contenido = candidato
                        break
                candidatos = [
                    ruta for ruta in download_dir.iterdir()
                    if ruta.is_file() and not ruta.name.lower().endswith(".crdownload")
                ]
                candidato_ruta = max(
                    candidatos,
                    key=lambda ruta: ruta.stat().st_mtime,
                    default=None,
                )
                if candidato_ruta is not None:
                    candidato = await asyncio.to_thread(candidato_ruta.read_bytes)
                    if candidato.startswith(b"%PDF"):
                        contenido = candidato
                        nombre_sri = candidato_ruta.name
                        break
            except OSError:
                logger.debug("Esperando el archivo PDF del SRI | ruc=%s", ruc, exc_info=True)
            await asyncio.sleep(0.25)

        if not contenido.startswith(b"%PDF"):
            with tempfile.NamedTemporaryFile(prefix=f"decl_{ruc}_", suffix=".pdf", delete=False) as tmp:
                temporario = Path(tmp.name)
            try:
                await descarga.save_as(str(temporario))
                contenido = await asyncio.to_thread(temporario.read_bytes)
            except Exception as exc:
                raise RuntimeError(f"No se pudo recuperar el PDF desde Playwright: {exc}") from exc
            finally:
                temporario.unlink(missing_ok=True)

        if not contenido.startswith(b"%PDF"):
            raise ValueError(
                f"El SRI no devolvió un PDF válido para la serie {registro['serie']}; "
                f"bytes={len(contenido)}; archivo={nombre_sri!r}."
            )

        # El nombre original se incluye solo en el registro interno; el archivo
        # final se normaliza a Declaracion_<serie>.pdf para evitar duplicados.
        # Deja limpia la carpeta entre descargas; así un archivo anterior no
        # puede confundirse con el PDF de la siguiente declaración.
        for archivo_temporal in download_dir.iterdir():
            try:
                if archivo_temporal.is_file():
                    archivo_temporal.unlink(missing_ok=True)
            except OSError:
                logger.debug(
                    "No se pudo retirar archivo temporal de descarga | ruc=%s | archivo=%s",
                    ruc, archivo_temporal.name, exc_info=True,
                )
        return contenido, nombre_sri

    @staticmethod
    def _listar_existentes_ttcweb(ruc: str) -> list[dict[str, Any]]:
        token = str(settings.TOTALCOUNTS_INTERNAL_TOKEN or "").strip()
        base_url = str(settings.TOTALCOUNTS_INTERNAL_URL or "").strip().rstrip("/")
        if not token:
            raise RuntimeError("TOTALCOUNTS_INTERNAL_TOKEN no está configurado en Conta.")
        if not base_url:
            raise RuntimeError("TOTALCOUNTS_INTERNAL_URL no está configurado en Conta.")
        req = urllib.request.Request(
            f"{base_url}/internal/conta/declaraciones-sri/{ruc}/",
            headers={"Accept": "application/json", "X-TotalCounts-Internal": token},
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"TtCWeb respondió HTTP {exc.code} al listar documentos: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"No se pudo listar los documentos en TtCWeb: {exc.reason}") from exc
        if not data.get("ok"):
            raise RuntimeError(str(data.get("mensaje", "TtCWeb no devolvió la lista de documentos.")))
        return list(data.get("files", []))

    @staticmethod
    def _guardar_en_ttcweb(
        ruc: str,
        tipo: str,
        anio: int,
        mes: str,
        nombre_archivo: str,
        contenido: bytes,
    ) -> dict[str, Any]:
        token = str(settings.TOTALCOUNTS_INTERNAL_TOKEN or "").strip()
        base_url = str(settings.TOTALCOUNTS_INTERNAL_URL or "").strip().rstrip("/")
        if not token:
            raise RuntimeError("TOTALCOUNTS_INTERNAL_TOKEN no está configurado en Conta.")
        if not base_url:
            raise RuntimeError("TOTALCOUNTS_INTERNAL_URL no está configurado en Conta.")

        boundary = "----ContaDeclaracion" + __import__("uuid").uuid4().hex

        def field(nombre: str, valor: str) -> bytes:
            return (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{nombre}"\r\n\r\n'
                f"{valor}\r\n"
            ).encode("utf-8")

        body = bytearray()
        body.extend(field("tipo", tipo))
        body.extend(field("anio_fiscal", str(anio)))
        body.extend(field("mes", mes))
        body.extend(field("nombre_archivo", nombre_archivo))
        body.extend(
            (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="archivo"; filename="{nombre_archivo}"\r\n'
                "Content-Type: application/pdf\r\n\r\n"
            ).encode("utf-8")
        )
        body.extend(contenido)
        body.extend(b"\r\n")
        body.extend(f"--{boundary}--\r\n".encode("utf-8"))

        req = urllib.request.Request(
            f"{base_url}/internal/conta/declaraciones-sri/{ruc}/",
            data=bytes(body),
            headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Accept": "application/json",
                "X-TotalCounts-Internal": token,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as response:
                result = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"TtCWeb rechazó la declaración ({exc.code}): {body_text}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"No se pudo conectar con TtCWeb para guardar el PDF: {exc.reason}") from exc
        if not result.get("ok"):
            raise RuntimeError(str(result.get("mensaje", "TtCWeb no guardó el PDF.")))
        return result
