from __future__ import annotations

import calendar
import json
from datetime import date
from typing import Any

from sqlalchemy import text

from app.database.client_connection import obtener_session_cliente
from app.services.sri_cliente_sync_service import SriClienteSyncService


class SriVentasValidatorService:
    """Valida ventas emitidas contra el SRI, día por día.

    Primera pasada: cuenta las filas que devuelve el SRI en todas sus páginas
    y las compara con COUNT(*) de ventas en la base del cliente.

    Solo cuando los conteos son diferentes se hace una segunda pasada, factura
    por factura, usando la clave de acceso para encontrar faltantes y sobrantes.
    Los faltantes del SRI se vuelven a guardar en ventas; los sobrantes locales
    solamente se reportan y nunca se eliminan automáticamente.
    """

    @classmethod
    async def sincronizar_mes(
        cls,
        ruc: str,
        anio: int,
        mes: int,
        tipo_comprobante: int = 1,
        job_id: str | None = None,
    ) -> dict[str, Any]:
        cred = SriClienteSyncService._credenciales(ruc)
        result: dict[str, Any] = {
            "ruc": ruc,
            "cliente": cred["nombre"],
            "anio": anio,
            "mes": mes,
            "tipo_comprobante": SriClienteSyncService._tipo(tipo_comprobante),
            "sri": 0,
            "ya_existentes": 0,
            "descargadas": 0,
            "guardadas": 0,
            "errores": [],
            "paginas": 0,
            "dias_revisados": 0,
            "dias_ok": 0,
            "dias_diferentes": 0,
            "faltantes": 0,
            "sobrantes": 0,
            "detalle_dias": [],
        }

        p = browser = context = page = chrome_process = None
        db = None

        try:
            SriClienteSyncService._job_update(
                job_id,
                mensaje="Validador: abriendo sesión del SRI.",
            )
            p, browser, context, page, chrome_process = await SriClienteSyncService._login(
                ruc, cred["clave"]
            )
            SriClienteSyncService._job_update(
                job_id,
                estado="captcha",
                mensaje="Validador: consultando comprobantes emitidos. Si aparece CAPTCHA, resuélvalo en Chromium.",
            )
            await SriClienteSyncService._consultar_emitidos(page, anio, mes)
            SriClienteSyncService._job_update(
                job_id,
                estado="ejecutando",
                mensaje="Validador activo. Comparando cantidades día por día.",
            )

            db = obtener_session_cliente(ruc)
            ultimo_dia = calendar.monthrange(anio, mes)[1]

            dias_a_revisar = list(range(1, ultimo_dia + 1))
            if job_id:
                with __import__("app.database.connection", fromlist=["engine"]).engine.connect() as job_db:
                    detalle_job = job_db.execute(
                        text("SELECT detalle FROM conta_sri_jobs WHERE job_id = :job_id"),
                        {"job_id": job_id},
                    ).scalar()
                try:
                    payload_job = json.loads(detalle_job or "{}")
                    fechas_objetivo = payload_job.get("dias_objetivo") or []
                except (TypeError, ValueError):
                    fechas_objetivo = []
                if fechas_objetivo:
                    dias_a_revisar = sorted({
                        date.fromisoformat(str(fecha)).day
                        for fecha in fechas_objetivo
                        if date.fromisoformat(str(fecha)).year == anio
                        and date.fromisoformat(str(fecha)).month == mes
                    })
                    if not dias_a_revisar:
                        raise ValueError("No hay fechas válidas para reparar en el período seleccionado.")

            for dia in dias_a_revisar:
                SriClienteSyncService._verificar_cancelacion(job_id)
                fecha_consulta = date(anio, mes, dia)
                fecha_txt = fecha_consulta.strftime("%Y-%m-%d")
                fecha_ui = fecha_consulta.strftime("%d/%m/%Y")

                SriClienteSyncService._job_update(
                    job_id,
                    mensaje=f"Validador: contando {fecha_ui} en SRI y base de datos.",
                )

                cantidad_sri = await cls._contar_dia_sri(
                    page, fecha_consulta, result, job_id
                )
                cantidad_bd = db.execute(text("""
                    SELECT COUNT(*)
                    FROM ventas
                    WHERE TRIM(fecfactur::text) = :fecha
                      AND TRIM(mes::text) = :mes
                      AND TRIM(año::text) = :anio
                      AND TRIM(codcomp::text) = '18'
                """), {
                    "fecha": fecha_ui,
                    "mes": f"{mes:02d}",
                    "anio": str(anio),
                }).scalar_one()

                result["dias_revisados"] += 1
                if cantidad_sri == cantidad_bd:
                    result["dias_ok"] += 1
                dia_info = {
                    "fecha": fecha_txt,
                    "sri": int(cantidad_sri),
                    "base_datos": int(cantidad_bd),
                    "estado": "OK" if cantidad_sri == cantidad_bd else "DIFERENCIA",
                    "faltantes": 0,
                    "sobrantes": 0,
                }

                SriClienteSyncService._job_update(
                    job_id,
                    dias_revisados=result["dias_revisados"],
                    dias_ok=result["dias_ok"],
                    dias_diferentes=result["dias_diferentes"],
                    mensaje=(
                        f"Comparación {fecha_ui}: SRI={cantidad_sri}, BD={cantidad_bd} — "
                        f"{'OK' if cantidad_sri == cantidad_bd else 'DIFERENCIA'}."
                    ),
                )

                if cantidad_sri != cantidad_bd:
                    result["dias_diferentes"] += 1
                    SriClienteSyncService._job_update(
                        job_id,
                        dias_revisados=result["dias_revisados"],
                        dias_ok=result["dias_ok"],
                        dias_diferentes=result["dias_diferentes"],
                        mensaje=(
                            f"Diferencia {fecha_ui}: SRI={cantidad_sri}, "
                            f"BD={cantidad_bd}. Revisando factura por factura."
                        ),
                    )
                    faltantes, sobrantes = await cls._revisar_dia_uno_por_uno(
                        page, db, fecha_consulta, result, job_id
                    )
                    dia_info["faltantes"] = len(faltantes)
                    dia_info["sobrantes"] = len(sobrantes)
                    dia_info["claves_faltantes"] = faltantes[:100]
                    dia_info["claves_sobrantes"] = sobrantes[:100]
                    result["faltantes"] += len(faltantes)
                    result["sobrantes"] += len(sobrantes)

                    if job_id:
                        with __import__("app.database.connection", fromlist=["engine"]).engine.connect() as job_db:
                            operacion_job = job_db.execute(
                                text("SELECT operacion FROM conta_sri_jobs WHERE job_id = :job_id"),
                                {"job_id": job_id},
                            ).scalar()
                        if operacion_job == "ventas_reparar":
                            cantidad_bd_final = db.execute(text("""
                                SELECT COUNT(*)
                                FROM ventas
                                WHERE TRIM(fecfactur::text) = :fecha
                                  AND TRIM(mes::text) = :mes
                                  AND TRIM(año::text) = :anio
                                  AND TRIM(codcomp::text) = '18'
                            """), {
                                "fecha": fecha_ui,
                                "mes": f"{mes:02d}",
                                "anio": str(anio),
                            }).scalar_one()
                            dia_info["base_datos"] = int(cantidad_bd_final)
                            dia_info["estado"] = (
                                "OK" if int(cantidad_sri) == int(cantidad_bd_final)
                                and not sobrantes
                                else "DIFERENCIA"
                            )
                            if dia_info["estado"] == "OK":
                                result["dias_diferentes"] = max(0, result["dias_diferentes"] - 1)
                                result["dias_ok"] += 1

                    SriClienteSyncService._job_update(
                        job_id,
                        faltantes=result["faltantes"],
                        sobrantes=result["sobrantes"],
                        guardadas=result["guardadas"],
                        descargadas=result["descargadas"],
                        mensaje=(
                            f"{fecha_ui}: revisión individual terminada. "
                            f"Faltantes={len(faltantes)}, sobrantes={len(sobrantes)}."
                        ),
                    )

                result["detalle_dias"].append(dia_info)
                SriClienteSyncService._job_update(
                    job_id,
                    dias_revisados=result["dias_revisados"],
                    dias_ok=result["dias_ok"],
                    dias_diferentes=result["dias_diferentes"],
                    faltantes=result["faltantes"],
                    sobrantes=result["sobrantes"],
                    detalle=json.dumps(result["detalle_dias"], ensure_ascii=False, default=str),
                )

            detalle = json.dumps(
                result["detalle_dias"],
                ensure_ascii=False,
                default=str,
            )
            SriClienteSyncService._job_update(
                job_id,
                detalle=detalle,
                mensaje=(
                    f"Validador terminado: {result['dias_revisados']} días revisados, "
                    f"{result['dias_diferentes']} con diferencia, "
                    f"{result['faltantes']} faltantes recuperados."
                ),
            )
            result["ok"] = not result["errores"]
            return result

        finally:
            if db is not None:
                db.close()
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
                await p.stop()

    @classmethod
    async def _contar_dia_sri(
        cls,
        page,
        fecha: date,
        result: dict[str, Any],
        job_id: str | None,
    ) -> int:
        """Cuenta todas las filas del día recorriendo todas las páginas."""
        cantidad_inicial = await SriClienteSyncService._consultar_emitidos_dia(
            page, fecha
        )
        if cantidad_inicial == 0:
            return 0

        total = 0
        pagina = 1

        while True:
            SriClienteSyncService._verificar_cancelacion(job_id)
            filas = page.locator("#frmPrincipal\\:tablaCompEmitidos_data tr")
            cantidad = await filas.count()
            total += cantidad
            result["sri"] += cantidad
            result["paginas"] += 1

            SriClienteSyncService._job_update(
                job_id,
                sri=result["sri"],
                paginas=result["paginas"],
                mensaje=(
                    f"Validador: {fecha.strftime('%d/%m/%Y')} "
                    f"página {pagina}, {cantidad} registros; acumulado del día={total}."
                ),
            )

            boton_next = page.locator("[class*='ui-paginator-next']").first
            if await boton_next.count() == 0:
                break

            clases = (await boton_next.get_attribute("class") or "").lower()
            aria = (await boton_next.get_attribute("aria-disabled") or "").lower()
            disabled = await boton_next.get_attribute("disabled")
            if (
                disabled is not None
                or aria == "true"
                or "ui-state-disabled" in clases
                or "disabled" in clases
            ):
                break

            primera = ""
            try:
                if await filas.count():
                    primera = await filas.first.inner_text()
            except Exception:
                pass

            await boton_next.click()

            for _ in range(30):
                await page.wait_for_timeout(500)
                try:
                    if not primera or await filas.first.inner_text() != primera:
                        break
                except Exception:
                    pass

            pagina += 1

        await SriClienteSyncService._volver_pagina_1_emitidos(page)
        return total

    @classmethod
    async def _revisar_dia_uno_por_uno(
        cls,
        page,
        db,
        fecha: date,
        result: dict[str, Any],
        job_id: str | None,
    ) -> tuple[list[str], list[str]]:
        """Obtiene cada clave del SRI y la compara individualmente con ventas."""
        await SriClienteSyncService._volver_pagina_1_emitidos(page)
        await SriClienteSyncService._consultar_emitidos_dia(page, fecha)

        claves_sri: set[str] = set()
        faltantes: list[str] = []
        pagina = 1

        while True:
            SriClienteSyncService._verificar_cancelacion(job_id)
            filas = page.locator("#frmPrincipal\\:tablaCompEmitidos_data tr")
            cantidad = await filas.count()

            for idx in range(cantidad):
                SriClienteSyncService._verificar_cancelacion(job_id)
                texto_fila = ""
                fecha_factura = fecha.strftime("%d/%m/%Y")
                numero_factura = "No identificado"
                try:
                    try:
                        texto_fila = " ".join((await filas.nth(idx).inner_text()).split())
                    except Exception:
                        texto_fila = ""

                    # La fila del listado de emitidos suele mostrar fecha y
                    # número de comprobante aun cuando el detalle no abre.
                    import re
                    fechas_fila = re.findall(r"\b(?:\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2})\b", texto_fila)
                    if fechas_fila:
                        fecha_factura = fechas_fila[0]
                    numeros_fila = re.findall(r"\b\d{3}-\d{3}-\d{9}\b", texto_fila)
                    if numeros_fila:
                        numero_factura = numeros_fila[0]
                    else:
                        secuenciales = re.findall(r"\b\d{15}\b", texto_fila)
                        if secuenciales:
                            numero_factura = f"{secuenciales[0][:3]}-{secuenciales[0][3:6]}-{secuenciales[0][6:]}"

                    html = await SriClienteSyncService._obtener_detalle_emitido(page, idx)
                    if not html:
                        claves_fila = re.findall(r"\b\d{49}\b", texto_fila)
                        diagnostico_clave = ""
                        if claves_fila:
                            consulta_clave = await SriClienteSyncService._consultar_validez_por_clave(
                                page, claves_fila[0]
                            )
                            diagnostico_clave = (
                                f" Consulta individual por clave: {consulta_clave.get('estado', 'NO_VERIFICADO')}. "
                                f"{consulta_clave.get('detalle', '')[:500]}"
                            )
                        else:
                            diagnostico_clave = " No se pudo extraer una clave de acceso de 49 dígitos de la fila."
                        raise ValueError(
                            "No se pudo abrir el detalle de la factura. "
                            f"Fecha: {fecha_factura}; número de factura: {numero_factura}. "
                            f"Fila SRI: {texto_fila or 'sin texto disponible'}."
                            f"{diagnostico_clave}"
                        )

                    factura = SriClienteSyncService._parsear_factura_emitida_html(html)
                    if factura["fecha"].date() != fecha:
                        continue

                    clave = factura["clave_acceso"].strip()
                    if not clave:
                        raise ValueError("Factura sin clave de acceso.")

                    claves_sri.add(clave)

                    existe = db.execute(text("""
                        SELECT 1
                        FROM ventas
                        WHERE TRIM(autorizacion::text) = :clave
                        LIMIT 1
                    """), {"clave": clave}).first()

                    if existe:
                        result["ya_existentes"] += 1
                    else:
                        # Solo insertamos lo que realmente está en el SRI y
                        # falta en la base. Nunca borramos sobrantes locales.
                        SriClienteSyncService._insertar_venta(db, factura)
                        db.commit()
                        faltantes.append(clave)
                        result["guardadas"] += 1
                        result["descargadas"] += 1

                    SriClienteSyncService._job_update(
                        job_id,
                        sri=result["sri"],
                        ya_existentes=result["ya_existentes"],
                        guardadas=result["guardadas"],
                        descargadas=result["guardadas"],
                        mensaje=(
                            f"Revisión individual {fecha.strftime('%d/%m/%Y')}: "
                            f"factura {idx + 1}/{cantidad} de página {pagina}."
                        ),
                    )

                except Exception as exc:
                    db.rollback()
                    result["errores"].append({
                        "fecha": fecha.isoformat(),
                        "pagina": pagina,
                        "fila": idx + 1,
                        "numero_factura": numero_factura,
                        "fecha_factura": fecha_factura,
                        "detalle": str(exc),
                    })
                    SriClienteSyncService._job_update(
                        job_id,
                        errores=result["errores"],
                        mensaje=(
                            f"Error validando {fecha.strftime('%d/%m/%Y')} "
                            f"fila {idx + 1}: {exc}"
                        ),
                    )

            boton_next = page.locator("[class*='ui-paginator-next']").first
            if await boton_next.count() == 0:
                break

            clases = (await boton_next.get_attribute("class") or "").lower()
            aria = (await boton_next.get_attribute("aria-disabled") or "").lower()
            disabled = await boton_next.get_attribute("disabled")
            if (
                disabled is not None
                or aria == "true"
                or "ui-state-disabled" in clases
                or "disabled" in clases
            ):
                break

            primera = ""
            try:
                if await filas.count():
                    primera = await filas.first.inner_text()
            except Exception:
                pass

            await boton_next.click()

            for _ in range(30):
                await page.wait_for_timeout(500)
                try:
                    if not primera or await filas.first.inner_text() != primera:
                        break
                except Exception:
                    pass

            pagina += 1

        db_keys = db.execute(text("""
            SELECT TRIM(autorizacion::text)
            FROM ventas
            WHERE TRIM(fecfactur::text) = :fecha
              AND TRIM(mes::text) = :mes
              AND TRIM(año::text) = :anio
              AND TRIM(codcomp::text) = '18'
              AND COALESCE(TRIM(autorizacion::text), '') <> ''
        """), {
            "fecha": fecha.strftime("%d/%m/%Y"),
            "mes": f"{fecha.month:02d}",
            "anio": str(fecha.year),
        }).scalars().all()
        claves_bd = {str(k).strip() for k in db_keys if k}

        # "faltantes" conserva las claves que realmente no estaban en BD antes
        # de insertarlas. Se deduplica por seguridad.
        faltantes = sorted(set(faltantes))
        sobrantes = sorted(claves_bd - claves_sri)

        await SriClienteSyncService._volver_pagina_1_emitidos(page)
        return faltantes, sobrantes
