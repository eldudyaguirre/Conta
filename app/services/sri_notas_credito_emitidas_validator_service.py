from __future__ import annotations

import calendar
from datetime import date
from typing import Any

from sqlalchemy import text

from app.database.client_connection import obtener_session_cliente
from app.services.sri_cliente_sync_service import SriClienteSyncService
from app.services.sri_ventas_validator_service import SriVentasValidatorService


class SriNotasCreditoEmitidasValidatorService:
    """Compara las notas de crédito emitidas del SRI con ncventas por día.

    Cuando encuentra diferencias, vuelve a procesar únicamente ese día usando
    el mismo parser y guardado seguro que la sincronización normal de NC.
    La clave de autorización evita duplicados y nunca se eliminan registros.
    """

    @classmethod
    async def sincronizar_mes(
        cls,
        ruc: str,
        anio: int,
        mes: int,
        tipo_comprobante: int = 3,
        job_id: str | None = None,
    ) -> dict[str, Any]:
        cred = SriClienteSyncService._credenciales(ruc)
        result: dict[str, Any] = {
            "ruc": ruc,
            "cliente": cred["nombre"],
            "anio": anio,
            "mes": mes,
            "tipo_comprobante": "04",
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
        p = browser = context = page = chrome_process = profile_dir = None
        db = None
        try:
            SriClienteSyncService._job_update(
                job_id, mensaje="Validador de notas de crédito: abriendo sesión del SRI."
            )
            p, browser, context, page, chrome_process, profile_dir = await SriClienteSyncService._login(
                ruc, cred["clave"]
            )
            SriClienteSyncService._job_update(
                job_id,
                estado="captcha",
                mensaje="Consultando notas de crédito emitidas en el SRI.",
            )
            await SriClienteSyncService._consultar_emitidos(
                page, anio, mes, tipo_emitido="nota_credito"
            )
            SriClienteSyncService._job_update(
                job_id,
                estado="ejecutando",
                mensaje="Comparando notas de crédito emitidas día por día.",
            )
            db = obtener_session_cliente(ruc)
            ultimo_dia = calendar.monthrange(anio, mes)[1]
            for dia in range(1, ultimo_dia + 1):
                SriClienteSyncService._verificar_cancelacion(job_id)
                fecha = date(anio, mes, dia)
                fecha_ui = fecha.strftime("%d/%m/%Y")
                cantidad_sri = await SriVentasValidatorService._contar_dia_sri(
                    page, fecha, result, job_id
                )
                cantidad_bd = db.execute(text("""
                    SELECT COUNT(*)
                    FROM ncventas
                    WHERE TRIM(fecnc::text) = :fecha
                      AND TRIM(mes::text) = :mes
                      AND TRIM("año"::text) = :anio
                      AND TRIM(codcomp::text) = '04'
                """), {
                    "fecha": fecha_ui,
                    "mes": f"{mes:02d}",
                    "anio": str(anio),
                }).scalar_one()

                diferencia = int(cantidad_sri) != int(cantidad_bd)
                dia_info = {
                    "fecha": fecha.isoformat(),
                    "sri": int(cantidad_sri),
                    "base_datos": int(cantidad_bd),
                    "estado": "DIFERENCIA" if diferencia else "OK",
                    "faltantes": 0,
                    "sobrantes": 0,
                }
                result["dias_revisados"] += 1
                if diferencia:
                    result["dias_diferentes"] += 1
                    SriClienteSyncService._job_update(
                        job_id,
                        mensaje=(
                            f"Hay diferencia en {fecha_ui}: SRI={cantidad_sri}, "
                            f"BD={cantidad_bd}. Recuperando notas de ese día."
                        ),
                    )
                    proceso = {
                        "sri": 0, "ya_existentes": 0, "descargadas": 0,
                        "guardadas": 0, "errores": [], "paginas": 0,
                    }
                    await SriClienteSyncService._procesar_emitidos_ventas(
                        page, db, proceso, job_id, set(), anio, mes,
                        texto_tipo="nota de crédito",
                        codcomp="04",
                        destino="ncventas",
                        dias_objetivo=[fecha.isoformat()],
                    )
                    db.commit()
                    result["ya_existentes"] += proceso["ya_existentes"]
                    result["descargadas"] += proceso["descargadas"]
                    result["guardadas"] += proceso["guardadas"]
                    result["paginas"] += proceso["paginas"]
                    result["errores"].extend(proceso["errores"])
                    cantidad_final = db.execute(text("""
                        SELECT COUNT(*)
                        FROM ncventas
                        WHERE TRIM(fecnc::text) = :fecha
                          AND TRIM(mes::text) = :mes
                          AND TRIM("año"::text) = :anio
                          AND TRIM(codcomp::text) = '04'
                    """), {
                        "fecha": fecha_ui,
                        "mes": f"{mes:02d}",
                        "anio": str(anio),
                    }).scalar_one()
                    dia_info["base_datos"] = int(cantidad_final)
                    dia_info["faltantes"] = max(0, int(cantidad_sri) - int(cantidad_bd))
                    dia_info["estado"] = (
                        "OK" if int(cantidad_final) == int(cantidad_sri) else "DIFERENCIA"
                    )
                    if dia_info["estado"] == "OK":
                        result["dias_diferentes"] -= 1
                        result["dias_ok"] += 1
                    result["faltantes"] += dia_info["faltantes"]
                else:
                    result["dias_ok"] += 1

                result["detalle_dias"].append(dia_info)
                SriClienteSyncService._job_update(
                    job_id,
                    sri=result["sri"],
                    dias_revisados=result["dias_revisados"],
                    dias_ok=result["dias_ok"],
                    dias_diferentes=result["dias_diferentes"],
                    faltantes=result["faltantes"],
                    guardadas=result["guardadas"],
                    ya_existentes=result["ya_existentes"],
                    detalle=__import__("json").dumps(
                        result["detalle_dias"], ensure_ascii=False
                    ),
                    mensaje=(
                        f"Notas de crédito {fecha_ui}: SRI={cantidad_sri}, "
                        f"BD={dia_info['base_datos']} — {dia_info['estado']}."
                    ),
                )

            result["ok"] = not result["errores"] and result["dias_diferentes"] == 0
            SriClienteSyncService._job_update(
                job_id,
                detalle=__import__("json").dumps(result["detalle_dias"], ensure_ascii=False),
                errores=result["errores"],
                mensaje=(
                    f"Validación de notas de crédito terminada: "
                    f"{result['dias_revisados']} días revisados, "
                    f"{result['dias_diferentes']} con diferencia, "
                    f"{result['guardadas']} notas nuevas guardadas y "
                    f"{len(result['errores'])} errores."
                ),
            )
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
            SriClienteSyncService._cerrar_chrome(chrome_process, profile_dir)
            SriClienteSyncService._eliminar_perfil_chrome(profile_dir)
            if p is not None:
                await p.stop()
