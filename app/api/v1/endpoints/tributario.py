from fastapi import APIRouter, HTTPException, Query

from app.services.cliente_service import ClienteService
from app.services.tributario_service import TributarioService


router = APIRouter(
    prefix="/tributario",
    tags=["Tributario"],
)


@router.get("/{ruc}/compras")
def listar_compras(
    ruc: str,
    anio: int | None = Query(default=None),
    mes: int | None = Query(default=None, ge=1, le=12),
    fecha_desde: str | None = Query(default=None),
    fecha_hasta: str | None = Query(default=None),
    tipcom: str | None = Query(default=None),
):

    cliente = ClienteService.obtener_cliente(ruc)

    if not cliente:
        raise HTTPException(
            status_code=404,
            detail="Cliente no encontrado en BdTotal"
        )

    try:

        compras = TributarioService.listar_compras(
            ruc=ruc,
            anio=anio,
            mes=mes,
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            tipcom=tipcom,
        )

        return {
            "ruc": ruc,
            "cliente": cliente["nombre"],
            "base_datos": ruc,
            "total": len(compras),
            "compras": compras,
        }

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=f"No se pudieron consultar las compras: {str(e)}"
        )

@router.get("/{ruc}/compras/resumen")
def resumen_compras(
    ruc: str,
    anio: int = Query(...),
    mes: int | None = Query(default=None, ge=1, le=12),
):

    cliente = ClienteService.obtener_cliente(ruc)

    if not cliente:
        raise HTTPException(
            status_code=404,
            detail="Cliente no encontrado en BdTotal"
        )

    try:

        resumen = TributarioService.resumen_compras(
            ruc=ruc,
            anio=anio,
            mes=mes,
        )

        return {
            "ruc": ruc,
            "cliente": cliente["nombre"],
            "base_datos": ruc,
            **resumen,
        }

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=f"No se pudo generar el resumen de compras: {str(e)}"
        )    