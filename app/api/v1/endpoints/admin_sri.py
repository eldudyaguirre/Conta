from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.security.dependencies import get_admin_user
from app.services.sri_cliente_sync_service import SriClienteSyncService


router = APIRouter(
    prefix="/admin/sri",
    tags=["Administración SRI"],
)


class SincronizarComprasRequest(BaseModel):
    ruc: str = Field(min_length=13, max_length=13, pattern=r"^\d{13}$")
    anio: int = Field(ge=2000, le=2100)
    mes: int = Field(ge=1, le=12)
    tipo_comprobante: int = Field(default=1, ge=1, le=7)



class SincronizarVentasRequest(BaseModel):
    ruc: str = Field(min_length=13, max_length=13, pattern=r"^\d{13}$")
    anio: int = Field(ge=2000, le=2100)
    mes: int = Field(ge=1, le=12)


@router.post("/ventas/sincronizar")
async def sincronizar_ventas(
    request: SincronizarVentasRequest,
    usuario: dict = Depends(get_admin_user),
):
    """Inicia la sincronización de facturas emitidas hacia ventas."""
    try:
        resultado = SriClienteSyncService.iniciar_sincronizacion(
            ruc=request.ruc,
            anio=request.anio,
            mes=request.mes,
            tipo_comprobante=1,
            operacion="ventas",
        )
        return {
            "usuario": usuario["usrname"],
            "tipo": "sri_sync_ventas",
            **resultado,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error iniciando sincronización de ventas: {exc}",
        )

@router.post("/ventas/validar")
async def validar_ventas(
    request: SincronizarVentasRequest,
    usuario: dict = Depends(get_admin_user),
):
    """Valida las ventas emitidas comparando SRI vs BD día por día."""
    try:
        resultado = SriClienteSyncService.iniciar_sincronizacion(
            ruc=request.ruc,
            anio=request.anio,
            mes=request.mes,
            tipo_comprobante=1,
            operacion="ventas_validar",
        )
        return {
            "usuario": usuario["usrname"],
            "tipo": "sri_validar_ventas",
            **resultado,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error iniciando validador de ventas SRI: {exc}",
        )


@router.post("/ventas/notas-credito/sincronizar")
async def sincronizar_notas_credito_emitidas(
    request: SincronizarVentasRequest,
    usuario: dict = Depends(get_admin_user),
):
    """Inicia la sincronización de notas de crédito emitidas hacia ventas."""
    try:
        resultado = SriClienteSyncService.iniciar_sincronizacion(
            ruc=request.ruc,
            anio=request.anio,
            mes=request.mes,
            tipo_comprobante=3,
            operacion="notas_credito_emitidas",
        )
        return {
            "usuario": usuario["usrname"],
            "tipo": "sri_sync_notas_credito_emitidas",
            **resultado,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error iniciando sincronización de notas de crédito emitidas: {exc}",
        )


@router.post("/compras/retenciones/sincronizar")
async def sincronizar_retenciones_recibidas(
    request: SincronizarComprasRequest,
    usuario: dict = Depends(get_admin_user),
):
    """Sincroniza comprobantes de retención recibidos y actualiza ventas."""
    try:
        resultado = SriClienteSyncService.iniciar_sincronizacion(
            ruc=request.ruc,
            anio=request.anio,
            mes=request.mes,
            tipo_comprobante=7,
            operacion="retenciones_recibidas",
        )
        return {
            "usuario": usuario["usrname"],
            "tipo": "sri_sync_retenciones_recibidas",
            **resultado,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error iniciando sincronización de retenciones recibidas: {exc}",
        )


@router.post("/compras/notas-credito/sincronizar")
async def sincronizar_notas_credito_recibidas(
    request: SincronizarComprasRequest,
    usuario: dict = Depends(get_admin_user),
):
    """Inicia la sincronización de notas de crédito recibidas hacia comprasnue."""
    try:
        resultado = SriClienteSyncService.iniciar_sincronizacion(
            ruc=request.ruc,
            anio=request.anio,
            mes=request.mes,
            # En la consulta RECIBIDOS del SRI, 3 corresponde a Nota de Crédito.
            # El 04 se guarda en comprasnue.tipcom, pero no es el valor del combo del SRI.
            tipo_comprobante=3,
            operacion="notas_credito_recibidas",
        )
        return {
            "usuario": usuario["usrname"],
            "tipo": "sri_sync_notas_credito_recibidas",
            **resultado,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error iniciando sincronización de notas de crédito recibidas: {exc}",
        )


@router.post("/compras/sincronizar")
async def sincronizar_compras(
    request: SincronizarComprasRequest,
    usuario: dict = Depends(get_admin_user),
):
    """Inicia la sincronización en segundo plano y responde inmediatamente."""
    try:
        SriClienteSyncService._iva_debug_log(
            "API COMPRAS | solicitud recibida | ruc=%s | anio=%s | mes=%s | tipo=%s",
            request.ruc, request.anio, request.mes, request.tipo_comprobante,
        )
        resultado = SriClienteSyncService.iniciar_sincronizacion(
            ruc=request.ruc,
            anio=request.anio,
            mes=request.mes,
            tipo_comprobante=request.tipo_comprobante,
        )
        SriClienteSyncService._iva_debug_log(
            "API COMPRAS | trabajo creado | resultado=%s",
            resultado,
        )
        return {
            "usuario": usuario["usrname"],
            "tipo": "sri_sync",
            **resultado,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error iniciando sincronización del SRI: {exc}",
        )


@router.post("/cancelar/{job_id}")
async def cancelar_sincronizacion(
    job_id: str,
    usuario: dict = Depends(get_admin_user),
):
    """Solicita detener un trabajo SRI pendiente o en ejecución."""
    resultado = SriClienteSyncService.cancelar_sincronizacion(job_id)
    if resultado is None:
        raise HTTPException(status_code=404, detail="Trabajo de sincronización no encontrado.")
    return {
        "usuario": usuario["usrname"],
        "tipo": "sri_sync_cancelar",
        **resultado,
    }


@router.get("/compras/estado/{job_id}")
async def estado_compras(
    job_id: str,
    usuario: dict = Depends(get_admin_user),
):
    """Consulta el estado de un trabajo SRI sin esperar al SRI."""
    resultado = SriClienteSyncService.estado_sincronizacion(job_id)
    if resultado is None:
        raise HTTPException(status_code=404, detail="Trabajo de sincronización no encontrado.")
    return {
        "usuario": usuario["usrname"],
        "tipo": "sri_sync_status",
        **resultado,
    }
