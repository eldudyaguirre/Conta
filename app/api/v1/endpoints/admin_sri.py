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

@router.post("/compras/sincronizar")
async def sincronizar_compras(
    request: SincronizarComprasRequest,
    usuario: dict = Depends(get_admin_user),
):
    """Inicia la sincronización en segundo plano y responde inmediatamente."""
    try:
        resultado = SriClienteSyncService.iniciar_sincronizacion(
            ruc=request.ruc,
            anio=request.anio,
            mes=request.mes,
            tipo_comprobante=request.tipo_comprobante,
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
