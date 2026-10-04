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


@router.post("/compras/sincronizar")
async def sincronizar_compras(
    request: SincronizarComprasRequest,
    usuario: dict = Depends(get_admin_user),
):
    """Primera fase: sincroniza un solo cliente y período."""
    try:
        resultado = await SriClienteSyncService.sincronizar_mes(
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
            detail=f"Error sincronizando comprobantes del SRI: {exc}",
        )
