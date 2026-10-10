from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.security.dependencies import get_admin_user
from app.services.sri_worker_pool_service import SriWorkerPoolService


router = APIRouter(
    prefix="/admin/sri/worker-pool",
    tags=["Worker Pool SRI"],
)


class PruebaWorkerPoolRequest(BaseModel):
    anio: int = Field(ge=2000, le=2100)
    mes: int = Field(ge=1, le=12)
    cantidad: int = Field(default=5, ge=1, le=10)
    tipo_comprobante: int = Field(default=1, ge=1, le=7)
    operacion: Literal[
        "compras",
        "notas_credito_recibidas",
        "retenciones_recibidas",
        "ventas",
        "notas_credito_emitidas",
        "retenciones_emitidas",
        "ventas_validar",
    ] = "compras"


@router.post("/prueba")
def iniciar_prueba_worker_pool(
    request: PruebaWorkerPoolRequest,
    usuario: dict = Depends(get_admin_user),
):
    """Encola una prueba controlada con clientes activos distintos."""
    try:
        resultado = SriWorkerPoolService.iniciar_prueba(
            anio=request.anio,
            mes=request.mes,
            cantidad=request.cantidad,
            tipo_comprobante=request.tipo_comprobante,
            operacion=request.operacion,
        )
        return {
            "usuario": usuario["usrname"],
            "tipo": "sri_worker_pool_prueba",
            **resultado,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error iniciando la prueba del worker pool: {exc}",
        )


@router.get("/prueba/{grupo_id}")
def estado_prueba_worker_pool(
    grupo_id: str,
    usuario: dict = Depends(get_admin_user),
):
    """Devuelve progreso agregado y detalle por cada trabajo del grupo."""
    resultado = SriWorkerPoolService.estado_prueba(grupo_id)
    if resultado is None:
        raise HTTPException(
            status_code=404,
            detail="No existe un grupo de prueba con ese identificador.",
        )
    return {
        "usuario": usuario["usrname"],
        "tipo": "sri_worker_pool_estado",
        **resultado,
    }


@router.get("/workers")
def estado_workers_sri(
    usuario: dict = Depends(get_admin_user),
):
    """Lista workers registrados, heartbeat y trabajo actual."""
    return {
        "usuario": usuario["usrname"],
        "tipo": "sri_worker_pool_workers",
        **SriWorkerPoolService.estado_workers(),
    }
