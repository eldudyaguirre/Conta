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
    rucs: list[str] = Field(
        min_length=1,
        max_length=5,
        description="Entre 1 y 5 RUC seleccionados explícitamente para la prueba.",
    )
    anio: int = Field(ge=2000, le=2100)
    mes: int = Field(ge=1, le=12)
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
    confirmar_ejecucion_real: bool = False


@router.post("/prueba/preview")
def previsualizar_worker_pool(
    request: PruebaWorkerPoolRequest,
    usuario: dict = Depends(get_admin_user),
):
    """Valida RUC y credenciales sin crear trabajos ni abrir Chromium."""
    try:
        resultado = SriWorkerPoolService.previsualizar(
            rucs=request.rucs,
            anio=request.anio,
            mes=request.mes,
            tipo_comprobante=request.tipo_comprobante,
            operacion=request.operacion,
        )
        return {
            "usuario": usuario["usrname"],
            "tipo": "sri_worker_pool_preview",
            **resultado,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error validando la previsualización del worker pool: {exc}",
        )


@router.post("/prueba")
def iniciar_prueba_worker_pool(
    request: PruebaWorkerPoolRequest,
    usuario: dict = Depends(get_admin_user),
):
    """Encola trabajos reales solo después de confirmar expresamente."""
    if not request.confirmar_ejecucion_real:
        raise HTTPException(
            status_code=400,
            detail=(
                "No se creó ningún trabajo. Primero usa /prueba/preview y, "
                "cuando confirmes los RUC, envía confirmar_ejecucion_real=true."
            ),
        )
    try:
        resultado = SriWorkerPoolService.iniciar_prueba(
            rucs=request.rucs,
            anio=request.anio,
            mes=request.mes,
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
