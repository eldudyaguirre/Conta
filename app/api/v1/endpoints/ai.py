from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.ai.assistant import ContaAssistant
from app.services.cliente_service import ClienteService


router = APIRouter(
    prefix="/ai",
    tags=["Conta AI"],
)


assistant = ContaAssistant()


class ChatRequest(BaseModel):
    ruc: str
    mensaje: str
    anio: int | None = None
    mes: int | None = None


@router.post("/chat")
def chat(request: ChatRequest):

    cliente = ClienteService.obtener_cliente(
        request.ruc
    )

    if not cliente:
        raise HTTPException(
            status_code=404,
            detail="Cliente no encontrado.",
        )

    resultado = assistant.responder(
        pregunta=request.mensaje,
        ruc=request.ruc,
        cliente=cliente["nombre"],
        anio=request.anio,
        mes=request.mes,
    )

    return {
        "ruc": cliente["ruc"],
        "cliente": cliente["nombre"],
        **resultado,
    }