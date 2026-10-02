from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.ai.assistant import ContaAssistant
from app.services.cliente_service import ClienteService


router = APIRouter(
    prefix="/ai",
    tags=["Conta AI"],
)

assistant = ContaAssistant()


class ChatRequest(BaseModel):
    ruc: str = Field(min_length=13, max_length=13, pattern=r"^\d{13}$")
    mensaje: str = Field(min_length=1, max_length=4000)
    conversation_id: str | None = Field(default=None, max_length=100)
    anio: int | None = Field(default=None, ge=2000, le=2100)
    mes: int | None = Field(default=None, ge=1, le=12)


class ClearConversationRequest(BaseModel):
    ruc: str = Field(min_length=13, max_length=13, pattern=r"^\d{13}$")
    conversation_id: str = Field(min_length=1, max_length=100)


@router.get("/status")
def ai_status():
    return {
        "app": "Conta",
        "ai_enabled": assistant.ai_disponible,
        "provider": "openai" if assistant.ai_disponible else None,
    }


@router.post("/chat")
def chat(request: ChatRequest):
    cliente = ClienteService.obtener_cliente(request.ruc)

    if not cliente:
        raise HTTPException(
            status_code=404,
            detail="Cliente no encontrado.",
        )

    try:
        resultado = assistant.responder(
            pregunta=request.mensaje,
            ruc=request.ruc,
            cliente=cliente["nombre"],
            conversation_id=request.conversation_id,
            anio=request.anio,
            mes=request.mes,
        )

        return {
            "ruc": cliente["ruc"],
            "cliente": cliente["nombre"],
            **resultado,
        }

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando la consulta de Conta: {exc}",
        )


@router.post("/conversation/clear")
def clear_conversation(request: ClearConversationRequest):
    cliente = ClienteService.obtener_cliente(request.ruc)

    if not cliente:
        raise HTTPException(
            status_code=404,
            detail="Cliente no encontrado.",
        )

    assistant.limpiar_conversacion(
        ruc=request.ruc,
        conversation_id=request.conversation_id,
    )

    return {
        "status": "ok",
        "ruc": request.ruc,
        "conversation_id": request.conversation_id,
    }
