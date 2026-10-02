from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.ai.assistant import ContaAssistant
from app.security.admin_auth import AdminAuthService
from app.security.dependencies import get_admin_user


router = APIRouter(
    prefix="/admin",
    tags=["Administración"],
)

assistant = ContaAssistant()


class AdminLoginRequest(BaseModel):
    usrname: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=1, max_length=200)


class AdminChatRequest(BaseModel):
    mensaje: str = Field(min_length=1, max_length=4000)
    conversation_id: str | None = Field(default=None, max_length=100)


@router.post("/login")
def admin_login(request: AdminLoginRequest):
    usuario = AdminAuthService.autenticar(
        usrname=request.usrname,
        password=request.password,
    )

    if not usuario:
        raise HTTPException(
            status_code=401,
            detail="Usuario o contraseña incorrectos.",
        )

    return {
        "access_token": AdminAuthService.crear_token(usuario),
        "token_type": "bearer",
        "usuario": {
            "usrname": usuario["usrname"],
            "nombre": usuario["nombre"],
        },
    }


@router.get("/me")
def admin_me(usuario: dict = Depends(get_admin_user)):
    return {
        "usuario": usuario,
    }


@router.post("/chat")
def admin_chat(
    request: AdminChatRequest,
    usuario: dict = Depends(get_admin_user),
):
    try:
        resultado = assistant.responder_admin(
            pregunta=request.mensaje,
            usuario=usuario["usrname"],
            conversation_id=request.conversation_id,
        )

        return {
            "usuario": usuario["usrname"],
            **resultado,
        }

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando la consulta administrativa de Conta: {exc}",
        )
