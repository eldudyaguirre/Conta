from fastapi import Depends, Header, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.config import settings
from app.security.admin_auth import AdminAuthService


_bearer = HTTPBearer(auto_error=False)


def get_admin_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
    internal_token: str | None = Header(default=None, alias="X-TotalCounts-Internal"),
    internal_user: str | None = Header(default=None, alias="X-TotalCounts-User"),
) -> dict:
    """Autenticación administrativa.

    En producción TotalCounts se autentica en Django y llama a Conta mediante
    un secreto interno que nunca llega al navegador. Se conserva Bearer para
    compatibilidad y diagnóstico local.
    """
    if (
        settings.TOTALCOUNTS_INTERNAL_TOKEN
        and internal_token
        and internal_token == settings.TOTALCOUNTS_INTERNAL_TOKEN
    ):
        usuario = (internal_user or "ADMIN").strip()
        return {"usrname": usuario, "nombre": usuario}

    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=401,
            detail="Autenticación administrativa requerida.",
        )

    usuario = AdminAuthService.validar_token(credentials.credentials)

    if not usuario:
        raise HTTPException(
            status_code=401,
            detail="Sesión administrativa inválida o expirada.",
        )

    return usuario
