from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.security.admin_auth import AdminAuthService


_bearer = HTTPBearer(auto_error=False)


def get_admin_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> dict:
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
