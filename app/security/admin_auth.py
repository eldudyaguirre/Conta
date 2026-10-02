import base64
import hashlib
import hmac
import json
import secrets
import time
from typing import Any

from sqlalchemy import text

from app.database.connection import engine


class AdminAuthService:
    """Autenticación de usuarios administrativos de TotalCounts."""

    _table_name = "seguridad"
    _token_ttl = 8 * 60 * 60
    _secret = secrets.token_bytes(32)

    @classmethod
    def autenticar(cls, usrname: str, password: str) -> dict[str, Any] | None:
        usrname = usrname.strip()

        if not usrname or not password:
            return None

        sql = text(f"""
            SELECT
                usrname,
                nomusuari,
                conusuari
            FROM {cls._table_name}
            WHERE usrname = :usrname
            LIMIT 1
        """)

        with engine.connect() as connection:
            row = connection.execute(
                sql,
                {"usrname": usrname},
            ).mappings().first()

        if not row:
            return None

        password_db = str(row["conusuari"] or "")

        # La tabla existente de TotalCounts contiene la credencial en
        # conusuari. No se devuelve ni se registra ese valor.
        if not hmac.compare_digest(password_db, password):
            return None

        return {
            "usrname": str(row["usrname"]),
            "nombre": str(row["nomusuari"] or row["usrname"]),
        }

    @classmethod
    def crear_token(cls, usuario: dict[str, Any]) -> str:
        payload = {
            "usrname": usuario["usrname"],
            "nombre": usuario["nombre"],
            "exp": int(time.time()) + cls._token_ttl,
        }

        raw = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        encoded = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
        signature = hmac.new(
            cls._secret,
            encoded.encode("ascii"),
            hashlib.sha256,
        ).digest()
        signature_encoded = base64.urlsafe_b64encode(signature).decode("ascii").rstrip("=")

        return f"{encoded}.{signature_encoded}"

    @classmethod
    def validar_token(cls, token: str) -> dict[str, Any] | None:
        try:
            encoded, signature = token.split(".", 1)

            expected = hmac.new(
                cls._secret,
                encoded.encode("ascii"),
                hashlib.sha256,
            ).digest()

            received = base64.urlsafe_b64decode(
                signature + "=" * (-len(signature) % 4)
            )

            if not hmac.compare_digest(expected, received):
                return None

            payload = json.loads(
                base64.urlsafe_b64decode(
                    encoded + "=" * (-len(encoded) % 4)
                ).decode("utf-8")
            )

            if int(payload.get("exp", 0)) < int(time.time()):
                return None

            if not payload.get("usrname"):
                return None

            return {
                "usrname": str(payload["usrname"]),
                "nombre": str(payload.get("nombre") or payload["usrname"]),
            }

        except Exception:
            return None
