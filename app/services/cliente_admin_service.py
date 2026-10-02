import unicodedata
from typing import Any

from sqlalchemy import text

from app.database.connection import engine


class ClienteAdminService:

    @staticmethod
    def buscar_clientes(nombre: str) -> list[dict[str, Any]]:
        termino = " ".join(nombre.strip().split())

        if not termino:
            return []

        sql = text("""
            SELECT
                ruccedcli,
                nomclient,
                activo
            FROM clientes
            ORDER BY nomclient
        """)

        tokens = ClienteAdminService._normalizar(termino).split()

        with engine.connect() as connection:
            rows = connection.execute(sql).mappings().all()

        resultados = []

        for row in rows:
            nombre_cliente = str(row["nomclient"] or "")
            nombre_normalizado = ClienteAdminService._normalizar(nombre_cliente)

            if all(token in nombre_normalizado.split() for token in tokens):
                resultados.append({
                    "ruc": row["ruccedcli"],
                    "nombre": nombre_cliente,
                    "activo": bool(row["activo"]),
                })

        return resultados[:20]

    @staticmethod
    def obtener_cliente(ruc: str) -> dict[str, Any] | None:
        sql = text("""
            SELECT
                ruccedcli,
                nomclient,
                activo
            FROM clientes
            WHERE ruccedcli = :ruc
            LIMIT 1
        """)

        with engine.connect() as connection:
            row = connection.execute(sql, {"ruc": ruc}).mappings().first()

        if not row:
            return None

        return {
            "ruc": row["ruccedcli"],
            "nombre": row["nomclient"],
            "activo": bool(row["activo"]),
        }

    @staticmethod
    def consultar_clave_sri(ruc: str) -> dict[str, Any] | None:
        sql = text("""
            SELECT
                ruccedcli,
                nomclient,
                activo,
                clavesri
            FROM clientes
            WHERE ruccedcli = :ruc
            LIMIT 1
        """)

        with engine.connect() as connection:
            row = connection.execute(sql, {"ruc": ruc}).mappings().first()

        if not row:
            return None

        return {
            "ruc": row["ruccedcli"],
            "nombre": row["nomclient"],
            "activo": bool(row["activo"]),
            "clave_sri": row["clavesri"] or "",
        }

    @staticmethod
    def cambiar_estado(ruc: str, activo: bool) -> dict[str, Any] | None:
        sql = text("""
            UPDATE clientes
            SET activo = :activo
            WHERE ruccedcli = :ruc
            RETURNING
                ruccedcli,
                nomclient,
                activo
        """)

        with engine.begin() as connection:
            row = connection.execute(
                sql,
                {
                    "ruc": ruc,
                    "activo": activo,
                },
            ).mappings().first()

        if not row:
            return None

        return {
            "ruc": row["ruccedcli"],
            "nombre": row["nomclient"],
            "activo": bool(row["activo"]),
        }

    @staticmethod
    def cambiar_clave_sri(
        ruc: str,
        nueva_clave: str,
    ) -> dict[str, Any] | None:
        sql = text("""
            UPDATE clientes
            SET clavesri = :clave
            WHERE ruccedcli = :ruc
            RETURNING
                ruccedcli,
                nomclient,
                activo
        """)

        with engine.begin() as connection:
            row = connection.execute(
                sql,
                {
                    "ruc": ruc,
                    "clave": nueva_clave,
                },
            ).mappings().first()

        if not row:
            return None

        return {
            "ruc": row["ruccedcli"],
            "nombre": row["nomclient"],
            "activo": bool(row["activo"]),
            "clave_actualizada": True,
        }

    @staticmethod
    def _normalizar(valor: str) -> str:
        valor = unicodedata.normalize("NFKD", valor)
        valor = "".join(
            caracter
            for caracter in valor
            if not unicodedata.combining(caracter)
        )
        return " ".join(valor.lower().split())
