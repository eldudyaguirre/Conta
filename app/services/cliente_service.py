from sqlalchemy import text

from app.database.connection import engine


class ClienteService:

    @staticmethod
    def listar_clientes():
        sql = text("""
            SELECT
                ruccedcli,
                nomclient
            FROM clientes
            ORDER BY nomclient
        """)

        with engine.connect() as connection:
            resultado = connection.execute(sql)

            return [
                {
                    "ruc": row.ruccedcli,
                    "nombre": row.nomclient,
                }
                for row in resultado
            ]

    @staticmethod
    def obtener_cliente(ruc: str):
        sql = text("""
            SELECT
                ruccedcli,
                nomclient
            FROM clientes
            WHERE ruccedcli = :ruc
            LIMIT 1
        """)

        with engine.connect() as connection:
            row = connection.execute(
                sql,
                {"ruc": ruc}
            ).mappings().first()

            if not row:
                return None

            return {
                "ruc": row["ruccedcli"],
                "nombre": row["nomclient"],
            }