from sqlalchemy import text

from app.database.session import cliente_session


class BaseClienteService:

    @staticmethod
    def listar_tablas(ruc: str):

        sql = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)

        with cliente_session(ruc) as db:

            resultado = db.execute(sql)

            return [
                row[0]
                for row in resultado
            ]

    @staticmethod
    def estructura_tabla(ruc: str, tabla: str):

        sql = text("""
            SELECT
                column_name,
                data_type,
                is_nullable,
                character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = :tabla
            ORDER BY ordinal_position
        """)

        with cliente_session(ruc) as db:

            resultado = db.execute(
                sql,
                {"tabla": tabla}
            )

            return [
                {
                    "columna": row.column_name,
                    "tipo": row.data_type,
                    "nullable": row.is_nullable,
                    "longitud": row.character_maximum_length,
                }
                for row in resultado
            ]