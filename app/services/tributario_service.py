from decimal import Decimal, InvalidOperation
from datetime import datetime

from sqlalchemy import text

from app.database.session import cliente_session


class TributarioService:

    @staticmethod
    def decimal(valor):
        """
        Convierte valores almacenados como texto
        a Decimal de forma segura.
        """

        if valor is None:
            return Decimal("0")

        valor = str(valor).strip()

        if not valor or valor.upper() == "NULL":
            return Decimal("0")

        try:
            return Decimal(valor.replace(",", "."))
        except (InvalidOperation, ValueError):
            return Decimal("0")

    @staticmethod
    def fecha(valor):
        """
        Convierte fechas almacenadas como texto.
        """

        if valor is None:
            return None

        valor = str(valor).strip()

        if not valor or valor.upper() == "NULL":
            return None

        formatos = (
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
        )

        for formato in formatos:
            try:
                return datetime.strptime(valor, formato).date()
            except ValueError:
                continue

        return None

    @staticmethod
    def listar_compras(
        ruc: str,
        anio: int | None = None,
        mes: int | None = None,
        fecha_desde: str | None = None,
        fecha_hasta: str | None = None,
        tipcom: str | None = None,
    ):

        condiciones = []
        parametros = {}

        if anio is not None:
            condiciones.append(
                'TRIM("año") = :anio'
            )
            parametros["anio"] = str(anio)

        if mes is not None:
            condiciones.append(
                'TRIM(mes) = :mes'
            )
            parametros["mes"] = f"{mes:02d}"

        if tipcom:
            condiciones.append(
                "TRIM(tipcom) = :tipcom"
            )
            parametros["tipcom"] = tipcom

        if fecha_desde:
            condiciones.append(
                """
                TO_DATE(TRIM(fecemi), 'DD/MM/YYYY')
                >= TO_DATE(:fecha_desde, 'YYYY-MM-DD')
                """
            )
            parametros["fecha_desde"] = fecha_desde

        if fecha_hasta:
            condiciones.append(
                """
                TO_DATE(TRIM(fecemi), 'DD/MM/YYYY')
                <= TO_DATE(:fecha_hasta, 'YYYY-MM-DD')
                """
            )
            parametros["fecha_hasta"] = fecha_hasta

        where = ""

        if condiciones:
            where = "WHERE " + " AND ".join(condiciones)

        sql = text(f"""
            SELECT
                numcompra,
                codsus,
                tipid,
                ruccedprovee,
                tipcom,
                fecreg,
                numest,
                numptoemi,
                numsec,
                fecemi,
                numaut,

                baseimpnoobj,
                baseimpiva0,
                baseimpiva5,
                baseimpiva8,
                baseimpiva12,
                baseimpiva14,
                baseimpiva15,
                baseexenta,

                montoice,
                montoiva5,
                montoiva8,
                montoiva12,
                montoiva14,
                montoiva15,

                retencioniva10,
                retencioniva20,
                retencioniva30,
                retencioniva70,
                retencioniva100,

                totbases,

                codret,
                baseimpret,
                porret,
                valret,

                numestret,
                numptoemiret,
                numsecret,
                numautret,
                fecret,

                tipopago,

                codtipodoc,
                numestmod,
                numptoemimod,
                numsecmod,
                numautmod,

                mes,
                "año",

                nomprovee

            FROM comprasnue

            {where}

            ORDER BY
                TO_DATE(
                    NULLIF(TRIM(fecemi), ''),
                    'DD/MM/YYYY'
                ),
                numcompra
        """)

        with cliente_session(ruc) as db:

            resultado = db.execute(
                sql,
                parametros
            )

            compras = []

            for row in resultado.mappings():

                tipocom = str(row["tipcom"] or "").strip()

                if tipocom == "01":
                    tipo_documento = "FACTURA"
                elif tipocom == "02":
                    tipo_documento = "NOTA DE VENTA"
                elif tipocom == "04":
                    tipo_documento = "NOTA DE CREDITO"
                else:
                    tipo_documento = "OTRO"

                compras.append({
                    "numero": row["numcompra"],
                    "tipo_comprobante": tipocom,
                    "tipo_documento": tipo_documento,

                    "fecha": TributarioService.fecha(
                        row["fecemi"]
                    ),

                    "proveedor": {
                        "ruc": row["ruccedprovee"],
                        "nombre": row["nomprovee"],
                        "tipo_identificacion": row["tipid"],
                    },

                    "comprobante": {
                        "establecimiento": row["numest"],
                        "punto_emision": row["numptoemi"],
                        "secuencial": row["numsec"],
                        "autorizacion": row["numaut"],
                    },

                    "bases": {
                        "no_objeto": TributarioService.decimal(
                            row["baseimpnoobj"]
                        ),
                        "iva_0": TributarioService.decimal(
                            row["baseimpiva0"]
                        ),
                        "iva_5": TributarioService.decimal(
                            row["baseimpiva5"]
                        ),
                        "iva_8": TributarioService.decimal(
                            row["baseimpiva8"]
                        ),
                        "iva_12": TributarioService.decimal(
                            row["baseimpiva12"]
                        ),
                        "iva_14": TributarioService.decimal(
                            row["baseimpiva14"]
                        ),
                        "iva_15": TributarioService.decimal(
                            row["baseimpiva15"]
                        ),
                        "exenta": TributarioService.decimal(
                            row["baseexenta"]
                        ),
                    },

                    "impuestos": {
                        "ice": TributarioService.decimal(
                            row["montoice"]
                        ),
                        "iva_5": TributarioService.decimal(
                            row["montoiva5"]
                        ),
                        "iva_8": TributarioService.decimal(
                            row["montoiva8"]
                        ),
                        "iva_12": TributarioService.decimal(
                            row["montoiva12"]
                        ),
                        "iva_14": TributarioService.decimal(
                            row["montoiva14"]
                        ),
                        "iva_15": TributarioService.decimal(
                            row["montoiva15"]
                        ),
                    },

                    "retenciones": {
                        "iva_10": TributarioService.decimal(
                            row["retencioniva10"]
                        ),
                        "iva_20": TributarioService.decimal(
                            row["retencioniva20"]
                        ),
                        "iva_30": TributarioService.decimal(
                            row["retencioniva30"]
                        ),
                        "iva_70": TributarioService.decimal(
                            row["retencioniva70"]
                        ),
                        "iva_100": TributarioService.decimal(
                            row["retencioniva100"]
                        ),
                        "renta": TributarioService.decimal(
                            row["valret"]
                        ),
                    },

                    "retencion_renta": {
                        "codigo": row["codret"],
                        "base": TributarioService.decimal(
                            row["baseimpret"]
                        ),
                        "porcentaje": TributarioService.decimal(
                            row["porret"]
                        ),
                        "valor": TributarioService.decimal(
                            row["valret"]
                        ),
                    },

                    "retencion": {
                        "establecimiento": row["numestret"],
                        "punto_emision": row["numptoemiret"],
                        "secuencial": row["numsecret"],
                        "autorizacion": row["numautret"],
                        "fecha": TributarioService.fecha(
                            row["fecret"]
                        ),
                    },

                    "forma_pago": row["tipopago"],

                    "documento_modificado": {
                        "tipo": row["codtipodoc"],
                        "establecimiento": row["numestmod"],
                        "punto_emision": row["numptoemimod"],
                        "secuencial": row["numsecmod"],
                        "autorizacion": row["numautmod"],
                    },

                    "periodo": {
                        "mes": row["mes"],
                        "anio": row["año"],
                    },
                })

            return compras

    @staticmethod
    def resumen_compras(
        ruc: str,
        anio: int,
        mes: int | None = None,
    ):

        condiciones = [
            'TRIM("año") = :anio'
        ]

        parametros = {
            "anio": str(anio)
        }

        if mes is not None:
            condiciones.append(
                'TRIM(mes) = :mes'
            )

            parametros["mes"] = f"{mes:02d}"

        where = " AND ".join(condiciones)

        sql = text(f"""
            SELECT

                COUNT(*) AS total_comprobantes,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(baseimpnoobj), '') AS NUMERIC)
                ), 0) AS base_no_objeto,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(baseimpiva0), '') AS NUMERIC)
                ), 0) AS base_iva_0,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(baseimpiva5), '') AS NUMERIC)
                ), 0) AS base_iva_5,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(baseimpiva8), '') AS NUMERIC)
                ), 0) AS base_iva_8,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(baseimpiva12), '') AS NUMERIC)
                ), 0) AS base_iva_12,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(baseimpiva14), '') AS NUMERIC)
                ), 0) AS base_iva_14,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(baseimpiva15), '') AS NUMERIC)
                ), 0) AS base_iva_15,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(baseexenta), '') AS NUMERIC)
                ), 0) AS base_exenta,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(montoice), '') AS NUMERIC)
                ), 0) AS ice,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(montoiva5), '') AS NUMERIC)
                ), 0) AS iva_5,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(montoiva8), '') AS NUMERIC)
                ), 0) AS iva_8,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(montoiva12), '') AS NUMERIC)
                ), 0) AS iva_12,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(montoiva14), '') AS NUMERIC)
                ), 0) AS iva_14,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(montoiva15), '') AS NUMERIC)
                ), 0) AS iva_15,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(retencioniva10), '') AS NUMERIC)
                ), 0) AS retencion_iva_10,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(retencioniva20), '') AS NUMERIC)
                ), 0) AS retencion_iva_20,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(retencioniva30), '') AS NUMERIC)
                ), 0) AS retencion_iva_30,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(retencioniva70), '') AS NUMERIC)
                ), 0) AS retencion_iva_70,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(retencioniva100), '') AS NUMERIC)
                ), 0) AS retencion_iva_100,

                COALESCE(SUM(
                    CAST(NULLIF(TRIM(valret), '') AS NUMERIC)
                ), 0) AS retencion_renta

            FROM comprasnue

            WHERE {where}
        """)

        with cliente_session(ruc) as db:

            row = db.execute(
                sql,
                parametros
            ).mappings().first()

            return {
                "total_comprobantes": row["total_comprobantes"],

                "bases": {
                    "no_objeto": TributarioService.decimal(
                        row["base_no_objeto"]
                    ),
                    "iva_0": TributarioService.decimal(
                        row["base_iva_0"]
                    ),
                    "iva_5": TributarioService.decimal(
                        row["base_iva_5"]
                    ),
                    "iva_8": TributarioService.decimal(
                        row["base_iva_8"]
                    ),
                    "iva_12": TributarioService.decimal(
                        row["base_iva_12"]
                    ),
                    "iva_14": TributarioService.decimal(
                        row["base_iva_14"]
                    ),
                    "iva_15": TributarioService.decimal(
                        row["base_iva_15"]
                    ),
                    "exenta": TributarioService.decimal(
                        row["base_exenta"]
                    ),
                },

                "impuestos": {
                    "ice": TributarioService.decimal(
                        row["ice"]
                    ),
                    "iva_5": TributarioService.decimal(
                        row["iva_5"]
                    ),
                    "iva_8": TributarioService.decimal(
                        row["iva_8"]
                    ),
                    "iva_12": TributarioService.decimal(
                        row["iva_12"]
                    ),
                    "iva_14": TributarioService.decimal(
                        row["iva_14"]
                    ),
                    "iva_15": TributarioService.decimal(
                        row["iva_15"]
                    ),
                },

                "retenciones": {
                    "iva_10": TributarioService.decimal(
                        row["retencion_iva_10"]
                    ),
                    "iva_20": TributarioService.decimal(
                        row["retencion_iva_20"]
                    ),
                    "iva_30": TributarioService.decimal(
                        row["retencion_iva_30"]
                    ),
                    "iva_70": TributarioService.decimal(
                        row["retencion_iva_70"]
                    ),
                    "iva_100": TributarioService.decimal(
                        row["retencion_iva_100"]
                    ),
                    "renta": TributarioService.decimal(
                        row["retencion_renta"]
                    ),
                },

                "periodo": {
                    "anio": anio,
                    "mes": mes,
                },
            }        