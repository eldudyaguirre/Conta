from app.ai.context import ContaContextManager
from app.ai.tools import ContaTools


class ContaAssistant:

    def __init__(self):
        self.tools = ContaTools()
        self.context = ContaContextManager()

    def responder(
        self,
        pregunta: str,
        ruc: str,
        cliente: str | None = None,
        anio: int | None = None,
        mes: int | None = None,
    ):

        contexto = self.context.obtener(
            ruc=ruc,
            cliente=cliente,
        )

        # -------------------------------------------------
        # ACTUALIZAR CONTEXTO
        # -------------------------------------------------

        contexto.actualizar_periodo(
            anio=anio,
            mes=mes,
        )

        contexto.ultima_pregunta = pregunta

        pregunta_limpia = pregunta.strip().lower()

        # -------------------------------------------------
        # DETECTAR COMPRAS
        # -------------------------------------------------

        palabras_compras = [
            "compré",
            "compre",
            "compras",
            "comprado",
            "facturas recibidas",
            "facturas de compra",
            "proveedores",
        ]

        es_compra = any(
            palabra in pregunta_limpia
            for palabra in palabras_compras
        )

        # -------------------------------------------------
        # CONSULTA RELACIONADA CON LA CONSULTA ANTERIOR
        # -------------------------------------------------

        palabras_contexto = [
            "y cuánto",
            "y cuanto",
            "y el iva",
            "y las retenciones",
            "de esas compras",
            "de esas",
            "eso",
            "esas compras",
        ]

        usa_contexto = any(
            palabra in pregunta_limpia
            for palabra in palabras_contexto
        )

        if usa_contexto and contexto.modulo == "compras":
            es_compra = True

        # -------------------------------------------------
        # COMPRAS
        # -------------------------------------------------

        if es_compra:

            if contexto.anio is None or contexto.mes is None:
                return {
                    "tipo": "requiere_periodo",
                    "respuesta": (
                        "Necesito saber el año y el mes "
                        "que deseas consultar."
                    ),
                    "contexto": self._contexto_json(contexto),
                }

            resultado = self.tools.resumen_compras(
                ruc=ruc,
                anio=contexto.anio,
                mes=contexto.mes,
            )

            contexto.guardar_consulta(
                pregunta=pregunta,
                modulo="compras",
                resultado=resultado,
            )

            respuesta = self._responder_compras(
                pregunta=pregunta_limpia,
                resultado=resultado,
            )

            return {
                "tipo": "resumen_compras",
                "respuesta": respuesta,
                "datos": resultado,
                "contexto": self._contexto_json(contexto),
            }

        # -------------------------------------------------
        # NO ENTENDIDO
        # -------------------------------------------------

        return {
            "tipo": "no_entendido",
            "respuesta": (
                "Todavía no tengo una herramienta para "
                "responder esa consulta."
            ),
            "contexto": self._contexto_json(contexto),
        }

    # =====================================================
    # RESPUESTA DE COMPRAS
    # =====================================================

    @staticmethod
    def _responder_compras(
        pregunta: str,
        resultado: dict,
    ):

        total = resultado.get(
            "total_comprobantes",
            0,
        )

        bases = resultado.get(
            "bases",
            {},
        )

        impuestos = resultado.get(
            "impuestos",
            {},
        )

        retenciones = resultado.get(
            "retenciones",
            {},
        )

        periodo = resultado.get(
            "periodo",
            {},
        )

        anio = periodo.get("anio")
        mes = periodo.get("mes")

        # ---------------------------------------------
        # IVA
        # ---------------------------------------------

        if (
            "iva" in pregunta
            and "15" in pregunta
        ):
            base = float(
                bases.get("iva_15", 0) or 0
            )

            iva = float(
                impuestos.get("iva_15", 0) or 0
            )

            return (
                f"De las compras de {mes:02d}/{anio}, "
                f"la base gravada con IVA 15% fue de "
                f"${base:,.2f} y el IVA registrado fue "
                f"de ${iva:,.2f}."
            )

        # ---------------------------------------------
        # RETENCIONES
        # ---------------------------------------------

        if "retencion" in pregunta:

            renta = float(
                retenciones.get("renta", 0) or 0
            )

            iva10 = float(
                retenciones.get("iva_10", 0) or 0
            )

            iva20 = float(
                retenciones.get("iva_20", 0) or 0
            )

            iva30 = float(
                retenciones.get("iva_30", 0) or 0
            )

            iva70 = float(
                retenciones.get("iva_70", 0) or 0
            )

            iva100 = float(
                retenciones.get("iva_100", 0) or 0
            )

            total_iva = (
                iva10
                + iva20
                + iva30
                + iva70
                + iva100
            )

            return (
                f"En {mes:02d}/{anio}, las retenciones "
                f"de IVA suman ${total_iva:,.2f} y las "
                f"retenciones de renta suman "
                f"${renta:,.2f}."
            )

        # ---------------------------------------------
        # TOTAL COMPRAS
        # ---------------------------------------------

        total_bases = sum(
            float(valor or 0)
            for valor in bases.values()
        )

        iva15 = float(
            impuestos.get("iva_15", 0) or 0
        )

        return (
            f"En {mes:02d}/{anio} registraste "
            f"{total} comprobantes de compra. "
            f"Las bases registradas suman "
            f"${total_bases:,.2f} y el IVA registrado "
            f"es de ${iva15:,.2f}."
        )

    # =====================================================
    # CONTEXTO
    # =====================================================

    @staticmethod
    def _contexto_json(contexto):

        return {
            "ruc": contexto.ruc,
            "cliente": contexto.cliente,
            "anio": contexto.anio,
            "mes": contexto.mes,
            "modulo": contexto.modulo,
            "ultima_pregunta": contexto.ultima_pregunta,
        }