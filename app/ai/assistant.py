from typing import Any

from app.ai.context import ContaContextManager
from app.ai.openai_provider import ContaOpenAIProvider
from app.ai.tools import ContaTools
from app.core.config import settings


class ContaAssistant:
    def __init__(self):
        self.context = ContaContextManager(
            max_history_messages=settings.AI_MAX_HISTORY_MESSAGES
        )
        self.provider = (
            ContaOpenAIProvider()
            if settings.AI_ENABLED and settings.OPENAI_API_KEY
            else None
        )

    @property
    def ai_disponible(self) -> bool:
        return self.provider is not None

    def responder(
        self,
        pregunta: str,
        ruc: str,
        cliente: str | None = None,
        conversation_id: str | None = None,
        anio: int | None = None,
        mes: int | None = None,
    ) -> dict[str, Any]:
        pregunta = pregunta.strip()
        if not pregunta:
            raise ValueError("La pregunta no puede estar vacía.")

        contexto = self.context.obtener(
            ruc=ruc,
            conversation_id=conversation_id,
            cliente=cliente,
        )
        contexto.actualizar_periodo(anio=anio, mes=mes)

        if self.provider is not None:
            resultado = self.provider.responder(
                pregunta=pregunta,
                ruc=ruc,
                cliente=cliente or "",
                anio=contexto.anio,
                mes=contexto.mes,
                historial=contexto.historial,
                tools=ContaTools(ruc),
            )

            self.context.guardar(
                contexto=contexto,
                pregunta=pregunta,
                respuesta=resultado["respuesta"],
            )

            return {
                "tipo": "ai",
                "respuesta": resultado["respuesta"],
                "modelo": resultado["modelo"],
                "tool_calls": resultado["tool_calls"],
                "contexto": self.context.serializar(contexto),
            }

        resultado = self._fallback(
            pregunta=pregunta,
            ruc=ruc,
            anio=contexto.anio,
            mes=contexto.mes,
        )

        self.context.guardar(
            contexto=contexto,
            pregunta=pregunta,
            respuesta=resultado["respuesta"],
            modulo=resultado.get("modulo"),
            resultado=resultado.get("datos"),
        )

        return {
            **resultado,
            "contexto": self.context.serializar(contexto),
            "ai_disponible": False,
        }

    def limpiar_conversacion(self, ruc: str, conversation_id: str) -> None:
        self.context.limpiar(ruc, conversation_id)

    def _fallback(
        self,
        pregunta: str,
        ruc: str,
        anio: int | None,
        mes: int | None,
    ) -> dict[str, Any]:
        pregunta_limpia = pregunta.lower()

        palabras_compras = (
            "compré",
            "compre",
            "compras",
            "comprado",
            "facturas recibidas",
            "facturas de compra",
            "proveedores",
        )

        if not any(
            palabra in pregunta_limpia
            for palabra in palabras_compras
        ):
            return {
                "tipo": "no_entendido",
                "respuesta": (
                    "La IA todavía no está configurada. "
                    "Por ahora puedo consultar compras."
                ),
            }

        if anio is None or mes is None:
            return {
                "tipo": "requiere_periodo",
                "respuesta": (
                    "Necesito saber el año y el mes que deseas consultar."
                ),
            }

        resultado = ContaTools(ruc).resumen_compras(
            anio=anio,
            mes=mes,
        )

        bases = resultado["bases"]
        impuestos = resultado["impuestos"]

        total_bases = sum(
            float(valor or 0)
            for valor in bases.values()
        )
        iva15 = float(impuestos.get("iva_15", 0) or 0)

        respuesta = (
            f"En {mes:02d}/{anio} registraste "
            f"{resultado['total_comprobantes']} comprobantes de compra. "
            "Las bases registradas suman $"
            f"{total_bases:,.2f} y el IVA 15% registrado es $"
            f"{iva15:,.2f}."
        )

        return {
            "tipo": "resumen_compras",
            "respuesta": respuesta,
            "datos": resultado,
            "modulo": "compras",
        }
