from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4


@dataclass
class ContaContext:
    conversation_id: str
    ruc: str
    cliente: str | None = None
    anio: int | None = None
    mes: int | None = None
    modulo: str | None = None
    ultima_pregunta: str | None = None
    ultima_respuesta: str | None = None
    ultimo_resultado: dict[str, Any] = field(default_factory=dict)
    historial: list[dict[str, str]] = field(default_factory=list)

    def actualizar_periodo(self, anio: int | None = None, mes: int | None = None) -> None:
        if anio is not None:
            self.anio = anio
        if mes is not None:
            self.mes = mes

    def guardar_consulta(
        self,
        pregunta: str,
        respuesta: str,
        modulo: str | None = None,
        resultado: dict[str, Any] | None = None,
    ) -> None:
        self.ultima_pregunta = pregunta
        self.ultima_respuesta = respuesta
        if modulo is not None:
            self.modulo = modulo
        if resultado is not None:
            self.ultimo_resultado = resultado
        self.historial.append({"role": "user", "content": pregunta})
        self.historial.append({"role": "assistant", "content": respuesta})


class ContaContextManager:
    def __init__(self, max_history_messages: int = 20):
        self._contextos: dict[tuple[str, str], ContaContext] = {}
        self.max_history_messages = max_history_messages

    def obtener(
        self,
        ruc: str,
        conversation_id: str | None = None,
        cliente: str | None = None,
    ) -> ContaContext:
        conversation_id = (
            conversation_id.strip()
            if conversation_id and conversation_id.strip()
            else str(uuid4())
        )
        key = (ruc, conversation_id)
        if key not in self._contextos:
            self._contextos[key] = ContaContext(
                conversation_id=conversation_id,
                ruc=ruc,
                cliente=cliente,
            )
        contexto = self._contextos[key]
        if cliente:
            contexto.cliente = cliente
        return contexto

    def guardar(
        self,
        contexto: ContaContext,
        pregunta: str,
        respuesta: str,
        modulo: str | None = None,
        resultado: dict[str, Any] | None = None,
    ) -> None:
        contexto.guardar_consulta(
            pregunta=pregunta,
            respuesta=respuesta,
            modulo=modulo,
            resultado=resultado,
        )
        if len(contexto.historial) > self.max_history_messages:
            contexto.historial = contexto.historial[-self.max_history_messages:]

    def limpiar(self, ruc: str, conversation_id: str) -> None:
        self._contextos.pop((ruc, conversation_id), None)

    @staticmethod
    def serializar(contexto: ContaContext) -> dict[str, Any]:
        return {
            "conversation_id": contexto.conversation_id,
            "ruc": contexto.ruc,
            "cliente": contexto.cliente,
            "anio": contexto.anio,
            "mes": contexto.mes,
            "modulo": contexto.modulo,
            "ultima_pregunta": contexto.ultima_pregunta,
        }
