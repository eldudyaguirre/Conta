from dataclasses import dataclass, field
from typing import Any


@dataclass
class ContaContext:
    ruc: str
    cliente: str | None = None

    anio: int | None = None
    mes: int | None = None

    modulo: str | None = None
    ultima_pregunta: str | None = None

    ultimo_resultado: dict[str, Any] = field(default_factory=dict)

    def actualizar_periodo(
        self,
        anio: int | None = None,
        mes: int | None = None,
    ):
        if anio is not None:
            self.anio = anio

        if mes is not None:
            self.mes = mes

    def guardar_consulta(
        self,
        pregunta: str,
        modulo: str,
        resultado: dict[str, Any],
    ):
        self.ultima_pregunta = pregunta
        self.modulo = modulo
        self.ultimo_resultado = resultado


class ContaContextManager:

    def __init__(self):
        self._contextos: dict[str, ContaContext] = {}

    def obtener(
        self,
        ruc: str,
        cliente: str | None = None,
    ) -> ContaContext:

        if ruc not in self._contextos:
            self._contextos[ruc] = ContaContext(
                ruc=ruc,
                cliente=cliente,
            )

        contexto = self._contextos[ruc]

        if cliente:
            contexto.cliente = cliente

        return contexto

    def limpiar(self, ruc: str):
        self._contextos.pop(ruc, None)