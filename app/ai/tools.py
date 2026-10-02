import json
from typing import Any

from app.services.tributario_service import TributarioService


TOOL_DEFINITIONS = [
    {
        "type": "function",
        "name": "resumen_compras",
        "description": (
            "Obtiene el resumen tributario de las compras de un cliente "
            "para un año y mes determinados. Usa esta herramienta para "
            "totales de comprobantes, bases, IVA, ICE y retenciones."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "anio": {"type": "integer", "description": "Año tributario."},
                "mes": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 12,
                    "description": "Mes tributario, de 1 a 12.",
                },
            },
            "required": ["anio", "mes"],
            "additionalProperties": False,
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "listar_compras",
        "description": (
            "Obtiene el detalle de comprobantes de compra de un cliente "
            "para un año y mes. tipcom 01=factura, 02=nota de venta, "
            "04=nota de crédito."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "anio": {"type": "integer", "description": "Año tributario."},
                "mes": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 12,
                    "description": "Mes tributario, de 1 a 12.",
                },
                "tipcom": {
                    "type": ["string", "null"],
                    "enum": ["01", "02", "04", None],
                    "description": "Tipo de comprobante o null para todos.",
                },
            },
            "required": ["anio", "mes", "tipcom"],
            "additionalProperties": False,
        },
        "strict": True,
    },
]


class ContaTools:
    def __init__(self, ruc: str):
        self.ruc = ruc

    def resumen_compras(self, anio: int, mes: int) -> dict[str, Any]:
        resultado = TributarioService.resumen_compras(
            ruc=self.ruc,
            anio=anio,
            mes=mes,
        )
        return self._json_safe(resultado)

    def listar_compras(
        self,
        anio: int,
        mes: int,
        tipcom: str | None = None,
    ) -> dict[str, Any]:
        registros = TributarioService.listar_compras(
            ruc=self.ruc,
            anio=anio,
            mes=mes,
            tipcom=tipcom,
        )
        limite = 100
        return {
            "total": len(registros),
            "limitado": len(registros) > limite,
            "registros": self._json_safe(registros[:limite]),
        }

    def ejecutar(self, nombre: str, argumentos: dict[str, Any]) -> dict[str, Any]:
        if nombre == "resumen_compras":
            return self.resumen_compras(
                anio=int(argumentos["anio"]),
                mes=int(argumentos["mes"]),
            )
        if nombre == "listar_compras":
            return self.listar_compras(
                anio=int(argumentos["anio"]),
                mes=int(argumentos["mes"]),
                tipcom=argumentos.get("tipcom"),
            )
        raise ValueError(f"Herramienta no permitida: {nombre}")

    @staticmethod
    def _json_safe(valor: Any) -> Any:
        return json.loads(json.dumps(valor, default=str))
