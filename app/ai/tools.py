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
        "name": "resumen_ventas",
        "description": (
            "Obtiene el resumen tributario de las ventas y comprobantes "
            "emitidos por un cliente para un año y mes determinados. "
            "Usa esta herramienta para totales, bases, IVA, ICE y retenciones."
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
        "name": "listar_ventas",
        "description": (
            "Obtiene el detalle de comprobantes de venta emitidos por un "
            "cliente para un año y mes."
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
        "name": "resumen_notas_credito",
        "description": (
            "Obtiene el resumen de notas de crédito de ventas emitidas por "
            "un cliente para un año y mes determinados. Usa esta herramienta "
            "para cantidad, bases, IVA e ICE de las notas de crédito."
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
        "name": "listar_notas_credito",
        "description": (
            "Obtiene el detalle de notas de crédito de ventas emitidas por "
            "un cliente para un año y mes, incluyendo la factura modificada."
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

    def resumen_ventas(self, anio: int, mes: int) -> dict[str, Any]:
        resultado = TributarioService.resumen_ventas(
            ruc=self.ruc,
            anio=anio,
            mes=mes,
        )
        return self._json_safe(resultado)

    def listar_ventas(self, anio: int, mes: int) -> dict[str, Any]:
        registros = TributarioService.listar_ventas(
            ruc=self.ruc,
            anio=anio,
            mes=mes,
        )
        limite = 100
        return {
            "total": len(registros),
            "limitado": len(registros) > limite,
            "registros": self._json_safe(registros[:limite]),
        }

    def resumen_notas_credito(self, anio: int, mes: int) -> dict[str, Any]:
        resultado = TributarioService.resumen_notas_credito(
            ruc=self.ruc,
            anio=anio,
            mes=mes,
        )
        return self._json_safe(resultado)

    def listar_notas_credito(self, anio: int, mes: int) -> dict[str, Any]:
        registros = TributarioService.listar_notas_credito(
            ruc=self.ruc,
            anio=anio,
            mes=mes,
        )
        limite = 100
        return {
            "total": len(registros),
            "limitado": len(registros) > limite,
            "registros": self._json_safe(registros[:limite]),
        }

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
        if nombre == "resumen_ventas":
            return self.resumen_ventas(
                anio=int(argumentos["anio"]),
                mes=int(argumentos["mes"]),
            )
        if nombre == "listar_ventas":
            return self.listar_ventas(
                anio=int(argumentos["anio"]),
                mes=int(argumentos["mes"]),
            )
        if nombre == "resumen_notas_credito":
            return self.resumen_notas_credito(
                anio=int(argumentos["anio"]),
                mes=int(argumentos["mes"]),
            )
        if nombre == "listar_notas_credito":
            return self.listar_notas_credito(
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
