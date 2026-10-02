import json
from typing import Any

from app.services.tributario_service import TributarioService
from app.services.cliente_admin_service import ClienteAdminService


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


TOOL_DEFINITIONS.extend([
    {
        "type": "function",
        "name": "buscar_cliente",
        "description": (
            "Busca clientes administrativos por nombre. Usa esta herramienta "
            "cuando el usuario mencione un cliente por nombre y necesites "
            "identificar su RUC antes de consultar o modificar sus datos."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "nombre": {
                    "type": "string",
                    "description": "Nombre o apellidos del cliente a buscar.",
                },
            },
            "required": ["nombre"],
            "additionalProperties": False,
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "consultar_clave_sri",
        "description": (
            "Consulta la clave SRI del cliente identificado por su RUC. "
            "Es una operación administrativa sensible y solo debe usarse "
            "cuando el usuario solicite expresamente la clave."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "ruc": {
                    "type": "string",
                    "pattern": "^\\d{13}$",
                    "description": "RUC obtenido previamente mediante buscar_cliente.",
                },
            },
            "required": ["ruc"],
            "additionalProperties": False,
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "consultar_estado_cliente",
        "description": (
            "Consulta si un cliente está activo o inactivo."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "ruc": {
                    "type": "string",
                    "pattern": "^\\d{13}$",
                    "description": "RUC del cliente.",
                },
            },
            "required": ["ruc"],
            "additionalProperties": False,
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "activar_cliente",
        "description": (
            "Activa administrativamente un cliente. Usa primero buscar_cliente "
            "si el usuario identificó al cliente por nombre."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "ruc": {
                    "type": "string",
                    "pattern": "^\\d{13}$",
                    "description": "RUC obtenido de buscar_cliente o del contexto administrativo.",
                },
            },
            "required": ["ruc"],
            "additionalProperties": False,
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "desactivar_cliente",
        "description": (
            "Desactiva administrativamente un cliente. Usa primero buscar_cliente "
            "si el usuario identificó al cliente por nombre."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "ruc": {
                    "type": "string",
                    "pattern": "^\\d{13}$",
                    "description": "RUC obtenido de buscar_cliente o del contexto administrativo.",
                },
            },
            "required": ["ruc"],
            "additionalProperties": False,
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "cambiar_clave_sri",
        "description": (
            "Cambia la clave SRI de un cliente. Usa primero buscar_cliente "
            "si el usuario identificó al cliente por nombre. La nueva clave "
            "debe ser exactamente la proporcionada por el usuario."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "ruc": {
                    "type": "string",
                    "pattern": "^\\d{13}$",
                    "description": "RUC obtenido de buscar_cliente.",
                },
                "nueva_clave": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 200,
                    "description": "Nueva clave SRI proporcionada por el usuario.",
                },
            },
            "required": ["ruc", "nueva_clave"],
            "additionalProperties": False,
        },
        "strict": True,
    },
])


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


    def buscar_cliente(self, nombre: str) -> dict[str, Any]:
        resultados = ClienteAdminService.buscar_clientes(nombre)
        return {
            "total": len(resultados),
            "clientes": self._json_safe(resultados),
        }

    def consultar_clave_sri(self, ruc: str) -> dict[str, Any]:
        resultado = ClienteAdminService.consultar_clave_sri(ruc)
        if not resultado:
            raise ValueError("Cliente no encontrado.")
        return self._json_safe(resultado)

    def consultar_estado_cliente(self, ruc: str) -> dict[str, Any]:
        resultado = ClienteAdminService.obtener_cliente(ruc)
        if not resultado:
            raise ValueError("Cliente no encontrado.")
        return self._json_safe(resultado)

    def activar_cliente(self, ruc: str) -> dict[str, Any]:
        resultado = ClienteAdminService.cambiar_estado(ruc, True)
        if not resultado:
            raise ValueError("Cliente no encontrado.")
        return self._json_safe(resultado)

    def desactivar_cliente(self, ruc: str) -> dict[str, Any]:
        resultado = ClienteAdminService.cambiar_estado(ruc, False)
        if not resultado:
            raise ValueError("Cliente no encontrado.")
        return self._json_safe(resultado)

    def cambiar_clave_sri(self, ruc: str, nueva_clave: str) -> dict[str, Any]:
        resultado = ClienteAdminService.cambiar_clave_sri(
            ruc=ruc,
            nueva_clave=nueva_clave,
        )
        if not resultado:
            raise ValueError("Cliente no encontrado.")
        return self._json_safe(resultado)

    def ejecutar(self, nombre: str, argumentos: dict[str, Any]) -> dict[str, Any]:

        if nombre == "buscar_cliente":
            return self.buscar_cliente(
                nombre=str(argumentos["nombre"]),
            )
        if nombre == "consultar_clave_sri":
            return self.consultar_clave_sri(
                ruc=str(argumentos["ruc"]),
            )
        if nombre == "consultar_estado_cliente":
            return self.consultar_estado_cliente(
                ruc=str(argumentos["ruc"]),
            )
        if nombre == "activar_cliente":
            return self.activar_cliente(
                ruc=str(argumentos["ruc"]),
            )
        if nombre == "desactivar_cliente":
            return self.desactivar_cliente(
                ruc=str(argumentos["ruc"]),
            )
        if nombre == "cambiar_clave_sri":
            return self.cambiar_clave_sri(
                ruc=str(argumentos["ruc"]),
                nueva_clave=str(argumentos["nueva_clave"]),
            )
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
