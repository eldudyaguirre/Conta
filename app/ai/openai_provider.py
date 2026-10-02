import json
from typing import Any

from openai import OpenAI

from app.ai.prompts import build_system_prompt
from app.ai.tools import TOOL_DEFINITIONS, ContaTools
from app.core.config import settings


class ContaOpenAIProvider:
    def __init__(self):
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        self.model = settings.OPENAI_MODEL

    def responder(
        self,
        pregunta: str,
        ruc: str,
        cliente: str,
        anio: int | None,
        mes: int | None,
        historial: list[dict[str, str]],
        tools: ContaTools,
        tool_definitions: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        instrucciones = build_system_prompt(
            ruc=ruc,
            cliente=cliente,
            anio=anio,
            mes=mes,
        )

        definitions = tool_definitions or TOOL_DEFINITIONS

        response = self.client.responses.create(
            model=self.model,
            instructions=instrucciones,
            input=[
                *historial,
                {"role": "user", "content": pregunta},
            ],
            tools=definitions,
        )

        tool_calls = 0

        while True:
            calls = [
                item for item in response.output
                if getattr(item, "type", None) == "function_call"
            ]

            if not calls:
                break

            if tool_calls + len(calls) > settings.AI_MAX_TOOL_CALLS:
                raise RuntimeError(
                    "Conta alcanzó el límite de llamadas de herramientas."
                )

            outputs = []

            for call in calls:
                try:
                    argumentos = json.loads(call.arguments)
                    resultado = tools.ejecutar(call.name, argumentos)
                    salida = {"ok": True, "resultado": resultado}
                except Exception as exc:
                    salida = {"ok": False, "error": str(exc)}

                outputs.append({
                    "type": "function_call_output",
                    "call_id": call.call_id,
                    "output": json.dumps(
                        salida,
                        ensure_ascii=False,
                        default=str,
                    ),
                })

            tool_calls += len(calls)

            response = self.client.responses.create(
                model=self.model,
                instructions=instrucciones,
                previous_response_id=response.id,
                input=outputs,
                tools=definitions,
            )

        respuesta = response.output_text.strip()

        if not respuesta:
            raise RuntimeError(
                "El modelo no devolvió una respuesta de texto."
            )

        return {
            "respuesta": respuesta,
            "modelo": self.model,
            "tool_calls": tool_calls,
        }
