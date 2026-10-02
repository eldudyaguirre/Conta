import json
from typing import Any

from google import genai
from google.genai import types

from app.ai.prompts import build_system_prompt
from app.ai.tools import TOOL_DEFINITIONS, ContaTools
from app.core.config import settings


class ContaGeminiProvider:
    def __init__(self):
        self.client = genai.Client(api_key=settings.GEMINI_API_KEY)
        self.model = settings.GEMINI_MODEL

    def _build_tool(self, definitions):
        function_declarations = [
            types.FunctionDeclaration(
                name=definition["name"],
                description=definition.get("description", ""),
                parameters_json_schema=definition["parameters"],
            )
            for definition in definitions
        ]
        return types.Tool(function_declarations=function_declarations)

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
        tool = self._build_tool(definitions)

        contents: list[Any] = []
        for mensaje in historial:
            role = "model" if mensaje["role"] == "assistant" else "user"
            contents.append(
                types.Content(
                    role=role,
                    parts=[types.Part.from_text(text=mensaje["content"])],
                )
            )

        contents.append(
            types.Content(
                role="user",
                parts=[types.Part.from_text(text=pregunta)],
            )
        )

        tool_calls = 0

        while True:
            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=instrucciones,
                    tools=[tool],
                ),
            )

            calls = list(response.function_calls or [])
            if not calls:
                break

            if tool_calls + len(calls) > settings.AI_MAX_TOOL_CALLS:
                raise RuntimeError(
                    "Conta alcanzó el límite de llamadas de herramientas."
                )

            if not response.candidates:
                raise RuntimeError(
                    "Gemini no devolvió candidatos para procesar la herramienta."
                )

            contents.append(response.candidates[0].content)

            function_parts = []
            for call in calls:
                try:
                    argumentos = dict(call.args or {})
                    resultado = tools.ejecutar(call.name, argumentos)
                    salida = {"ok": True, "resultado": resultado}
                except Exception as exc:
                    salida = {"ok": False, "error": str(exc)}

                function_parts.append(
                    types.Part.from_function_response(
                        name=call.name,
                        response=salida,
                    )
                )

            contents.append(
                types.Content(
                    role="user",
                    parts=function_parts,
                )
            )
            tool_calls += len(calls)

        respuesta = (response.text or "").strip()
        if not respuesta:
            raise RuntimeError(
                "Gemini no devolvió una respuesta de texto."
            )

        return {
            "respuesta": respuesta,
            "modelo": self.model,
            "tool_calls": tool_calls,
        }
