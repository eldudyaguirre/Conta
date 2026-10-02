# Conta

Asistente tributario de TotalCounts.

## Arquitectura

Conta usa una base central `BdTotal` para localizar clientes y se conecta a la
base PostgreSQL de cada cliente usando su RUC como nombre de base de datos.

La capa de IA no ejecuta SQL. El modelo utiliza herramientas controladas que
llaman a los servicios tributarios de Conta.

## Estructura

```
app/
├── ai/
│   ├── assistant.py
│   ├── context.py
│   ├── openai_provider.py
│   ├── prompts.py
│   └── tools.py
├── api/
├── core/
├── database/
└── services/
```

## Configuración

Copia `.env.example` como `.env` y completa:

- conexión a PostgreSQL;
- `OPENAI_API_KEY`;
- `OPENAI_MODEL`.

Nunca subas `.env` a GitHub.

## Instalación

Windows / PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Ejecución

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 2408 --reload
```

Swagger:

```
http://127.0.0.1:2408/docs
```

## API de Conta AI

### Estado

```
GET /api/v1/ai/status
```

### Chat

```
POST /api/v1/ai/chat
```

Ejemplo:

```json
{
  "ruc": "0102045275001",
  "mensaje": "¿Cuánto compré en agosto?",
  "anio": 2026,
  "mes": 8
}
```

La primera respuesta devuelve un `conversation_id`. Para continuar la misma
conversación se envía ese identificador en las siguientes solicitudes.

### Limpiar conversación

```
POST /api/v1/ai/conversation/clear
```

## Seguridad

El RUC que llega a la API se valida contra `BdTotal`. Las herramientas de IA
reciben internamente ese RUC y no permiten que el modelo seleccione una base de
datos arbitraria.

Las herramientas actuales son de solo lectura.

## Alcance actual

- Compras
- Bases imponibles
- IVA
- ICE
- Retenciones
- Detalle de comprobantes de compras

El módulo SRI permanece fuera del alcance actual de Conta.
