from app.ai.context import ContaContextManager


def test_context_isolated_by_ruc_and_conversation():
    manager = ContaContextManager()

    first = manager.obtener(
        ruc="0102045275001",
        conversation_id="conv-a",
        cliente="Cliente A",
    )
    second = manager.obtener(
        ruc="0102045275001",
        conversation_id="conv-b",
        cliente="Cliente A",
    )

    first.actualizar_periodo(anio=2026, mes=8)

    assert first.anio == 2026
    assert first.mes == 8
    assert second.anio is None
    assert second.mes is None


def test_context_generates_conversation_id():
    manager = ContaContextManager()

    context = manager.obtener(
        ruc="0102045275001",
        cliente="Cliente A",
    )

    assert context.conversation_id
    assert context.ruc == "0102045275001"


def test_context_history_is_trimmed():
    manager = ContaContextManager(max_history_messages=2)
    context = manager.obtener(
        ruc="0102045275001",
        conversation_id="conv-a",
    )

    manager.guardar(
        context,
        "pregunta 1",
        "respuesta 1",
    )
    manager.guardar(
        context,
        "pregunta 2",
        "respuesta 2",
    )

    assert len(context.historial) == 2
    assert context.historial[0]["content"] == "pregunta 2"
