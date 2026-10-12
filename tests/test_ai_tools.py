from app.ai.tools import TOOL_DEFINITIONS


def test_ai_tools_are_strict_and_have_required_schema():
    assert TOOL_DEFINITIONS

    for tool in TOOL_DEFINITIONS:
        assert tool["type"] == "function"
        assert tool["strict"] is True
        assert tool["parameters"]["additionalProperties"] is False
        assert "required" in tool["parameters"]


def test_tenant_scoped_tax_tools_do_not_accept_a_database_ruc():
    # Estas herramientas operan exclusivamente sobre el RUC autenticado
    # en el contexto de Conta; el modelo no puede elegir otra base de datos.
    tenant_tools = {
        "resumen_compras",
        "resumen_ventas",
        "listar_ventas",
        "resumen_notas_credito",
        "listar_notas_credito",
        "listar_compras",
    }
    by_name = {tool["name"]: tool for tool in TOOL_DEFINITIONS}

    for name in tenant_tools:
        assert name in by_name
        assert "ruc" not in by_name[name]["parameters"]["properties"]


def test_admin_tools_require_ruc_for_server_side_lookup_validation():
    # Las herramientas administrativas sí reciben RUC, pero ContaTools
    # rechaza cualquier RUC que no haya sido resuelto con buscar_cliente.
    by_name = {tool["name"]: tool for tool in TOOL_DEFINITIONS}
    admin_tools = {
        "consultar_clave_sri",
        "consultar_estado_cliente",
        "activar_cliente",
        "desactivar_cliente",
        "cambiar_clave_sri",
    }

    for name in admin_tools:
        assert name in by_name
        assert "ruc" in by_name[name]["parameters"]["properties"]
        assert "ruc" in by_name[name]["parameters"]["required"]
