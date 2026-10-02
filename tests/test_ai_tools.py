from app.ai.tools import TOOL_DEFINITIONS


def test_ai_tools_are_strict_and_have_required_schema():
    assert TOOL_DEFINITIONS

    for tool in TOOL_DEFINITIONS:
        assert tool["type"] == "function"
        assert tool["strict"] is True
        assert tool["parameters"]["additionalProperties"] is False
        assert "required" in tool["parameters"]


def test_ai_tools_do_not_accept_a_database_ruc():
    for tool in TOOL_DEFINITIONS:
        properties = tool["parameters"]["properties"]
        assert "ruc" not in properties
