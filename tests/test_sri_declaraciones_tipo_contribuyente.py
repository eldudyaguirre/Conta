import pytest

from app.services.sri_declaraciones_service import (
    OBLIGACIONES,
    _obligacion_renta_por_tipo_contribuyente,
)


@pytest.mark.parametrize(
    ("tipo_contribuyente", "obligacion"),
    [
        ("PERSONA NATURAL", "renta_natural"),
        ("Persona Natural Obligada a Llevar Contabilidad", "renta_natural"),
        ("PERSONA JURÍDICA", "renta_sociedades"),
        ("PERSONA JURIDICA", "renta_sociedades"),
        ("SOCIEDAD", "renta_sociedades"),
    ],
)
def test_tipo_contribuyente_selecciona_obligacion_sri(tipo_contribuyente, obligacion):
    assert _obligacion_renta_por_tipo_contribuyente(tipo_contribuyente) == obligacion


def test_codigos_de_obligaciones_de_renta_sri():
    assert OBLIGACIONES["renta_natural"]["data_rk"] == "14"
    assert OBLIGACIONES["renta_sociedades"]["data_rk"] == "101"


def test_tipo_contribuyente_desconocido_muestra_error_claro():
    with pytest.raises(ValueError, match="tipo_contribuyente"):
        _obligacion_renta_por_tipo_contribuyente("")
