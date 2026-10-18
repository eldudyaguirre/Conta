import pytest

from app.services.sri_worker_pool_service import SriWorkerPoolService


def test_worker_pool_accepts_one_to_five_unique_13_digit_rucs():
    assert SriWorkerPoolService._validar_rucs(["1790012345001"]) == [
        "1790012345001"
    ]
    assert SriWorkerPoolService._validar_rucs(
        ["1790012345001", "0990012345001"]
    ) == ["1790012345001", "0990012345001"]


@pytest.mark.parametrize(
    "rucs",
    [
        [],
        ["1790012345001"] * 6,
        ["123"],
        ["1790012345001", "1790012345001"],
        ["179001234500A"],
    ],
)
def test_worker_pool_rejects_invalid_ruc_selections(rucs):
    with pytest.raises(ValueError):
        SriWorkerPoolService._validar_rucs(rucs)


def test_worker_pool_trims_ruc_whitespace():
    assert SriWorkerPoolService._validar_rucs([" 1790012345001 "]) == [
        "1790012345001"
    ]


def test_pool_active_job_check_is_by_ruc_not_period(monkeypatch):
    class FakeResult:
        def mappings(self):
            return self

        def all(self):
            return []

    class FakeDB:
        statement = None

        def execute(self, statement, params):
            self.statement = str(statement)
            self.params = params
            return FakeResult()

    db = FakeDB()
    SriWorkerPoolService._consultar_clientes(
        db,
        ["1790012345001"],
        anio=2026,
        mes=9,
        tipo="01",
        operacion="compras",
    )

    assert "j.estado IN ('pendiente', 'ejecutando', 'captcha')" in db.statement
    assert "j.anio = :anio" not in db.statement
    assert "j.mes = :mes" not in db.statement
    assert "j.operacion = :operacion" not in db.statement


def test_normal_sync_returns_warning_for_any_active_job(monkeypatch):
    from app.services.sri_cliente_sync_service import SriClienteSyncService

    class FakeDB:
        def execute(self, statement, params):
            return self

        def mappings(self):
            return self

        def first(self):
            return {
                "job_id": "job-existente",
                "estado": "ejecutando",
                "anio": 2025,
                "mes": 12,
                "tipo_comprobante": "01",
                "operacion": "ventas",
                "creado": None,
            }

    class FakeEngine:
        def begin(self):
            class Context:
                def __enter__(self):
                    return FakeDB()

                def __exit__(self, *args):
                    return False

            return Context()

    monkeypatch.setattr(SriClienteSyncService, "_ensure_jobs_table", lambda: None)
    monkeypatch.setattr(SriClienteSyncService, "bloquear_solicitudes_ruc", lambda db, ruc: None)
    monkeypatch.setattr(SriClienteSyncService, "obtener_solicitud_activa_ruc", lambda db, ruc: {
        "job_id": "job-existente",
        "estado": "ejecutando",
        "anio": 2025,
        "mes": 12,
        "tipo_comprobante": "01",
        "operacion": "ventas",
        "creado": None,
    })
    monkeypatch.setattr("app.services.sri_cliente_sync_service.engine", FakeEngine())

    resultado = SriClienteSyncService.iniciar_sincronizacion(
        ruc="1790012345001",
        anio=2026,
        mes=9,
        tipo_comprobante=1,
        operacion="compras",
    )

    assert resultado["duplicado"] is True
    assert resultado["job_id"] == "job-existente"
    assert "No se creó otra solicitud" in resultado["aviso"]
