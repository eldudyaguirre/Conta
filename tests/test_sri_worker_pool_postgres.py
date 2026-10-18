"""Integration tests against the disposable PostgreSQL database configured in CI."""

import os
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import text

from app.database.connection import engine
from app.services.sri_cliente_sync_service import SriClienteSyncService


pytestmark = pytest.mark.skipif(
    os.getenv("SRI_DB_INTEGRATION") != "1",
    reason="Requires an isolated PostgreSQL test database",
)

TEST_RUC = "1790012345001"


@pytest.fixture
def postgres_client(monkeypatch):
    # CI points this test at a disposable PostgreSQL 16 service, never production.
    with engine.begin() as db:
        db.execute(text("""
            CREATE TABLE IF NOT EXISTS clientes (
                ruccedcli VARCHAR(13) PRIMARY KEY,
                nomclient TEXT NOT NULL DEFAULT '',
                activo BOOLEAN NOT NULL DEFAULT TRUE,
                clavesri TEXT NOT NULL DEFAULT ''
            )
        """))
        db.execute(text("DELETE FROM conta_sri_jobs WHERE TRIM(ruc) = :ruc"), {"ruc": TEST_RUC}) if _table_exists(db, "conta_sri_jobs") else None
        db.execute(text("""
            INSERT INTO clientes (ruccedcli, nomclient, activo, clavesri)
            VALUES (:ruc, 'Cliente de integración', TRUE, 'clave-ficticia')
            ON CONFLICT (ruccedcli) DO UPDATE
            SET nomclient = EXCLUDED.nomclient,
                activo = TRUE,
                clavesri = EXCLUDED.clavesri
        """), {"ruc": TEST_RUC})

    # Prepare schema once before racing, then exercise the actual transaction,
    # advisory lock, active-job query, and insert path concurrently.
    SriClienteSyncService._ensure_jobs_table()
    with engine.begin() as db:
        db.execute(text("DELETE FROM conta_sri_jobs WHERE TRIM(ruc) = :ruc"), {"ruc": TEST_RUC})

    monkeypatch.setattr(
        SriClienteSyncService,
        "_ensure_jobs_table",
        classmethod(lambda cls: None),
    )
    yield TEST_RUC

    with engine.begin() as db:
        db.execute(text("DELETE FROM conta_sri_jobs WHERE TRIM(ruc) = :ruc"), {"ruc": TEST_RUC)
        db.execute(text("DELETE FROM clientes WHERE TRIM(ruccedcli) = :ruc"), {"ruc": TEST_RUC})


def _table_exists(db, table_name: str) -> bool:
    return bool(db.execute(text("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = :table_name
        )
    """), {"table_name": table_name}).scalar())


def test_concurrent_sync_requests_for_same_ruc_create_only_one_job(postgres_client):
    def request(period):
        year, month = period
        return SriClienteSyncService.iniciar_sincronizacion(
            ruc=postgres_client,
            anio=year,
            mes=month,
            tipo_comprobante=1,
            operacion="compras" if month == 9 else "ventas",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(request, [(2026, 9), (2026, 10)]))

    assert sum(1 for result in results if result.get("duplicado") is False) == 1
    assert sum(1 for result in results if result.get("duplicado") is True) == 1

    with engine.connect() as db:
        active_count = db.execute(text("""
            SELECT COUNT(*)
            FROM conta_sri_jobs
            WHERE TRIM(ruc) = :ruc
              AND estado IN ('pendiente', 'ejecutando', 'captcha')
        """), {"ruc": postgres_client}).scalar()

    assert active_count == 1
