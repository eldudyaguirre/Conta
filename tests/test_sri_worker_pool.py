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
