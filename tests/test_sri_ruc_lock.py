from __future__ import annotations

import unittest
from unittest.mock import patch

from app.services import sri_ruc_lock


class _Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.row = None

    def execute(self, query, params):
        if "pg_try_advisory_lock" in query:
            self.connection.attempts += 1
            self.connection.locked = True
            self.row = (True,)
        elif "pg_advisory_unlock" in query:
            self.connection.locked = False
            self.row = (True,)

    def fetchone(self):
        return self.row

    def close(self):
        pass


class _Connection:
    def __init__(self):
        self.locked = False
        self.closed = False
        self.attempts = 0

    def cursor(self):
        return _Cursor(self)

    def close(self):
        self.closed = True


class SriRucLockTests(unittest.IsolatedAsyncioTestCase):
    async def test_lock_released_when_operation_raises(self):
        connection = _Connection()

        @sri_ruc_lock.bloquear_sesion_sri_por_ruc
        async def operation(cls, ruc):
            self.assertTrue(connection.locked)
            raise RuntimeError("fallo simulado")

        with patch.object(sri_ruc_lock.engine, "raw_connection", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "fallo simulado"):
                await operation(None, "0105090062001")

        self.assertFalse(connection.locked)
        self.assertTrue(connection.closed)

    async def test_uses_ruc_as_lock_identity(self):
        connection = _Connection()
        seen = []
        original = sri_ruc_lock._try_lock

        def capture(conn, ruc):
            seen.append(ruc)
            return original(conn, ruc)

        @sri_ruc_lock.bloquear_sesion_sri_por_ruc
        async def operation(cls, ruc):
            return ruc

        with patch.object(sri_ruc_lock.engine, "raw_connection", return_value=connection):
            with patch.object(sri_ruc_lock, "_try_lock", side_effect=capture):
                result = await operation(None, "0190344363001")

        self.assertEqual(result, "0190344363001")
        self.assertEqual(seen, ["0190344363001"])
        self.assertTrue(connection.closed)


if __name__ == "__main__":
    unittest.main()
