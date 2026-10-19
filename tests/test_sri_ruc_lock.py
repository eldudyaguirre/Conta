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

    async def test_same_ruc_is_serialized(self):
        active = set()
        connection_by_task = {}
        guard = __import__("threading").Lock()
        maximum_active = 0

        def try_lock(connection, ruc):
            nonlocal maximum_active
            with guard:
                if ruc in active:
                    return False
                active.add(ruc)
                connection_by_task[id(connection)] = ruc
                maximum_active = max(maximum_active, len(active))
                return True

        original_unlock = sri_ruc_lock._unlock_and_close

        def unlock(connection, ruc, acquired):
            with guard:
                if acquired:
                    active.discard(ruc)
            connection.locked = False
            connection.closed = True

        @sri_ruc_lock.bloquear_sesion_sri_por_ruc
        async def operation(cls, ruc, started, release):
            started.append(ruc)
            await release.wait()

        connections = [_Connection(), _Connection()]
        with patch.object(sri_ruc_lock.engine, "raw_connection", side_effect=connections):
            with patch.object(sri_ruc_lock, "_try_lock", side_effect=try_lock):
                with patch.object(sri_ruc_lock, "_unlock_and_close", side_effect=unlock):
                    started = []
                    release = __import__("asyncio").Event()
                    first = __import__("asyncio").create_task(operation(None, "0105090062001", started, release))
                    for _ in range(100):
                        if started:
                            break
                        await __import__("asyncio").sleep(0.01)
                    second = __import__("asyncio").create_task(operation(None, "0105090062001", started, release))
                    await __import__("asyncio").sleep(0.08)
                    self.assertEqual(len(started), 1)
                    release.set()
                    await __import__("asyncio").gather(first, second)

        self.assertEqual(len(started), 2)
        self.assertEqual(maximum_active, 1)

    async def test_different_ruc_can_enter_concurrently(self):
        active = set()
        guard = __import__("threading").Lock()
        entered = __import__("asyncio").Queue()

        def try_lock(connection, ruc):
            with guard:
                if ruc in active:
                    return False
                active.add(ruc)
                return True

        def unlock(connection, ruc, acquired):
            with guard:
                if acquired:
                    active.discard(ruc)
            connection.closed = True

        @sri_ruc_lock.bloquear_sesion_sri_por_ruc
        async def operation(cls, ruc):
            await entered.put(ruc)
            await __import__("asyncio").sleep(0.05)
            return ruc

        connections = [_Connection(), _Connection()]
        with patch.object(sri_ruc_lock.engine, "raw_connection", side_effect=connections):
            with patch.object(sri_ruc_lock, "_try_lock", side_effect=try_lock):
                with patch.object(sri_ruc_lock, "_unlock_and_close", side_effect=unlock):
                    results = await __import__("asyncio").gather(
                        operation(None, "0105090062001"),
                        operation(None, "0190344363001"),
                    )

        self.assertEqual(set(results), {"0105090062001", "0190344363001"})
        self.assertEqual(set(active), set())


if __name__ == "__main__":
    unittest.main()
