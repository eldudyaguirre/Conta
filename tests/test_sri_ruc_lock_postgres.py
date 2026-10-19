from __future__ import annotations

import asyncio
import os
import unittest
from uuid import uuid4
from unittest.mock import patch

from app.services import sri_ruc_lock


@unittest.skipUnless(
    os.getenv("SRI_DB_INTEGRATION") == "1",
    "requiere PostgreSQL real (SRI_DB_INTEGRATION=1)",
)
class SriRucLockPostgresIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_ruc_waits_for_first_session_to_release_lock(self):
        ruc = f"integration-same-{uuid4()}"
        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        second_entered = asyncio.Event()

        @sri_ruc_lock.bloquear_sesion_sri_por_ruc
        async def operation(cls, value, entered, release=None):
            entered.set()
            if release is not None:
                await release.wait()

        first = asyncio.create_task(
            operation(None, ruc, first_entered, release_first)
        )
        await asyncio.wait_for(first_entered.wait(), timeout=5)

        second = asyncio.create_task(operation(None, ruc, second_entered))
        try:
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(second_entered.wait(), timeout=0.6)
        finally:
            release_first.set()

        await asyncio.wait_for(asyncio.gather(first, second), timeout=10)
        self.assertTrue(second_entered.is_set())

    async def test_different_rucs_can_hold_locks_concurrently(self):
        suffix = str(uuid4())
        entered = asyncio.Queue()
        release = asyncio.Event()

        @sri_ruc_lock.bloquear_sesion_sri_por_ruc
        async def operation(cls, value):
            await entered.put(value)
            await release.wait()
            return value

        first = asyncio.create_task(operation(None, f"integration-a-{suffix}"))
        second = asyncio.create_task(operation(None, f"integration-b-{suffix}"))
        try:
            values = {
                await asyncio.wait_for(entered.get(), timeout=5),
                await asyncio.wait_for(entered.get(), timeout=5),
            }
            self.assertEqual(values, {f"integration-a-{suffix}", f"integration-b-{suffix}"})
        finally:
            release.set()

        results = await asyncio.wait_for(asyncio.gather(first, second), timeout=10)
        self.assertEqual(set(results), values)


if __name__ == "__main__":
    unittest.main()
