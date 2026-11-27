import os
import unittest
from unittest.mock import patch

import gemini_manager
from gemini_manager import _configured_keys, gemini_generate


class GeminiKeyConfigTests(unittest.TestCase):
    def setUp(self):
        gemini_manager._STATE.clear()
        gemini_manager._NEXT_KEY_INDEX = 0
        for key in gemini_manager._TOTAL:
            gemini_manager._TOTAL[key] = 0

    def test_reads_multiple_unique_keys_in_priority_order(self):
        with patch.dict(os.environ, {
            "GEMINI_API_KEY": "legacy-key",
            "GEMINI_API_KEYS": "first-key, second-key;legacy-key",
            "GEMINI_API_KEY_1": "numbered-key",
        }, clear=True):
            self.assertEqual(
                _configured_keys(),
                ["legacy-key", "first-key", "second-key", "numbered-key"],
            )

    def test_missing_keys_returns_empty_list(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_configured_keys(), [])

    def test_status_exposes_only_masked_key_suffixes(self):
        with patch.dict(os.environ, {
            "GEMINI_API_KEYS": "secret-1234,secret-5678",
        }, clear=True):
            status = gemini_manager.gemini_status()

        self.assertEqual(status["configured"], 2)
        self.assertEqual(status["max_keys"], gemini_manager.MAX_KEYS)
        self.assertEqual(len(status["keys"]), 2)
        self.assertEqual(
            [row["masked"] for row in status["keys"]],
            ["…1234", "…5678"],
        )
        self.assertNotIn("secret-1234", str(status))
        self.assertNotIn("secret-5678", str(status))

    def test_missing_keys_do_not_call_provider(self):
        async def run_test():
            with patch.dict(os.environ, {}, clear=True):
                data, status = await gemini_generate("test-model", {"contents": []})
            self.assertIsNone(data)
            self.assertEqual(status, "not_configured")

        import asyncio
        asyncio.run(run_test())

    def test_single_key_success(self):
        class FakeResponse:
            status = 200

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def json(self, content_type=None):
                return {"candidates": []}

        class FakeSession:
            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            def post(self, *args, **kwargs):
                return FakeResponse()

        async def run_test():
            with patch.dict(os.environ, {"GEMINI_API_KEY": "single-key"}, clear=True):
                with patch("gemini_manager.aiohttp.ClientSession", FakeSession):
                    data, status = await gemini_generate(
                        "test-model", {"contents": []}
                    )
            self.assertEqual(status, "ok")
            self.assertEqual(data, {"candidates": []})
            self.assertEqual(gemini_manager._STATE["single-key"]["success"], 1)

        import asyncio
        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
