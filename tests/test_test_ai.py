import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from test_ai import _clean_questions, extract_questions_with_gemini


class CleanQuestionsTests(unittest.TestCase):
    def test_choice_options_are_normalized(self):
        result = _clean_questions({
            "questions": [{
                "text": "Amir Temur qachon tug‘ilgan?",
                "type": "choice",
                "options": [
                    {"text": "1336-yil"},
                    {"text": "1405-yil"},
                    {"text": "1399-yil"},
                ],
            }]
        })
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["type"], "choice")
        self.assertEqual([x["key"] for x in result[0]["options"]], ["A", "B", "C"])
        self.assertEqual(result[0]["images"], [])

    def test_one_option_becomes_written_question(self):
        result = _clean_questions({
            "questions": [{
                "text": "Tarixiy hodisani izohlang",
                "type": "choice",
                "options": [{"text": "Bitta variant"}],
            }]
        })
        self.assertEqual(result[0]["type"], "written")
        self.assertEqual(result[0]["options"], [])

    def test_empty_questions_are_removed(self):
        result = _clean_questions({
            "questions": [
                {"text": "   ", "options": []},
                {"text": "Savol matni", "options": []},
            ]
        })
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["text"], "Savol matni")

    def test_question_limit_detection_allows_one_extra(self):
        result = _clean_questions({
            "questions": [{"text": f"Savol {i}", "options": []} for i in range(105)]
        })
        self.assertEqual(len(result), 101)


class GeminiFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_key_uses_parser_fallback(self):
        with patch.dict("os.environ", {}, clear=True):
            result, status = await extract_questions_with_gemini("test.pdf", b"%PDF-test")
        self.assertEqual(result, [])
        self.assertEqual(status, "not_configured")

    async def test_extraction_uses_bounded_timeout(self):
        with patch("test_ai._gemini_json", new=AsyncMock(return_value=(None, "timeout"))) as mocked:
            result, status = await extract_questions_with_gemini(
                "test.pdf", b"%PDF-test", timeout_seconds=12
            )
        self.assertEqual(result, [])
        self.assertEqual(status, "timeout")
        self.assertEqual(mocked.await_args.kwargs["timeout_seconds"], 12)


if __name__ == "__main__":
    unittest.main()
