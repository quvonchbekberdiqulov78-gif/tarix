import unittest
from unittest.mock import AsyncMock, patch

from test_ai import _year_tokens, grade_written_answer


class WrittenGradingTests(unittest.IsolatedAsyncioTestCase):
    def test_year_tokens(self):
        self.assertEqual(_year_tokens("1991-yil 31-avgust"), {"1991"})

    async def test_wrong_year_is_always_wrong_without_ai(self):
        result, status = await grade_written_answer(
            "O‘zbekiston mustaqilligi qachon e’lon qilingan?",
            "1991-yil 31-avgust",
            "1992-yil 31-avgust",
        )
        self.assertEqual(status, "ok")
        self.assertEqual(result["score"], 0)

    async def test_correct_year_allows_ai_semantic_review(self):
        fake = {"candidates": [{"content": {"parts": [{"text": '{"results": [{"id": "1", "correct": true, "confidence": 0.96, "feedback": "To‘g‘ri"}]}'}]}}]}
        with patch("test_ai.gemini_generate", new=AsyncMock(return_value=(fake, "ok"))):
            result, status = await grade_written_answer(
                "O‘zbekiston mustaqilligi qachon e’lon qilingan?",
                "1991-yil 31-avgust",
                "1991 yil 31 avgust",
            )
        self.assertEqual(status, "ok")
        self.assertEqual(result["score"], 1)

    async def test_uncertain_answer_requires_teacher_review(self):
        fake = {"candidates": [{"content": {"parts": [{"text": '{"results": [{"id": "1", "correct": true, "confidence": 0.55, "feedback": "Noaniq"}]}'}]}}]}
        with patch("test_ai.gemini_generate", new=AsyncMock(return_value=(fake, "ok"))):
            result, status = await grade_written_answer("Savol", "Aniq ma’lumot", "Noaniq javob")
        self.assertEqual(status, "needs_review")
        self.assertIsNone(result)

    async def test_missing_reference_requires_teacher_review(self):
        result, status = await grade_written_answer("Savol", "", "Javob")
        self.assertIsNone(result)
        self.assertEqual(status, "needs_review")

    async def test_blank_answer_is_incorrect_without_ai(self):
        result, status = await grade_written_answer("Savol", "To‘g‘ri javob", "")
        self.assertEqual(status, "ok")
        self.assertEqual(result["score"], 0)


if __name__ == "__main__":
    unittest.main()
