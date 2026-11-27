# NEURON V2 — Gemini AI test module setup

## Render Environment Variables

Set one secret on the NEURON V2 Render service:

- `GEMINI_API_KEY`: the single Gemini API key.
- `GEMINI_MODEL`: optional model name; if omitted, the application uses its built-in default.

Do not add real keys to GitHub, source code, Telegram, or ChatGPT. Add the key only in Render Environment settings. The Super Admin status view displays only the final four characters; the full key is never returned or logged. The old `GEMINI_API_KEYS` variable is ignored.

## Test extraction

1. Teacher uploads a PDF or DOCX file.
2. Gemini extracts questions and options; the deterministic parser remains the fallback when AI is unavailable.
3. Teacher reviews every question and provides the correct choice or reference answer for written questions.
4. Nothing is published until the teacher confirms the answer key.

The module accepts up to 101 extracted questions. Existing test records are not migrated or deleted by this feature.

## Written-answer grading

- Gemini grades by meaning and essential facts, not exact character matching.
- Minor spelling, apostrophe, and one-letter mistakes are tolerated when the intended meaning is clearly correct.
- Historical years found in the teacher's reference answer are checked strictly before AI grading. A missing, different, or extra year results in an incorrect answer.
- Low-confidence or unavailable-AI results remain pending for teacher review.
- Blank answers are automatically incorrect.

## Failure behavior

Only one API key is used. If the key is missing, invalid, rate-limited, or temporarily unavailable, the AI operation reports a safe status and the existing deterministic fallback or teacher-review path is used. The application does not switch to another key.

## Privacy

Use anonymized test papers only; do not send student names, phone numbers, grades, or other personal information to the AI provider. Review Google's current Gemini API terms and quotas before production use.

## Official references

- https://ai.google.dev/gemini-api/docs/pricing
- https://ai.google.dev/gemini-api/docs/document-processing
- https://ai.google.dev/gemini-api/docs/models
