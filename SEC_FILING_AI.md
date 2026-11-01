# SEC filing explanations

The filing feed keeps each SEC filing's displayed title unchanged. Click the filing title or **Explain simply** to see a cached plain-language summary, key points, why it matters, caveats, and SEC filing links found inside the document. Clicking one of those related filings loads its own explanation.

The server reads credentials from environment variables; never put provider keys in source files:

- `GROQ_API_KEYS` — comma-separated Groq keys (or one `GROQ_API_KEY`). The default model is `openai/gpt-oss-20b`.
- `GEMINI_API_KEYS` — comma-separated Gemini keys (or one `GEMINI_API_KEY`). This is the fallback model pool and defaults to `gemini-3.5-flash-lite`.
- `GROQ_FILING_MODEL` optionally overrides the Groq model. `GEMINI_FILING_MODEL` optionally overrides the Gemini model; if unset, the app uses `GEMINI_MODEL` when set, then falls back to `gemini-3.5-flash-lite`.

Add secrets to the local root `.env` for local use and to the hosting provider's secret environment settings for deployment. The free tiers have provider-specific caps, and multiple keys under the same Groq organization or Gemini project do not multiply those quotas. The app caches each explanation for 30 days and falls back from Groq to Gemini when the first provider is unavailable.
