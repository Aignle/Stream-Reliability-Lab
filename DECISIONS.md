# Architecture and Product Decisions

Record meaningful decisions here. Keep entries short and evidence-based.

## Template

### YYYY-MM-DD — Decision title

**Decision:** What was chosen.

**Reason:** Why this is the simplest or strongest choice for the v0.1 acceptance criteria.

**Tradeoff:** What this choice does not solve or what should be revisited later.

---

## Initial constraints

- Local-first portfolio project.
- Synthetic data only.
- At-least-once delivery with idempotent processing.
- FastAPI, Pydantic, DuckDB, Streamlit, Playwright, Docker Compose, and GitHub Actions.
- No real streaming-platform integration or AI feature in v0.1.
