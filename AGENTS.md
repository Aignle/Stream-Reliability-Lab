# Stream Reliability Lab — Codex Instructions

## Mission

Build a working, explainable portfolio project that tests and analyzes a real-time event-driven application from event generation through browser rendering.

Prefer a functioning end-to-end product over speculative scale, elaborate abstractions, or premature optimization.

## Read first

Before making changes, read:

1. `PLAN.md`
2. `PROGRESS.md`
3. `DECISIONS.md`
4. The existing code, tests, and README

Keep `PROGRESS.md` and `DECISIONS.md` current as the implementation evolves.

## Autonomy

Make reasonable implementation decisions without asking for routine approval. Choose the simplest approach that satisfies the acceptance criteria and record meaningful tradeoffs in `DECISIONS.md`.

Pause only when work would require:

- credentials or private data;
- paid external services;
- publishing, deploying, pushing, or opening a pull request;
- destructive changes outside this repository;
- changing the core product goal or required stack.

## Required stack

Use a small local-first implementation based on:

- Python 3.12+
- FastAPI and Pydantic
- DuckDB
- pytest
- Playwright
- Streamlit
- vanilla HTML, CSS, and JavaScript for the overlay
- Docker Compose
- GitHub Actions

Do not add a competing framework or infrastructure product unless it is required to make the documented acceptance criteria work.

## Reliability contract

The system uses **at-least-once delivery with idempotent processing**.

- Persist a valid event before acknowledging it.
- Record every delivery attempt, including invalid and duplicate deliveries.
- Store one canonical event for each unique `event_id`.
- A retried delivery may be observed more than once, but it must not cause more than one visible effect.
- Do not claim universal exactly-once delivery.
- Keep generated, delivered, accepted, processed, dispatched, and rendered states distinguishable.
- Use UTC timestamps and stable event/run identifiers.

## Scope boundaries

Do not add these to v0.1:

- real Twitch, TikTok, YouTube, or other platform authentication;
- real usernames, messages, tokens, or private stream data;
- Kafka, Kubernetes, cloud infrastructure, or microservice sprawl;
- React or another frontend framework;
- accounts, payments, or multi-tenancy;
- an LLM or AI feature;
- claims based on unmeasured performance.

## Engineering expectations

- Keep domain logic separate from HTTP/WebSocket handlers.
- Use type hints in application code.
- Validate external inputs at boundaries.
- Prefer explicit status transitions and structured errors.
- Avoid hidden global state and silent exception handling.
- Keep dependencies minimal and documented.
- Use deterministic seeds in simulations and tests.
- Add or update tests with behavior changes.
- Do not weaken tests merely to make them pass.
- Never commit secrets or generated databases.

## Agent coordination

The main agent owns implementation and final decisions.

Use the project custom agents for independent read-heavy work:

- `reliability_architect` for architecture and invariant review;
- `test_engineer` for acceptance tests and fault coverage;
- `skeptical_reviewer` for correctness, security, and regression review;
- `simplicity_critic` for cleanup and unnecessary-complexity review.

Parallelize read, analysis, test planning, and review. Avoid parallel edits to the same working tree. Wait for delegated reviews and validate findings before changing code.

## Working method

1. Establish a runnable skeleton.
2. Build the thinnest complete vertical path:
   simulator → ingestion → persistence → processing → overlay → render acknowledgment.
3. Make that path deterministic and tested.
4. Add duplicate, invalid-payload, reconnect, delay/reorder, and burst scenarios.
5. Add analytics and dashboard views from stored evidence.
6. Add Docker and CI verification.
7. Perform a stabilization and cleanup pass.

Work in checkpoints. After each checkpoint:

- run the relevant tests and static checks;
- update `PROGRESS.md` with evidence, remaining work, and blockers;
- keep the application runnable;
- avoid leaving half-migrated architecture behind.

## Commands to provide

Create and document stable commands for at least:

- local setup;
- starting the application;
- running a deterministic demo scenario;
- running unit and integration tests;
- running Playwright end-to-end tests;
- linting, formatting checks, and type checking;
- running the full verification suite.

A `Makefile`, task runner, or small scripts are acceptable, but documented direct commands must also work.

## Completion report

Before declaring the goal complete:

- run the full documented verification suite;
- run the primary demo from a clean application state;
- have `skeptical_reviewer` and `simplicity_critic` review the result;
- fix validated critical/high findings and material test gaps;
- verify README commands against the actual implementation;
- list exact commands run and their results;
- report remaining limitations honestly;
- do not invent benchmark numbers.
