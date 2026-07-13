# Start Here

This folder is designed to be copied into the root of a new, empty Git repository named something like `stream-reliability-lab`.

## 1. Copy the starter files

The repository root should contain:

```text
AGENTS.md
PLAN.md
PROGRESS.md
DECISIONS.md
START_GOAL.txt
CLEANUP_GOAL.txt
.codex/
```

Keep `START_GOAL.txt` and `CLEANUP_GOAL.txt` in the repo while working, or delete them before the final portfolio release if you prefer.

## 2. Initialize Git

```bash
git init
git add AGENTS.md PLAN.md PROGRESS.md DECISIONS.md START_GOAL.txt CLEANUP_GOAL.txt .codex
git commit -m "chore: add product plan and Codex agent setup"
```

Creating an initial commit makes it much easier to inspect everything Codex changes.

## 3. Open the repository in Codex

Use the ChatGPT desktop app's Codex view, the Codex IDE extension, or run the CLI from the repository root:

```bash
codex
```

Trust only the repository you created. Select a permission mode that allows workspace edits and command execution, but keep publishing, external credentials, and destructive access under your control.

The project config enables Goal Mode and allows up to six direct subagent threads. Custom agents are read-only by design; the main thread owns implementation.

## 4. Start the broad build goal

Copy the entire contents of `START_GOAL.txt` into Codex.

If `/goal` is not recognized, enable it and restart the session:

```bash
codex features enable goals
```

Then paste `START_GOAL.txt` again.

## 5. Useful steering messages

Use these only when the run genuinely needs direction.

### Prioritize visible progress

```text
Continue toward the current goal. Prioritize the first complete simulator-to-browser vertical slice over secondary polish. Keep PROGRESS.md current and do not stop at a plan.
```

### Ask for a status report without changing direction

```text
Give me a compact status: current checkpoint, what was actually verified, the next proof command, and any blocker. Then continue the goal.
```

### Prevent architecture drift

```text
Stay within PLAN.md and AGENTS.md. Prefer the simplest implementation that passes the acceptance criteria; document noncritical improvements instead of expanding scope.
```

### Demand stronger verification

```text
Do not treat a passing unit test as proof of the end-to-end claim. Run the documented application and Playwright path, inspect stored lifecycle evidence, and fix any mismatch before continuing.
```

## 6. Review the result before accepting it

At minimum, personally do these things:

1. Inspect `git diff` and the dependency list.
2. Follow the README from a clean environment.
3. Open the overlay and dashboard.
4. Run the primary scenario.
5. Run the full test/check command.
6. Pick one important test, temporarily break its protected behavior, and confirm the test fails; restore the code afterward.
7. Read `DECISIONS.md` and make sure you can explain the major choices.

## 7. Run the cleanup goal

After v0.1 works, commit or create a checkpoint branch. Then paste the contents of `CLEANUP_GOAL.txt` into a new goal. This separates product creation from simplification and makes the cleanup diff reviewable.

## Suggested Git checkpoints

Even during a broad autonomous run, preserve useful checkpoints:

```text
chore: initialize project and verification tooling
feat: add deterministic event simulator and ingestion
feat: complete event lifecycle and browser acknowledgments
feat: add fault scenarios and reliability analytics
test: add end-to-end and CI verification
docs: publish portfolio demo and architecture
refactor: stabilize and simplify v0.1
```

Codex may create different commits or leave changes uncommitted depending on your environment. Do not allow it to push or publish without your explicit review.
