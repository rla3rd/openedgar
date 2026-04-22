# Engineering Steering Principles
*Inspired by The Pragmatic Programmer — applies to all code in this workspace*

This document is the standing technical constitution for all software written here. Every design decision, refactor, and new feature should be measured against these principles. It is not project-specific.

---

## Core Principles

### 1. DRY — Don't Repeat Yourself
There should be a single, authoritative representation of every piece of knowledge in the system. Duplication of logic, constants, schemas, or data formats is a defect, not a style choice. When two places need the same behavior, extract it; never copy it.

### 2. Orthogonality — One Job Per Component
Components should be independent and have narrow responsibilities. A change to one module should not ripple into unrelated modules. Prefer thin interfaces over shared state. If you need to explain what a class does using "and", split it.

### 3. Tracer Bullets — Measure Before You Optimize
Ship a thin, end-to-end slice first. Get real signal from real data before adding complexity. Benchmarks, eval scripts, and integration tests are tracer bullets — run them constantly, not just at the end.

### 4. Reversibility — Avoid Irreversible Decisions
Prefer designs that keep options open. Use configuration over hardcoding, abstract over concrete, and migration over schema destruction. If you are about to do something you cannot undo, pause and ask.

### 5. The Broken Window Rule — Fix Bad Code on Contact
Do not leave broken windows (bad designs, wrong decisions, poor code) unfixed. When you touch a file, leave it slightly better than you found it. Technical debt compounds.

### 6. Automation — If You Do It Twice, Script It
Manual verification is not verification. Evaluation, formatting, deployment, and testing must all be automated. The machine is the judge; human eyeballing is a smell.

### 7. Proximity — Keep Information Near Its Context
Data, documentation, and configuration should live as close as possible to the code that uses them. Distant indirection (separate lookup tables, out-of-band spreadsheets, README-only specifications) causes drift and bugs.

### 8. Small, Focused Commits
Each commit should represent one logical change. Mixing refactors with feature additions makes history unreadable and rollback risky.

### 9. The Power of Plain Text
Prefer plain text formats (Markdown, JSON, YAML, CSV) for data at rest, configuration, and interchange. Binary formats are acceptable only where performance demands it. Plain text survives tooling changes.

### 10. Design to Be Tested
Write code that can be verified without a full running system. Pure functions over side effects. Dependency injection over global state. If a component cannot be unit-tested in isolation, its design needs rethinking.

---

## Applied Rules for This Codebase

### One Parser Per Form Type
There is exactly one canonical parser per SEC form type, living in `openedgar/parsers/`. Experimental synthesizers in `sec_research/` must graduate into the canonical parser or be deleted. Parallel implementations are a DRY violation.

### Single Synthesis Engine
Training data, evaluation data, and production markdown must all be produced by the same code path. Any divergence between what the model trained on and what it is evaluated against invalidates all metrics.

### Schema = Ground Truth = Prompt Template
The database schema, the `serialize_ground_truth()` output, and the LLM `schema_template` must be structurally identical. A field added to the DB must be added to all three in the same commit.

### Eval Script is the Arbiter
No claim about model quality is valid without a field-level F1 score from `evaluate_ownership_llm.py`. Intuition is a hypothesis; the eval script is the experiment.

### Compressed Archives Are Immutable
`.zst` raw archives are write-once. Markdown sidecars (`.out.md.zst`) are derived and may be regenerated. Never modify a raw archive in place.

---
*Last Updated: 2026-04-21*
