# Contributing to Reflow

Thanks for your interest in contributing! This guide will help you get set up.

## Development Setup

```bash
# Clone the repo
git clone https://github.com/danfry1/reflow-ts.git
cd reflow-ts

# Install dependencies (bun is the primary package manager)
bun install

# Run tests (vitest is required - bun test is not supported)
bun run test

# Type check
bun run typecheck

# Build the library
bun run build

# Build the docs site
bun run site:build
```

## Project Structure

```
src/
  core/
    workflow.ts     # Workflow builder (createWorkflow, .step(), .onFailure())
    engine.ts       # Engine (createEngine, polling, execution, heartbeat)
    types.ts        # Shared types (StorageAdapter, WorkflowRun, StepResult, etc.)
  storage/
    sqlite-node.ts  # SQLite storage adapter for Node.js (better-sqlite3)
    sqlite-bun.ts   # SQLite storage adapter for Bun (bun:sqlite)
    memory.ts       # In-memory storage adapter (tests)
    codec.ts        # JSON serialization for PersistedValue
  test/
    index.ts        # Test helper (testEngine)
docs/
  site/src/         # Documentation website (plain HTML/CSS/JS)
```

## Running Tests

```bash
bun run test          # Run all tests once (uses vitest)
bun run test:watch    # Watch mode
```

Tests use vitest — do not use `bun test` (Bun's built-in runner), as the test suite relies on vitest-specific APIs. Tests are in `__tests__/` directories alongside the code they test.

## Making Changes

1. Create a branch from `main`
2. Write tests first when possible
3. Keep changes focused - one feature or fix per PR
4. Run `bun run test` and `bun run typecheck` before pushing

## Code Style

- TypeScript with `strict: true`
- No external linting tools - keep code clean and consistent with existing patterns
- Prefer explicit types on public APIs, infer types internally
- Use JSDoc on public interfaces and exported functions
- Step/workflow outputs must be JSON-serializable (`PersistedValue` type)

## Pull Requests

- Keep PRs small and focused
- Write a clear description of what changed and why
- Include tests for new functionality
- Ensure all existing tests pass
