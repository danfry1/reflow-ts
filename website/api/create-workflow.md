# `createWorkflow(config)`

Creates a new, immutable workflow builder.

```typescript
import { createWorkflow } from 'reflow-ts'
import { z } from 'zod'

const workflow = createWorkflow({
  name: 'process-content',
  input: z.object({ url: z.string() }),
})
```

| Parameter | Type | Description |
|---|---|---|
| `config.name` | `string` | Unique workflow name. Captured as a string-literal type. |
| `config.input` | `StandardSchemaV1` | Any [Standard Schema](https://github.com/standard-schema/standard-schema)-compatible schema used to validate input. |

Returns a [`Workflow`](/api/workflow) with `.step()`, `.parallel()`, `.onFailure()`, and `.parseInput()`.

The builder is **immutable** — every method returns a new `Workflow`, so a base workflow can be branched without the branches affecting each other. See [Workflows](/guide/workflows).

## `workflow.parseInput(input)`

Validates an unknown value against the workflow's input schema and returns the typed, parsed value. Throws [`ValidationError`](/api/errors) if validation fails. The engine calls this for you on `enqueue()`; it's exposed for validating input at your own boundaries.

```typescript
const parsed = workflow.parseInput(req.body) // typed as the schema output, or throws
```

Async schema validation is not supported — a schema that returns a `Promise` throws a `TypeError`.
