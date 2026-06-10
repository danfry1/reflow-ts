# Type Safety

Reflow tracks types through the entire workflow chain — most mistakes are compile errors, not runtime surprises.

- **Workflow name** is a string-literal type (`'order-fulfillment'`, not `string`).
- **Input** is validated by your schema library and inferred at the type level.
- **Step chaining** — each step's `prev` is typed as the return value of the previous step.
- **`steps`** — typed access to every prior step's output by name.
- **Engine** — `enqueue()` only accepts registered workflow names with matching input.
- **Test engine** — `run()` returns typed step results keyed by step name.

```typescript
// These are all compile-time errors:
engine.enqueue('typo', {})              // 'typo' is not a registered workflow
engine.enqueue('order-fulfillment', {}) // missing required input fields
workflow.step('x', async ({ prev }) => {
  prev.nonexistent                      // property doesn't exist on prev
})
```

## How it flows

`createWorkflow` captures the name as a literal and the input type from your [Standard Schema](https://github.com/standard-schema/standard-schema). Each `.step()` returns a new `Workflow` type that records the step's output, so the next handler's `prev` and the accumulated `steps` map stay accurate. `createEngine` collects the registered workflows into a map, which is what makes `enqueue()` reject unknown names and mismatched input.

## Inference helpers

For building typed wrappers around Reflow, several helper types are exported — `InferInput`, `InferSteps`, `WorkflowInputMap`, `WorkflowStepsMap`, and the `StepContext` / `StepConfig` / `EngineEvent` types. See the [Types reference](/api/types).
