/**
 * Shared assertions helpers for the test suite. Not part of the published
 * package — `tsdown` builds only the declared entry points, and this file is
 * excluded from coverage along with the rest of `__tests__`.
 */

/**
 * Read `items[index]`, failing loudly if it is not there.
 *
 * Index access is `T | undefined` under `noUncheckedIndexedAccess`, and tests
 * read positionally all the time (`steps[0].status`). This narrows with a real
 * runtime check rather than an assertion or a cast, so a wrong-length result
 * fails with the actual length instead of `Cannot read properties of undefined`.
 */
export function at<T>(items: readonly T[], index: number): T {
  const item = items[index]

  if (item === undefined) {
    throw new Error(
      `Expected an element at index ${index}, but the collection has ${items.length}`,
    )
  }

  return item
}
