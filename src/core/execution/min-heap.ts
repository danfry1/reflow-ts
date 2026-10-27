/**
 * A binary min-heap: `push` and `pop` in O(log n), `peek` in O(1).
 *
 * Keeps a fan-out of thousands of operations linear where a sorted array,
 * re-sorted on every change, would grow with the square of their number.
 */
export class MinHeap<T> {
  private readonly items: T[] = []

  constructor(private readonly before: (a: T, b: T) => boolean) {}

  get size(): number {
    return this.items.length
  }

  peek(): T | undefined {
    return this.items[0]
  }

  push(item: T): void {
    const items = this.items
    items.push(item)
    let index = items.length - 1
    while (index > 0) {
      const parent = (index - 1) >> 1
      const above = items[parent]
      if (above === undefined || !this.before(item, above)) break
      items[index] = above
      index = parent
    }
    items[index] = item
  }

  pop(): T | undefined {
    const items = this.items
    const top = items[0]
    const last = items.pop()
    if (top === undefined || last === undefined || items.length === 0) return top
    let index = 0
    for (;;) {
      const left = index * 2 + 1
      const right = left + 1
      let smallest = index
      let smallestItem = last
      const leftItem = items[left]
      if (leftItem !== undefined && this.before(leftItem, smallestItem)) {
        smallest = left
        smallestItem = leftItem
      }
      const rightItem = items[right]
      if (rightItem !== undefined && this.before(rightItem, smallestItem)) {
        smallest = right
        smallestItem = rightItem
      }
      if (smallest === index) break
      items[index] = smallestItem
      index = smallest
    }
    items[index] = last
    return top
  }
}
