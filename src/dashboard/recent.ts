/**
 * Short-lived sharing of reads between requests.
 *
 * Several viewers, or one page refreshing on every change, often ask for the
 * same counts or page within a second. Sharing the read keeps the dashboard's
 * load on storage flat however many are watching.
 */

/** Recent reads by key, with when each began. Bounded: see {@link shared}. */
export class RecentReads<T> {
  private readonly reads = new Map<string, { at: number; value: Promise<T> }>()

  constructor(
    private readonly ttlMs: number,
    private readonly capacity: number,
  ) {}

  /** How many reads are held. */
  get size(): number {
    return this.reads.size
  }

  /**
   * The read under `key` if it began within the TTL, otherwise a fresh one.
   *
   * Expired reads are dropped whenever a new one is kept, and beyond
   * `capacity` the oldest go first, so distinct keys — a cursor per page,
   * a search per keystroke — cannot grow the cache without bound. A failed
   * read is not kept, so the next request tries again.
   */
  shared(key: string, read: () => Promise<T>, now = Date.now()): Promise<T> {
    const recent = this.reads.get(key)
    if (recent && now - recent.at < this.ttlMs) return recent.value

    for (const [held, entry] of this.reads) {
      if (now - entry.at >= this.ttlMs) this.reads.delete(held)
    }
    this.reads.delete(key)
    while (this.reads.size >= this.capacity) {
      const oldest = this.reads.keys().next()
      if (oldest.done) break
      this.reads.delete(oldest.value)
    }

    const value = read()
    this.reads.set(key, { at: now, value })
    value.catch(() => {
      if (this.reads.get(key)?.value === value) this.reads.delete(key)
    })
    return value
  }
}
