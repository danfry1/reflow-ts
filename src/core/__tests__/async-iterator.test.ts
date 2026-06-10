import { describe, expect, it, vi } from 'vitest'
import { createBoundedAsyncIterator } from '../async-iterator'

describe('createBoundedAsyncIterator', () => {
  it('supports undefined values without confusing them with an empty buffer', async () => {
    const { iterator, subscriber } = createBoundedAsyncIterator<undefined>(1, () => {})
    const controller = new AbortController()

    await subscriber.push(undefined, controller.signal)

    await expect(iterator.next()).resolves.toEqual({ value: undefined, done: false })
    await iterator.return?.()
  })

  it('implements zero-capacity rendezvous delivery', async () => {
    const { iterator, subscriber } = createBoundedAsyncIterator<string>(0, () => {})
    const controller = new AbortController()
    let pushSettled = false
    const pushPromise = subscriber.push('event', controller.signal).then(() => {
      pushSettled = true
    })

    await Promise.resolve()
    expect(pushSettled).toBe(false)
    await expect(iterator.next()).resolves.toEqual({ value: 'event', done: false })
    await pushPromise
  })

  it('removes an aborted producer without disturbing later queued producers', async () => {
    const { iterator, subscriber } = createBoundedAsyncIterator<string>(1, () => {})
    const active = new AbortController()
    const aborted = new AbortController()
    const later = new AbortController()

    await subscriber.push('buffered', active.signal)
    const abortedPush = subscriber.push('aborted', aborted.signal)
    const laterPush = subscriber.push('later', later.signal)
    aborted.abort(new Error('cancelled'))

    await expect(abortedPush).rejects.toThrow('cancelled')
    await expect(iterator.next()).resolves.toEqual({ value: 'buffered', done: false })
    await laterPush
    await expect(iterator.next()).resolves.toEqual({ value: 'later', done: false })
  })

  it('close() preserves accepted buffered values and drops blocked producers', async () => {
    const { iterator, subscriber } = createBoundedAsyncIterator<string>(1, () => {})
    const controller = new AbortController()

    await subscriber.push('buffered', controller.signal)
    const blockedPush = subscriber.push('blocked', controller.signal)
    subscriber.close()

    await blockedPush
    await expect(iterator.next()).resolves.toEqual({ value: 'buffered', done: false })
    await expect(iterator.next()).resolves.toEqual({ value: undefined, done: true })
    await expect(subscriber.push('late', controller.signal)).resolves.toBeUndefined()
  })

  it('return() clears buffered values and invokes disposal exactly once', async () => {
    const onDispose = vi.fn()
    const { iterator, subscriber } = createBoundedAsyncIterator<string>(2, onDispose)
    const controller = new AbortController()

    await subscriber.push('a', controller.signal)
    await subscriber.push('b', controller.signal)
    await iterator.return?.()
    await iterator.return?.()
    await iterator[Symbol.asyncDispose]()

    expect(onDispose).toHaveBeenCalledOnce()
    await expect(iterator.next()).resolves.toEqual({ value: undefined, done: true })
  })

  it('rejects immediately when the producer signal is already aborted', async () => {
    const { iterator, subscriber } = createBoundedAsyncIterator<string>(1, () => {})
    const controller = new AbortController()
    controller.abort(new Error('already cancelled'))

    await expect(subscriber.push('event', controller.signal)).rejects.toThrow(
      'already cancelled',
    )
    await iterator.return?.()
  })

  it('handles an abort racing with producer listener registration', async () => {
    const { iterator, subscriber } = createBoundedAsyncIterator<string>(0, () => {})
    const controller = new AbortController()
    const signal = controller.signal
    const addEventListener = signal.addEventListener.bind(signal)
    vi.spyOn(signal, 'addEventListener').mockImplementation((type, listener, options) => {
      addEventListener(type, listener, options)
      controller.abort(new Error('registration race'))
    })

    await expect(subscriber.push('event', signal)).rejects.toThrow('registration race')
    await iterator.return?.()
  })
})
