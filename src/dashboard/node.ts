import type { IncomingMessage, ServerResponse } from 'node:http'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'

/**
 * Adapt a Fetch API handler — such as the one `createDashboard()` returns —
 * to Node's `(req, res)` signature, for `node:http` and Express.
 *
 * Streams the response body as it is produced, so live updates work, and
 * aborts the request's `signal` when the client disconnects.
 *
 * @example
 * ```ts
 * import { createServer } from 'node:http'
 * createServer(toNodeHandler(dashboard)).listen(4400)
 *
 * // Express strips the mount path, so leave `basePath` as '/'
 * app.use('/reflow', toNodeHandler(dashboard))
 * ```
 */
export function toNodeHandler(
  handler: (request: Request) => Promise<Response>,
): (req: IncomingMessage, res: ServerResponse) => void {
  return (req, res) => {
    void serve(handler, req, res).catch(() => {
      // Nothing about the failure is sent: the caller may be unauthenticated.
      if (!res.headersSent) {
        res.writeHead(500, { 'content-type': 'text/plain; charset=utf-8', 'x-content-type-options': 'nosniff' })
        res.end('Internal error')
      } else {
        res.destroy()
      }
    })
  }
}

async function serve(
  handler: (request: Request) => Promise<Response>,
  req: IncomingMessage,
  res: ServerResponse,
): Promise<void> {
  const aborted = new AbortController()
  res.on('close', () => aborted.abort())

  const headers = new Headers()
  for (const [name, value] of Object.entries(req.headers)) {
    if (Array.isArray(value)) {
      for (const entry of value) headers.append(name, entry)
    } else if (value !== undefined) {
      headers.set(name, value)
    }
  }

  const method = req.method ?? 'GET'
  const request = new Request(new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`), {
    method,
    headers,
    signal: aborted.signal,
    // The dashboard's endpoints take no request body.
  })

  const response = await handler(request)
  res.statusCode = response.status
  response.headers.forEach((value, name) => {
    res.setHeader(name, value)
  })

  if (response.body === null) {
    res.end()
    return
  }

  res.flushHeaders()
  // `pipeline` forwards an error from either side and tears both down, so a
  // body that fails mid-stream ends this response instead of crashing the
  // process with an unhandled 'error' event.
  await pipeline(readableFrom(response.body), res)
}

/**
 * A Node readable over a web stream. `Readable.fromWeb` would do, but its types
 * disagree across the Node versions supported here.
 */
function readableFrom(body: ReadableStream<Uint8Array>): Readable {
  const reader = body.getReader()
  return new Readable({
    read() {
      reader.read().then(
        ({ done, value }) => {
          this.push(done ? null : value)
        },
        (error: unknown) => {
          this.destroy(error instanceof Error ? error : new Error(String(error)))
        },
      )
    },
    destroy(error, callback) {
      reader.cancel(error ?? undefined).then(() => callback(error), () => callback(error))
    },
  })
}
