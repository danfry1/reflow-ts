import type { IncomingMessage, ServerResponse } from 'node:http'
import { Readable } from 'node:stream'

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
    void serve(handler, req, res).catch((error: unknown) => {
      if (!res.headersSent) {
        res.statusCode = 500
        res.end(error instanceof Error ? error.message : 'Internal error')
      } else {
        res.destroy(error instanceof Error ? error : undefined)
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
  // `Readable.fromWeb` would do, but its types disagree across Node versions
  // supported here; reading the stream by hand works on all of them.
  const reader = response.body.getReader()
  const stream = new Readable({
    read() {
      reader.read().then(
        ({ done, value }) => {
          this.push(done ? null : value)
        },
        (error: unknown) => {
          this.destroy(error instanceof Error ? error : undefined)
        },
      )
    },
    destroy(error, callback) {
      reader.cancel(error).then(() => callback(error), () => callback(error))
    },
  })
  stream.pipe(res)
}
