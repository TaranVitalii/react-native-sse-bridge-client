# react-native-sse-bridge-client

A native Server-Sent Events (SSE) client for React Native's classic bridge — for apps that can't use the New Architecture / Nitro Modules (older React Native versions, or New Architecture explicitly disabled).

[![Version](https://img.shields.io/npm/v/react-native-sse-bridge-client.svg)](https://www.npmjs.com/package/react-native-sse-bridge-client)
[![License](https://img.shields.io/npm/l/react-native-sse-bridge-client.svg)](https://github.com/TaranVitalii/react-native-sse-bridge-client/blob/main/LICENSE)

## Why

Most React Native SSE clients (including the popular `react-native-sse`) are built on top of `XMLHttpRequest`. Every reconnect opens a brand new HTTP request from scratch, which means a full TCP + TLS handshake every time — typically 300–600ms of latency the user sees on every reconnect, even to a server they were just talking to a second ago.

This library talks to the platform's native HTTP stack directly — `URLSession` on iOS, `OkHttp` on Android — which both keep a warm connection pool. As long as the same underlying client is reused across `connect()` calls (which it is, internally), a reconnect to the same host reuses the existing HTTP/2 connection instead of re-handshaking. `onMetrics` reports real, measured proof of this on every connection: whether it was a fresh handshake or a reused connection, and how long each phase took.

It's built on the classic Native Modules bridge (`RCTEventEmitter` on iOS, a plain `ReactContextBaseJavaModule` on Android) rather than Nitro/JSI — for the same connection-reuse proof on apps that can't run Nitro Modules. If your app can use the New Architecture, see [`react-native-nitro-sse-client`](https://github.com/TaranVitalii/react-native-nitro-sse-client) instead — same idea, JSI-direct, no bridge.

## Requirements

- React Native, any version that still ships the classic bridge (works with the New Architecture disabled too)
- iOS 13+ / Android per your app's own `minSdkVersion`

## Installation

```sh
npm install react-native-sse-bridge-client
cd ios && pod install
```

## Usage

```ts
import { createSSEStream } from 'react-native-sse-bridge-client'

const stream = createSSEStream()

stream.onOpen(() => console.log('connected'))

stream.addEventListener('message', event => {
  console.log(event.event, event.data) // event.id is optional per the SSE spec
})

stream.onError(message => console.log('error:', message))

stream.onMetrics(metrics => {
  if (metrics.phase === 'ttfb') {
    console.log(`time to first byte: ${metrics.ttfbMs}ms`)
  } else {
    // fires when the connection closes (disconnect(), or a superseding connect())
    console.log(
      `closed — reused=${metrics.connectionReused} ` +
        `connect=${metrics.connectMs}ms tls=${metrics.tlsMs}ms ttfb=${metrics.ttfbMs}ms`
    )
  }
})

stream.connect('https://your-server.example.com/events', {
  headers: { Authorization: 'Bearer …' },
})

// later
stream.disconnect()
// or, once you're done with this stream entirely:
stream.destroy()
```

## API

### `createSSEStream(): SSEStream`

Creates a new, independent stream. Classic Native Modules are singletons, so unlike a plain JSI HybridObject there's only ever one native module underneath — but every `SSEStream` you create gets its own connection, its own listeners, and its own lifecycle. Create as many as you need; they share the native connection pool, so reconnecting one doesn't disturb the others.

### `configureSSESession(options: SSESessionOptions): void`

Sets the shared session config (timeout, max connections per host) once, up front. Call this at app startup, before any screen creates/connects a stream — see [Configuring the shared session](#configuring-the-shared-session) below.

### `SSEStream` methods

| Method | Description |
| --- | --- |
| `connect(url: string, options?: SSEStreamOptions): void` | Opens a connection to `url`. Calling this again on the same stream cancels the previous connection first (its close metrics still fire). Headers — including `User-Agent` — are entirely JS-configured; nothing is hardcoded natively. |
| `disconnect(): void` | Closes the current connection, if any. |
| `addEventListener(type: string, callback: (event: SSEMessageEvent) => void): () => void` | Subscribes to a specific SSE `event:` type, mirroring the browser `EventSource` model — frames with no `event:` field (or `event: message`) are filed under `'message'`. Returns an unsubscribe function. |
| `onOpen(callback: () => void): () => void` | Fires when the server responds (response headers received). |
| `onError(callback: (message: string) => void): () => void` | Fires on a network/transport failure. Not called for a `disconnect()` you initiated yourself. |
| `onMetrics(callback: (metrics: SSEConnectionMetrics) => void): () => void` | Fires twice per connection — see below. |
| `destroy(): void` | Disconnects, drops every listener, and unsubscribes from the shared native event emitter. Call this when you're done with the stream (e.g. on unmount). |

### Event-type filtering happens natively

`addEventListener(type, cb)` doesn't just filter in JS — the set of types you've subscribed to is pushed down to the native module (at `connect()` time, and live thereafter), so a type nobody's listening for (a `ping` heartbeat, for example) is dropped natively before it ever crosses the bridge. Subscribing to zero types forwards everything, so a stream you haven't called `addEventListener` on yet still receives every frame.

### Types

```ts
interface SSEMessageEvent {
  id?: string
  event: string
  data: string
}

interface SSEConnectionMetrics {
  phase: 'ttfb' | 'closed'
  ttfbMs?: number
  connectMs?: number
  tlsMs?: number
  connectionReused?: boolean
}

interface SSEStreamOptions {
  headers?: Record<string, string>
  session?: SSESessionOptions
}

interface SSESessionOptions {
  timeoutSeconds?: number
  maxConnectionsPerHost?: number
}
```

### Configuring the shared session

The underlying `URLSession`/`OkHttpClient` is shared by every `SSEStream` in the app and, once created, kept alive for the app's lifetime — that's the whole mechanism behind connection reuse. Because of that, this config only takes effect once, on whichever `connect()` call ends up being the very first one made across the whole app; after that, the shared client already exists and any further attempt to set it is a no-op.

Rather than relying on "whichever stream happens to connect first" and passing `session` there, call `configureSSESession()` once at app startup — e.g. at the top of `App.tsx`, before any screen creates a stream:

```ts
// App.tsx
import { configureSSESession } from 'react-native-sse-bridge-client'

configureSSESession({ timeoutSeconds: 1800, maxConnectionsPerHost: 4 })

export default function App() {
  // screens create/connect their own streams from here on, all sharing this config
  ...
}
```

Every stream's `connect()` then picks this up automatically as its default. You can still pass `session` directly to a particular `connect()` call if you want that one call to override the app-wide default (it only actually applies if that call turns out to be the first one, same rule as above — a `console.warn` fires if it doesn't).

```ts
// lower-level escape hatch — same one-time-effect caveat as configureSSESession()
stream.connect(url, {
  session: { timeoutSeconds: 1800, maxConnectionsPerHost: 4 },
})
```

| Option | iOS | Android |
| --- | --- | --- |
| `timeoutSeconds` | `URLSessionConfiguration.timeoutIntervalForRequest` (default `3600`) | OkHttp `readTimeout` (default `0`, i.e. unlimited) |
| `maxConnectionsPerHost` | `URLSessionConfiguration.httpMaximumConnectionsPerHost` (default `6`) | OkHttp `Dispatcher.maxRequestsPerHost` — the closest equivalent; HTTP/2 hosts multiplex many requests over one connection regardless (default `5`, OkHttp's own default) |

### Reading `onMetrics`

Each connection reports metrics twice:

1. **`phase: 'ttfb'`** — fires the moment the first byte of the body arrives. Only `ttfbMs` is populated; use this for a real-time "how long did that take" readout.
2. **`phase: 'closed'`** — fires once the connection ends (you called `disconnect()`, or a new `connect()` superseded it). This carries the authoritative breakdown: `connectMs`, `tlsMs`, and `connectionReused`.

`connectMs`/`tlsMs` come back `undefined` — not `0` — when `connectionReused` is `true`. That's not missing data: it means the OS handed the request an already-open connection, so those phases genuinely didn't happen. A `0`-vs-`undefined` distinction here is the actual signal, which is why the fields are optional rather than defaulting to zero.

## How reconnects work

Reconnecting is entirely manual: call `connect()` again (optionally after `disconnect()`) whenever you want to. There is no built-in auto-reconnect or backoff on top of the server's `retry:` field — this is intentional for v1, so nothing sits between you and the exact moment a reconnect happens. If you need resilience against dropped connections, drive `connect()`/`disconnect()` from your own retry logic (e.g. on `onError`).

## Roadmap

- `Last-Event-ID` resumption on reconnect
- Optional built-in auto-reconnect with backoff, honoring the server's `retry:` field
- POST-based SSE (custom method/body)

Contributions toward any of these are welcome — see [Contributing](#contributing).

## How it's built

- **iOS**: a single `URLSession` (not `.shared`) created once and reused for every `connect()`/`disconnect()` cycle across every stream, so the connection pool persists across reconnects. SSE framing is parsed by hand, byte-level, from the streamed response body — no third-party SSE library. Handshake/TLS timings come from `URLSessionTaskMetrics`.
- **Android**: a single `OkHttpClient` created once, likewise reused across reconnects and streams. Requests go through `client.newCall(request).enqueue(...)` with the response body read and parsed manually — **not** through `okhttp-sse`'s `EventSource`, because `RealEventSource.connect()` internally does `client.newBuilder().eventListener(...)`, which silently replaces any `eventListenerFactory` you set on the client, making handshake timing impossible to observe through it. Handshake/TLS timings come from OkHttp's `EventListener`.
- **Multiplexing**: classic Native Modules can't be instantiated per-JS-object the way a Nitro `HybridObject` can, so every native method takes a `streamId` (generated in JS) and every emitted event carries it back — the JS-side `SSEStream` class filters the shared event emitter down to just its own stream.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you'd like to change.

## License

MIT
