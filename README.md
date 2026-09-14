# react-native-sse-bridge-client

A native Server-Sent Events (SSE) client for React Native's classic bridge — for apps that can't use the New Architecture / Nitro Modules (older React Native versions, or New Architecture explicitly disabled).

[![Version](https://img.shields.io/npm/v/react-native-sse-bridge-client.svg)](https://www.npmjs.com/package/react-native-sse-bridge-client)
[![License](https://img.shields.io/npm/l/react-native-sse-bridge-client.svg)](https://github.com/TaranVitalii/react-native-sse-bridge-client/blob/main/LICENSE)

## Why

Most React Native SSE clients (including the popular `react-native-sse`) are built on top of `XMLHttpRequest`. Every reconnect opens a brand new HTTP request from scratch, which means a full TCP + TLS handshake every time — typically 300–600ms of latency the user sees on every reconnect, even to a server they were just talking to a second ago.

This library talks to the platform's native HTTP stack directly — `URLSession` on iOS, `OkHttp` on Android — which both keep a warm connection pool. As long as the same underlying client is reused across `connect()` calls (which it is, internally), a reconnect to the same host reuses the existing HTTP/2 connection instead of re-handshaking. `onMetrics` reports real, measured proof of this on every connection: whether it was a fresh handshake or a reused one.

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

// error.type is 'http' | 'network' | 'timeout' | 'exception'; error.statusCode is set for 'http'
stream.onError(error => console.log('error:', error.type, error.message))

// fires whenever the connection ends, for any reason — including the automatic reconnect this
// library does by default, so this doesn't mean the stream gave up
stream.onClose(() => console.log('closed'))

// fires once, when the connection closes (disconnect(), a superseding connect(), or a failure)
stream.onMetrics(metrics => console.log('connection reused:', metrics.connectionReused))

// state is 'idle' | 'connecting' | 'open' | 'reconnecting' | 'paused' | 'closed' | 'failed'
stream.onStateChange(state => console.log('state:', state))

stream.connect('https://your-server.example.com/events', {
  headers: { Authorization: 'Bearer …' },
})

// later
stream.disconnect()
// or, once you're done with this stream entirely:
stream.destroy()
```

### POST requests

Some SSE APIs — most LLM chat-completion endpoints included — stream the response to a `POST`
whose body carries the request payload, rather than a plain `GET`. Pass `method`/`body`:

```ts
stream.connect('https://your-server.example.com/chat/completions', {
  method: 'POST',
  headers: {
    Authorization: 'Bearer …',
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({ model: 'your-model', messages, stream: true }),
})
```

### Refreshing headers before a request

`onBeforeRequest` is awaited immediately before every request this stream makes — the initial
`connect()` and every automatic reconnect alike — so it's the right place to refresh a short-lived
auth token, rather than a reconnect firing (and getting rejected) with a stale one:

```ts
stream.onBeforeRequest = async () => {
  const token = await getFreshAccessToken()
  return { Authorization: `Bearer ${token}` }
}

stream.connect('https://your-server.example.com/events')
```

Whatever headers it resolves with are merged over the `connect()`-time `headers`, resolved values
winning on a key collision — so a stream can rely entirely on `onBeforeRequest` for auth and skip
`headers` altogether, as above. If the returned promise rejects, the request proceeds anyway
without the extra headers (a broken token-refresh hook shouldn't block reconnecting outright).

## API

### `createSSEStream(): SSEStream`

Creates a new, independent stream. Classic Native Modules are singletons, so unlike a plain JSI HybridObject there's only ever one native module underneath — but every `SSEStream` you create gets its own connection, its own listeners, and its own lifecycle. Create as many as you need; they share the native connection pool, so reconnecting one doesn't disturb the others.

### `configureSSESession(options: SSESessionOptions): void`

Sets the shared session config (timeout, max connections per host) once, up front. Call this at app startup, before any screen creates/connects a stream — see [Configuring the shared session](#configuring-the-shared-session) below.

### `SSEStream` methods

| Method | Description |
| --- | --- |
| `connect(url: string, options?: SSEStreamOptions): void` | Opens a connection to `url`, `GET` by default — pass `options.method`/`options.body` for a POST-based SSE API (most LLM chat-completion endpoints). Calling this again on the same stream cancels the previous connection first (its close metrics still fire). Headers — including `User-Agent` — are entirely JS-configured; nothing is hardcoded natively. |
| `disconnect(): void` | Closes the current connection, if any. |
| `addEventListener(type: string, callback: (event: SSEMessageEvent) => void): () => void` | Subscribes to a specific SSE `event:` type, mirroring the browser `EventSource` model — frames with no `event:` field (or `event: message`) are filed under `'message'`. Returns an unsubscribe function. |
| `onOpen(callback: () => void): () => void` | Fires when the server responds with a successful (2xx) status. |
| `onError(callback: (error: SSEError) => void): () => void` | Fires on a non-2xx HTTP response, a Content-Type mismatch (unless `validateContentType: false`), a network/transport failure, a timeout, or an invalid URL — see [Types](#types) below for `SSEError`. Not called for a `disconnect()` you initiated yourself. |
| `onClose(callback: () => void): () => void` | Fires whenever the connection ends, for any reason — a normal server-side close, right after `onError`, or an explicit `disconnect()`. Fires again after every automatic reconnect's connection ends, so it does not mean the stream gave up. |
| `onMetrics(callback: (metrics: SSEConnectionMetrics) => void): () => void` | Fires once per connection, when it ends — see below. Like `addEventListener`, whether this has any listener is pushed down natively; with none, the event is dropped before it crosses the bridge. |
| `onStateChange(callback: (state: SSEConnectionState) => void): () => void` | Fires on every connection-state transition — see [Connection state](#connection-state) below. Only fires when the state actually changes. |
| `getState(): SSEConnectionState` | The stream's current connection state — see [Connection state](#connection-state) below. Always up to date; doesn't require an `onStateChange` listener to be registered. |
| `onBeforeRequest = hook` (settable property) | Awaited immediately before every request — the initial `connect()` and every automatic reconnect alike. See [Refreshing headers before a request](#refreshing-headers-before-a-request) above. Assigning replaces any previously set hook (unlike the other callbacks above, only one hook is meaningful at a time). |
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
  connectionReused: boolean
}

// 'http': non-2xx response — statusCode and message (the response body) are populated.
// 'invalid-content-type': a 2xx response whose Content-Type wasn't text/event-stream (only
// reported when validateContentType is true, the default) — message describes what was received.
// 'timeout': the request's own timeout (SSESessionOptions.timeoutSeconds) elapsed.
// 'network': a transport-level failure (DNS, connection refused, TLS, dropped connection, etc.).
// 'exception': the call couldn't even be attempted (e.g. an invalid URL).
type SSEErrorType = 'http' | 'invalid-content-type' | 'network' | 'timeout' | 'exception'

interface SSEError {
  message: string
  type: SSEErrorType
  statusCode?: number // only set when type is 'http'
}

interface SSEStreamOptions {
  headers?: Record<string, string>
  session?: SSESessionOptions
  reconnect?: SSEReconnectOptions
  method?: string // default 'GET'; use 'POST' (with `body`) for APIs that stream to a request body
  body?: string // raw request body (e.g. JSON.stringify(...)); set Content-Type via `headers`
  validateContentType?: boolean // default true — see SSEErrorType 'invalid-content-type' above
}

interface SSESessionOptions {
  timeoutSeconds?: number
  maxConnectionsPerHost?: number
}

interface SSEReconnectOptions {
  enabled?: boolean // default true
  // Base delay before the first reconnect attempt, in ms. Default 3000; overridden per-stream by
  // a server `retry:` field. Each consecutive failed attempt doubles the delay from here — see
  // maxIntervalMs/jitterFactor — this is a starting point, not a flat per-attempt delay.
  intervalMs?: number
  maxIntervalMs?: number // cap on the exponential backoff delay, in ms. Default 30000
  // Randomizes each computed delay by this fraction (0.0-1.0) — e.g. 0.5 turns a computed 4000ms
  // delay into a random value in [3000, 5000], so many clients don't retry in lockstep after a
  // shared outage. Default 0.5. 0 disables jitter.
  jitterFactor?: number
  maxAttempts?: number // default undefined (retry forever); resets to 0 after a successful onOpen
  // Default false. A 4xx response or a Content-Type mismatch does NOT trigger a reconnect by
  // default (except 429, which always retries) — that class of failure usually means retrying
  // identically won't help. Set true to retry every HTTP error, including 4xx.
  retryOnClientError?: boolean
  // Default true. Pauses reconnecting (instead of retrying into a dead network) whenever the
  // device has no network connectivity at all, resuming immediately once it's back — see
  // "Network-aware pause/resume" below.
  monitorNetwork?: boolean
}

// 'idle': never connected, or destroy()ed — the initial state.
// 'connecting': an explicit connect() call's first attempt is in flight.
// 'open': the connection is live, after onOpen.
// 'reconnecting': an automatic retry is pending (waiting out the backoff delay) or in flight.
// 'paused': reconnecting is on hold — the device currently has no network connectivity. Resumes
// automatically the instant connectivity returns.
// 'closed': ended intentionally — disconnect(), or the connection ended while reconnect.enabled
// was false.
// 'failed': automatic reconnect gave up — a non-retryable error, or reconnect.maxAttempts was
// reached. A fresh connect() is needed to try again.
type SSEConnectionState =
  | 'idle'
  | 'connecting'
  | 'open'
  | 'reconnecting'
  | 'paused'
  | 'closed'
  | 'failed'
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

Fires once per connection, when it ends (you called `disconnect()`, a new `connect()` superseded it, or it failed) — `connectionReused` is only knowable at that point, not any earlier. On iOS it comes from `URLSessionTaskMetrics`, which the OS only hands over once the task has fully finished; there's no way to report this alongside `onOpen` on either platform.

`connectionReused: true` means the OS handed this connection an already-open TCP/TLS session from the pool instead of doing a fresh handshake — the thing this whole library exists to make happen. `false` on every reconnect to the same host would mean something's wrong (a new `URLSession`/`OkHttpClient` being created somewhere, a host header mismatch, etc.).

A full per-phase timing breakdown (DNS/connect/TLS/TTFB) is still logged natively (`NSLog` on iOS, `Log.d` on Android, tag `BridgeSSE`) for whoever's debugging the library itself — it's just not sent across the bridge, since a granular breakdown isn't something most consumers of the library need.

## How reconnects work

Reconnecting is automatic by default, mirroring the browser `EventSource` model (and `react-native-sse`): whenever a connection ends for any reason other than your own `disconnect()` — a non-2xx response, a network/timeout error, or the server just closing the stream normally — the stream reconnects to the same URL after a delay.

- **Delay**: exponential backoff with jitter, starting at `reconnect.intervalMs` (default `3000`). Each consecutive failed attempt doubles the delay, capped at `reconnect.maxIntervalMs` (default `30000`), then randomized by `reconnect.jitterFactor` (default `0.5`) — e.g. attempts go roughly `3000ms → 6000ms → 12000ms → ...`, each jittered by ±25% (half of `jitterFactor`), up to the cap. A `retry:` field in the stream overrides the base (`intervalMs`) for that stream's *next* reconnects, and backoff resumes doubling from there. A successful `onOpen` resets the attempt counter, so the next failure starts back at the base delay.
- **`Last-Event-ID`**: if any received event had an `id:` field, it's sent as the `Last-Event-ID` header on the next automatic reconnect, so a server that supports it can resume from where it left off. An explicit `connect()` call always starts a fresh logical session — it does not send a stale `Last-Event-ID` from before.
- **Giving up**: set `reconnect.maxAttempts` to stop retrying after that many consecutive failures (default: retry forever). The counter resets to 0 after any successful `onOpen`. Giving up moves the stream to the `'failed'` state — see [Connection state](#connection-state) below.
- **Client errors**: a 4xx response or a Content-Type mismatch (see `validateContentType`) does **not** trigger a reconnect by default — retrying an identical request against a 401/403/404/etc. usually just repeats the same failure. The one default exception is `429` (rate limited), which always retries. Set `reconnect.retryOnClientError: true` to retry every HTTP error, including 4xx. 5xx, network, and timeout errors always retry (subject to `maxAttempts`), regardless of this setting.
- **Opting out**: `stream.connect(url, { reconnect: { enabled: false } })` disables it entirely — call `connect()` yourself (e.g. from `onError`/`onClose`) to drive reconnection your own way.
- **Network awareness**: while offline, reconnecting pauses entirely rather than retrying into a dead network — see [Network-aware pause/resume](#network-aware-pauseresume) below.

```ts
stream.connect(url, {
  reconnect: { intervalMs: 1000, maxIntervalMs: 20000, jitterFactor: 0.3, maxAttempts: 10 },
})
```

## Network-aware pause/resume

By default (`reconnect.monitorNetwork: true`), each stream watches the device's system-wide network reachability (`NWPathMonitor` on iOS, `ConnectivityManager` on Android) — not just "did this particular request fail," but "does the device have any network connectivity at all":

- Whenever the device is offline, a reconnect that would otherwise start a backoff timer moves to the `'paused'` state and waits instead — there's no point burning battery retrying into a network that isn't there.
- A connection that's currently `'open'` (or a reconnect already in flight) is proactively torn down and paused too, rather than waiting for the OS to eventually notice and time out.
- The instant connectivity returns, a paused stream reconnects immediately — bypassing the backoff delay — with a fresh attempt budget (`reconnectAttempts` resets to 0, so `maxAttempts` doesn't get consumed by a real connectivity gap that had nothing to do with the server).

An explicit `connect()` call always attempts regardless of current network status — this only ever pauses a stream that would otherwise be *automatically reconnecting*. Set `reconnect.monitorNetwork: false` to disable and let every reconnect go through the normal backoff/`maxAttempts` path unconditionally, matching the library's behavior before this feature existed.

```ts
stream.connect(url, {
  reconnect: { monitorNetwork: false }, // e.g. you already handle connectivity elsewhere
})
```

On Android this requires the `android.permission.ACCESS_NETWORK_STATE` permission, which the library declares in its own manifest (merged into your app's automatically) — most React Native apps already have it via other dependencies.

## Connection state

`getState()`/`onStateChange` expose the stream's connection lifecycle as an explicit `SSEConnectionState` — handy for driving a "reconnecting…" indicator without piecing it together from `onOpen`/`onError`/`onClose` yourself:

```
idle ──connect()──> connecting ──onOpen──> open
                        │                    │
                        │ (error/close)      │ (error/close)
                        ▼                    ▼
                    reconnecting <───────────┘
                     │   │    │
      (retryable,    │   │    │ (non-retryable, or
       under          │   │    │  maxAttempts reached)
       maxAttempts,   │   │    ▼
       online)        │   │  failed
                       │   │
       (offline)       │   └──────────┐
                       ▼               ▼
                     open           paused ──(connectivity returns)──> reconnecting

disconnect() (from any state) ──> closed
reconnect.enabled: false, connection ends ──> closed
offline while open/reconnecting (monitorNetwork) ──> paused
```

- `'reconnecting'` covers both "waiting out the backoff delay" and "the retry attempt itself in flight" — it doesn't flip back to `'connecting'` for each individual attempt.
- `'paused'` means reconnecting is on hold for lack of any network connectivity — see [Network-aware pause/resume](#network-aware-pauseresume) above. Resumes into `'reconnecting'` automatically.
- `'failed'` is terminal for that logical session — a client error (`retryOnClientError` not set) or an exhausted `maxAttempts` gave up. Call `connect()` again to start a fresh session.
- `onStateChange` only fires when the state actually changes — no duplicate events for repeated transitions into the same state.

## How it's built

- **iOS**: a single `URLSession` (not `.shared`) created once and reused for every `connect()`/`disconnect()` cycle across every stream, so the connection pool persists across reconnects. SSE framing is parsed by hand, byte-level, from the streamed response body — no third-party SSE library. Handshake/TLS timings come from `URLSessionTaskMetrics`.
- **Android**: a single `OkHttpClient` created once, likewise reused across reconnects and streams. Requests go through `client.newCall(request).enqueue(...)` with the response body read and parsed manually — **not** through `okhttp-sse`'s `EventSource`, because `RealEventSource.connect()` internally does `client.newBuilder().eventListener(...)`, which silently replaces any `eventListenerFactory` you set on the client, making handshake timing impossible to observe through it. Handshake/TLS timings come from OkHttp's `EventListener`.
- **Multiplexing**: classic Native Modules can't be instantiated per-JS-object the way a Nitro `HybridObject` can, so every native method takes a `streamId` (generated in JS) and every emitted event carries it back — the JS-side `SSEStream` class filters the shared event emitter down to just its own stream.
- **`onBeforeRequest`**: the classic bridge has no built-in way for native to call into JS and await a Promise result the way Nitro's HybridObject callbacks can — `RCTEventEmitter` only sends events one-way (native → JS). This is worked around with a hand-rolled round trip: instead of firing a request immediately, native emits an `onBeforeRequest` event carrying a globally unique `requestId`; JS resolves the hook and calls a `provideRequestHeaders(streamId, requestId, headers)` method back into native, which only fires the request if `requestId` still matches the attempt it's currently waiting on (a newer `connect()`/reconnect/`disconnect()` in between makes it stale, and it's dropped). A 10-second native-side timeout fires the request anyway if JS never calls back, so a broken hook can't hang a stream forever.
- **Network monitoring**: one `NWPathMonitor`/`ConnectivityManager.NetworkCallback` per stream (not a shared/broadcast monitor across every stream) — simpler and safer, at the cost of one lightweight monitor per concurrent stream. Each monitor ignores its own *first* status callback (only establishes a baseline) before reacting to any *change* from it, specifically to avoid a false-immediate-restart bug the reference implementation (`react-native-nitro-sse`) hit and documented fixing in its own changelog.

## License

MIT
