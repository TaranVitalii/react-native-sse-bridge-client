import { NativeEventEmitter, NativeModules } from 'react-native';

const { SSEBridgeClient: SSEBridgeClientNative } = NativeModules;

if (!SSEBridgeClientNative) {
  throw new Error(
    'SSEBridgeClient native module is not linked. Did you rebuild the app?',
  );
}

export interface SSEMessageEvent {
  id?: string;
  event: string;
  data: string;
}

/**
 * 'http': the server responded, but with a non-2xx status — `statusCode` and `message` (the
 * response body, if any) are populated.
 * 'invalid-content-type': the server responded 2xx, but with a Content-Type other than
 * `text/event-stream` — usually a misconfigured server/proxy (a login redirect page, a JSON
 * error body dressed up as 200, etc). `message` describes what was received.
 * 'timeout': the request's own timeout (SSESessionOptions.timeoutSeconds) elapsed with no
 * response.
 * 'network': a transport-level failure (DNS, connection refused, TLS, dropped connection, etc.)
 * — `message` is the OS's own error description.
 * 'exception': the call couldn't even be attempted (e.g. an invalid URL).
 */
export type SSEErrorType =
  | 'http'
  | 'invalid-content-type'
  | 'network'
  | 'timeout'
  | 'exception';

export interface SSEError {
  message: string;
  type: SSEErrorType;
  /** Only set when type is 'http'. */
  statusCode?: number;
}

/**
 * Fires once, when a connection ends (you called disconnect(), a new connect() superseded it,
 * or it failed) — `connectionReused` is only knowable at that point: on iOS it comes from
 * URLSessionTaskMetrics, which the OS only hands over once the task has fully finished, so
 * there's no way to report this any earlier (e.g. alongside onOpen) on either platform.
 */
export interface SSEConnectionMetrics {
  connectionReused: boolean;
}

/**
 * Governs automatic reconnection after a connection ends for any reason (error, or the server
 * closing the stream) other than an explicit disconnect(). Mirrors the browser EventSource /
 * react-native-sse model, with exponential backoff + jitter layered on top (see intervalMs,
 * maxIntervalMs, jitterFactor) rather than react-native-sse's flat delay.
 */
export interface SSEReconnectOptions {
  /** Default true. */
  enabled?: boolean;
  /**
   * Base delay before the first reconnect attempt, in ms. Default 3000. A `retry:` field in the
   * stream overrides this for that stream's subsequent reconnects (until connect() is called
   * again explicitly, which resets it back to this value). Each consecutive failed attempt after
   * the first doubles the delay (see maxIntervalMs, jitterFactor) — this is the starting point,
   * not a flat per-attempt delay.
   */
  intervalMs?: number;
  /**
   * Cap on the exponential backoff delay, in ms. Default 30000. Once doubling from intervalMs
   * would exceed this, the delay stays at this value for every subsequent attempt.
   */
  maxIntervalMs?: number;
  /**
   * Randomizes each computed backoff delay by this fraction (0.0-1.0), so e.g. a jitterFactor of
   * 0.5 turns a computed 4000ms delay into a random value in [3000, 5000]. Default 0.5 — this
   * spreads out reconnect attempts from many clients hitting the same outage at once ("thundering
   * herd"), so they don't all retry in lockstep. 0 disables jitter (exact exponential delay).
   */
  jitterFactor?: number;
  /**
   * Stop reconnecting after this many consecutive failed attempts. Default undefined (retry
   * forever). Resets to 0 after any successful onOpen.
   */
  maxAttempts?: number;
  /**
   * By default, a 4xx response (client error) or a Content-Type mismatch does NOT trigger a
   * reconnect — that class of failure reflects something wrong with the request/server config
   * that retrying identically won't fix. The one exception retried by default regardless is 429
   * (rate limited — a deliberate "back off and try again" signal). 5xx, network, and timeout
   * errors always retry (subject to maxAttempts). Set true to retry every HTTP error, including
   * 4xx.
   */
  retryOnClientError?: boolean;
  /**
   * Default true. While enabled, this stream watches the device's system-wide network
   * reachability (NWPathMonitor on iOS, ConnectivityManager on Android) and, whenever it's
   * offline:
   * - an automatic reconnect that would otherwise start a backoff timer instead moves straight to
   *   the 'paused' state (see SSEConnectionState) and waits — no point burning battery retrying
   *   into a dead network:
   * - a connection that's currently open (or a reconnect that's already in flight) is proactively
   *   torn down and paused too, rather than waiting for the OS to eventually notice and time out.
   * The instant connectivity returns, a paused stream reconnects immediately (bypassing the
   * backoff delay) with a fresh attempt budget (reconnectAttempts resets to 0, so maxAttempts
   * doesn't carry over a real connectivity gap). Only ever pauses a stream that would otherwise be
   * reconnecting — an explicit connect() call always attempts regardless of network status. Set
   * false to disable and let every reconnect go through the normal backoff/maxAttempts path
   * unconditionally, matching pre-network-monitoring behavior.
   */
  monitorNetwork?: boolean;
}

/**
 * 'idle': never connected, or destroy()ed — the initial state.
 * 'connecting': an explicit connect() call's first attempt is in flight, before its first
 * onOpen/onError.
 * 'open': the connection is live, after onOpen.
 * 'reconnecting': an automatic retry is pending (waiting out the backoff delay) or in flight,
 * after the connection ended for a reason SSEReconnectOptions allows retrying.
 * 'paused': reconnecting is on hold because the device currently has no network connectivity —
 * see SSEReconnectOptions.monitorNetwork. Resumes automatically (and immediately) the instant
 * connectivity returns.
 * 'closed': ended intentionally — an explicit disconnect(), or the connection ended while
 * reconnect.enabled was false.
 * 'failed': automatic reconnect gave up on this connect() session — either a non-retryable error
 * (see SSEReconnectOptions.retryOnClientError) or reconnect.maxAttempts was reached. A fresh
 * connect() call is needed to try again.
 */
export type SSEConnectionState =
  | 'idle'
  | 'connecting'
  | 'open'
  | 'reconnecting'
  | 'paused'
  | 'closed'
  | 'failed';

/**
 * The underlying URLSession (iOS) / OkHttpClient (Android) is shared by every SSEStream and,
 * once created, kept alive for the app's lifetime — that's what lets a reconnect reuse the
 * pooled HTTP/2 connection instead of re-handshaking. Because of that, this config only takes
 * effect once, on whichever connect() call ends up being the very first one made across ALL
 * streams in the app; after that, the shared client already exists and this is ignored
 * (recreating it would defeat the whole point — every connection already in the pool would be
 * dropped). Prefer configureSSESession() over passing `session` to connect() directly — it removes
 * the guesswork of "which stream connects first" by setting this once, up front, at app startup.
 */
export interface SSESessionOptions {
  /** Request timeout in seconds. Defaults to 3600 on iOS, unlimited (0) on Android. */
  timeoutSeconds?: number;
  /**
   * Max concurrent connections to a single host. Defaults to 6 on iOS. On Android this maps to
   * OkHttp's `Dispatcher.maxRequestsPerHost` (its closest equivalent — HTTP/2 hosts multiplex
   * many requests over one connection regardless), defaulting to OkHttp's own default (5).
   */
  maxConnectionsPerHost?: number;
}

export interface SSEStreamOptions {
  headers?: Record<string, string>;
  /** Only takes effect on the first connect() made across all streams — see SSESessionOptions. */
  session?: SSESessionOptions;
  /** Automatic reconnect after the connection ends (error, or the server closing the stream).
   * On by default — see SSEReconnectOptions. */
  reconnect?: SSEReconnectOptions;
  /** Default 'GET'. Use 'POST' (with `body`) for APIs that stream SSE responses to a request
   * body — e.g. most LLM chat-completion endpoints. */
  method?: string;
  /** Sent as the raw request body (e.g. `JSON.stringify(...)`). Set your own `Content-Type` via
   * `headers` — none is assumed. */
  body?: string;
  /** Default true. A non-`text/event-stream` Content-Type on an otherwise-successful response is
   * reported via onError (type 'invalid-content-type') instead of being treated as open. Set
   * false for a server that's valid SSE but sends a different/no Content-Type. */
  validateContentType?: boolean;
}

type Unsubscribe = () => void;

let nextStreamId = 0;
// Tracks whether ANY stream has ever called connect() — the shared native session/client is
// created lazily on that very first call, so this is the JS-side mirror of "does the session
// already exist" used to warn when a later connect()'s `session` options can't take effect.
let sharedSessionCreated = false;
// Set by configureSSESession(), applied to whichever connect() call ends up being the first one
// across the app — see configureSSESession() below.
let defaultSessionOptions: SSESessionOptions | undefined;

/**
 * Sets the shared session config (timeout, max connections per host) once, up front — call this
 * at app startup (e.g. in App.tsx, before any screen creates/connects a stream) instead of
 * passing `session` to whichever connect() happens to run first. Every stream's connect() then
 * uses this as its default (an explicit `session` passed to connect() still wins for that call).
 *
 * Must be called before the first connect() anywhere in the app — the underlying session is
 * created lazily on that call, so calling this any later has nothing left to configure and
 * logs a warning instead of silently doing nothing.
 */
export function configureSSESession(options: SSESessionOptions): void {
  if (sharedSessionCreated) {
    console.warn(
      '[react-native-sse-bridge-client] configureSSESession() was called after a stream had already ' +
        'connected — the shared session/client already exists, so these options have no effect. ' +
        'Call configureSSESession() once at app startup, before creating or connecting any SSEStream.',
    );
    return;
  }
  defaultSessionOptions = options;
}

// SSE frames with no `event:` field are filed under this key, matching how browser
// EventSource treats them as type 'message'.
const DEFAULT_MESSAGE_TYPE = 'message';

interface NativeStreamPayload {
  streamId: string;
}

interface NativeMessagePayload {
  id?: string;
  event?: string;
  data: string;
}

interface NativeErrorPayload {
  message: string;
  type: SSEErrorType;
  statusCode?: number;
}

interface NativeStatePayload {
  state: SSEConnectionState;
}

interface NativeBeforeRequestPayload {
  requestId: number;
}

// Declared explicitly (rather than left to NativeEventEmitter's default generic) so
// addListener()'s callback parameter is typed as our actual payload shape instead of the
// permissive default `Object`.
type SSEBridgeEventArgs =
  | [NativeStreamPayload]
  | [NativeStreamPayload & NativeMessagePayload]
  | [NativeStreamPayload & NativeErrorPayload]
  | [NativeStreamPayload & SSEConnectionMetrics]
  | [NativeStreamPayload & NativeStatePayload]
  | [NativeStreamPayload & NativeBeforeRequestPayload];

interface SSEBridgeEventMap {
  onOpen: [NativeStreamPayload];
  onMessage: [NativeStreamPayload & NativeMessagePayload];
  onError: [NativeStreamPayload & NativeErrorPayload];
  onClose: [NativeStreamPayload];
  onMetrics: [NativeStreamPayload & SSEConnectionMetrics];
  onStateChange: [NativeStreamPayload & NativeStatePayload];
  onBeforeRequest: [NativeStreamPayload & NativeBeforeRequestPayload];
  [key: string]: SSEBridgeEventArgs;
}

// One shared native module + one shared JS emitter, multiplexed across SSEStream instances by
// streamId — classic Native Modules are singletons, so this is the only way to get several
// independent logical streams out of one native module.
const emitter = new NativeEventEmitter<SSEBridgeEventMap>(
  SSEBridgeClientNative,
);

/**
 * One independently connect()-able/disconnect()-able SSE stream. Create as many as you need —
 * each carries its own url/headers and its own listeners; they all share the native module's
 * connection pool, so reconnecting one doesn't disturb the others.
 *
 * Event-type filtering mirrors the browser EventSource model: addEventListener('message', cb)
 * only fires for frames with no `event:` field (or `event: message`); a server sending
 * `event: ping` heartbeats never reaches that callback unless you explicitly listen for 'ping'.
 *
 * The filtering happens natively, not just in JS: the set of types passed to addEventListener()
 * is pushed down to the native module (at connect() time, and live via setEventFilter() for any
 * addEventListener()/unsubscribe() call after that), so an event type nobody subscribed to is
 * dropped before it ever crosses the bridge — it never costs a JS-thread call.
 *
 * Reconnects automatically after the connection ends for any reason other than disconnect()
 * (mirrors browser EventSource / react-native-sse) — disable via `connect(url, { reconnect: {
 * enabled: false } })` if you'd rather handle that yourself.
 */
export class SSEStream {
  private readonly id = `sse-${nextStreamId++}`;
  private messageListeners = new Map<
    string,
    Set<(event: SSEMessageEvent) => void>
  >();
  private openListeners = new Set<() => void>();
  private errorListeners = new Set<(error: SSEError) => void>();
  private closeListeners = new Set<() => void>();
  private metricsListeners = new Set<(metrics: SSEConnectionMetrics) => void>();
  private stateListeners = new Set<(state: SSEConnectionState) => void>();
  // Tracked internally (regardless of whether the caller ever calls onStateChange()) so
  // getState() has an answer synchronously, without a round-trip to native.
  private cachedState: SSEConnectionState = 'idle';
  // A single hook, not a Set like the listeners above — merging multiple onBeforeRequest hooks'
  // headers wouldn't have an obviously correct behavior, so (matching react-native-nitro-sse-
  // client) this replaces any previously set hook rather than adding another one.
  private beforeRequestHook?: () => Promise<Record<string, string>>;
  private nativeSubs: { remove: () => void }[];
  private destroyed = false;
  private connected = false;

  constructor() {
    this.nativeSubs = [
      emitter.addListener('onOpen', (body: NativeStreamPayload) => {
        if (body.streamId !== this.id) {
          return;
        }
        this.openListeners.forEach((cb) => cb());
      }),
      emitter.addListener(
        'onMessage',
        (body: NativeStreamPayload & NativeMessagePayload) => {
          if (body.streamId !== this.id) {
            return;
          }
          this.dispatchMessage(body);
        },
      ),
      emitter.addListener(
        'onError',
        (body: NativeStreamPayload & NativeErrorPayload) => {
          if (body.streamId !== this.id) {
            return;
          }
          const error: SSEError = {
            message: body.message,
            type: body.type,
            statusCode: body.statusCode,
          };
          this.errorListeners.forEach((cb) => cb(error));
        },
      ),
      emitter.addListener('onClose', (body: NativeStreamPayload) => {
        if (body.streamId !== this.id) {
          return;
        }
        this.closeListeners.forEach((cb) => cb());
      }),
      emitter.addListener(
        'onMetrics',
        (body: NativeStreamPayload & SSEConnectionMetrics) => {
          if (body.streamId !== this.id) {
            return;
          }
          const metrics: SSEConnectionMetrics = {
            connectionReused: body.connectionReused,
          };
          this.metricsListeners.forEach((cb) => cb(metrics));
        },
      ),
      emitter.addListener(
        'onStateChange',
        (body: NativeStreamPayload & NativeStatePayload) => {
          if (body.streamId !== this.id) {
            return;
          }
          this.cachedState = body.state;
          this.stateListeners.forEach((cb) => cb(body.state));
        },
      ),
      emitter.addListener(
        'onBeforeRequest',
        async (body: NativeStreamPayload & NativeBeforeRequestPayload) => {
          if (body.streamId !== this.id) {
            return;
          }
          let headers: Record<string, string> = {};
          try {
            headers = (await this.beforeRequestHook?.()) ?? {};
          } catch {
            // A hook that throws/rejects shouldn't block the request — proceed without the
            // extra headers rather than leaving native waiting (it has its own timeout too, but
            // there's no reason to wait for it here).
          }
          SSEBridgeClientNative.provideRequestHeaders(
            this.id,
            body.requestId,
            headers,
          );
        },
      ),
    ];
  }

  connect(url: string, options?: SSEStreamOptions): void {
    if (this.destroyed) {
      throw new Error('SSEStream has been destroyed');
    }
    // An explicit `session` on this call wins; otherwise fall back to whatever configureSSESession()
    // set at app startup, if anything.
    const session = options?.session ?? defaultSessionOptions;
    // Only warn when THIS call explicitly passed `session` and it's too late for it to apply —
    // silently falling back to the app-wide default (or to nothing) on a later stream is the
    // normal, expected case, not a mistake worth flagging.
    if (options?.session && sharedSessionCreated) {
      console.warn(
        '[react-native-sse-bridge-client] `session` options were ignored: the shared session/client ' +
          'was already created by an earlier connect() call (on this or another SSEStream). Session ' +
          'config only takes effect on the very first connect() made across the whole app — call ' +
          'configureSSESession() once at startup instead. See the README for details.',
      );
    }
    sharedSessionCreated = true;
    this.connected = true;
    SSEBridgeClientNative.connect(this.id, url, {
      ...options,
      session,
      eventTypes: Array.from(this.messageListeners.keys()),
      metricsEnabled: this.metricsListeners.size > 0,
      hasBeforeRequestListener: this.beforeRequestHook != null,
    });
  }

  private dispatchMessage(raw: NativeMessagePayload): void {
    const type = raw.event ?? DEFAULT_MESSAGE_TYPE;
    const event: SSEMessageEvent = { id: raw.id, event: type, data: raw.data };
    this.messageListeners.get(type)?.forEach((cb) => cb(event));
  }

  disconnect(): void {
    this.connected = false;
    SSEBridgeClientNative.disconnect(this.id);
  }

  addEventListener(
    type: string,
    callback: (event: SSEMessageEvent) => void,
  ): Unsubscribe {
    let set = this.messageListeners.get(type);
    if (!set) {
      set = new Set();
      this.messageListeners.set(type, set);
    }
    set.add(callback);
    this.pushEventFilter();
    return () => {
      set!.delete(callback);
      if (set!.size === 0) {
        this.messageListeners.delete(type);
      }
      this.pushEventFilter();
    };
  }

  // Only meaningful once connected — before that, connect() will read messageListeners' current
  // keys itself. An empty type list is sent as "no filter, forward everything" by the native
  // side, so a stream with zero addEventListener() calls still receives every frame.
  private pushEventFilter(): void {
    if (!this.connected) {
      return;
    }
    SSEBridgeClientNative.setEventFilter(
      this.id,
      Array.from(this.messageListeners.keys()),
    );
  }

  onOpen(callback: () => void): Unsubscribe {
    this.openListeners.add(callback);
    return () => this.openListeners.delete(callback);
  }

  onError(callback: (error: SSEError) => void): Unsubscribe {
    this.errorListeners.add(callback);
    return () => this.errorListeners.delete(callback);
  }

  /** Fires whenever the connection ends, for any reason — a normal server-side close, right
   * after onError, or an explicit disconnect(). Fires again after every automatic reconnect's
   * connection ends, so it does not mean the stream gave up. */
  onClose(callback: () => void): Unsubscribe {
    this.closeListeners.add(callback);
    return () => this.closeListeners.delete(callback);
  }

  // Like addEventListener()'s type filter, whether ANY onMetrics() listener exists is pushed
  // down to the native module — with zero listeners, the connectionReused event is dropped
  // natively before it ever crosses the bridge (it only fires once per connection anyway, so
  // this matters far less than message filtering, but it's free to do the same way).
  onMetrics(callback: (metrics: SSEConnectionMetrics) => void): Unsubscribe {
    this.metricsListeners.add(callback);
    this.pushMetricsEnabled();
    return () => {
      this.metricsListeners.delete(callback);
      this.pushMetricsEnabled();
    };
  }

  // Only meaningful once connected — before that, connect() reads metricsListeners' current
  // size itself.
  private pushMetricsEnabled(): void {
    if (!this.connected) {
      return;
    }
    SSEBridgeClientNative.setMetricsEnabled(
      this.id,
      this.metricsListeners.size > 0,
    );
  }

  /** Fires on every connection-state transition — see SSEConnectionState. */
  onStateChange(callback: (state: SSEConnectionState) => void): Unsubscribe {
    this.stateListeners.add(callback);
    return () => this.stateListeners.delete(callback);
  }

  /** The stream's current connection state — see SSEConnectionState. Always up to date; doesn't
   * require an onStateChange() listener to be registered. */
  getState(): SSEConnectionState {
    return this.cachedState;
  }

  /** Awaited immediately before every request this stream makes — the initial connect() and
   * every automatic reconnect alike — so it's the right place to refresh a short-lived auth
   * token rather than letting a reconnect fire with a stale one. Whatever headers it resolves
   * with are merged over the connect()-time headers (resolved values win on a key collision).
   * Assigning replaces any previously set hook; assign undefined to remove it. */
  set onBeforeRequest(
    hook: (() => Promise<Record<string, string>>) | undefined,
  ) {
    this.beforeRequestHook = hook;
    // Only meaningful once connected — before that, connect() reads beforeRequestHook itself.
    if (this.connected) {
      SSEBridgeClientNative.setBeforeRequestEnabled(this.id, hook != null);
    }
  }

  /** Disconnects, drops all listeners, and unsubscribes from the shared native emitter. */
  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    this.disconnect();
    this.nativeSubs.forEach((sub) => sub.remove());
    this.messageListeners.clear();
    this.openListeners.clear();
    this.errorListeners.clear();
    this.closeListeners.clear();
    this.metricsListeners.clear();
    this.stateListeners.clear();
  }
}

export function createSSEStream(): SSEStream {
  return new SSEStream();
}
