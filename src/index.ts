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
 * 'timeout': the request's own timeout (SSESessionOptions.timeoutSeconds) elapsed with no
 * response.
 * 'network': a transport-level failure (DNS, connection refused, TLS, dropped connection, etc.)
 * — `message` is the OS's own error description.
 * 'exception': the call couldn't even be attempted (e.g. an invalid URL).
 */
export type SSEErrorType = 'http' | 'network' | 'timeout' | 'exception';

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
 * react-native-sse model: reconnect is on by default, at a flat interval the server can override
 * per-stream via an SSE `retry:` field (no exponential backoff).
 */
export interface SSEReconnectOptions {
  /** Default true. */
  enabled?: boolean;
  /**
   * Delay before the first/next reconnect attempt, in ms. Default 3000. A `retry:` field in the
   * stream overrides this for that stream's subsequent reconnects (until connect() is called
   * again explicitly, which resets it back to this value).
   */
  intervalMs?: number;
  /**
   * Stop reconnecting after this many consecutive failed attempts. Default undefined (retry
   * forever). Resets to 0 after any successful onOpen.
   */
  maxAttempts?: number;
}

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

// Declared explicitly (rather than left to NativeEventEmitter's default generic) so
// addListener()'s callback parameter is typed as our actual payload shape instead of the
// permissive default `Object`.
type SSEBridgeEventArgs =
  | [NativeStreamPayload]
  | [NativeStreamPayload & NativeMessagePayload]
  | [NativeStreamPayload & NativeErrorPayload]
  | [NativeStreamPayload & SSEConnectionMetrics];

interface SSEBridgeEventMap {
  onOpen: [NativeStreamPayload];
  onMessage: [NativeStreamPayload & NativeMessagePayload];
  onError: [NativeStreamPayload & NativeErrorPayload];
  onClose: [NativeStreamPayload];
  onMetrics: [NativeStreamPayload & SSEConnectionMetrics];
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
  }
}

export function createSSEStream(): SSEStream {
  return new SSEStream();
}
