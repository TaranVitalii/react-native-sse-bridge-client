import { NativeEventEmitter, NativeModules } from 'react-native';

const { SSEBridgeClient: SSEBridgeClientNative } = NativeModules;

if (!SSEBridgeClientNative) {
  throw new Error(
    'SSEBridgeClient native module is not linked. Did you rebuild the app?',
  );
}

export type SSEMetricsPhase = 'ttfb' | 'closed';

export interface SSEMessageEvent {
  id?: string;
  event: string;
  data: string;
}

export interface SSEConnectionMetrics {
  phase: SSEMetricsPhase;
  ttfbMs?: number;
  connectMs?: number;
  tlsMs?: number;
  connectionReused?: boolean;
}

/**
 * The underlying URLSession (iOS) / OkHttpClient (Android) is shared by every SSEStream and,
 * once created, kept alive for the app's lifetime — that's what lets a reconnect reuse the
 * pooled HTTP/2 connection instead of re-handshaking. Because of that, `session` only takes
 * effect on the very first connect() call made across ALL streams in the app; once that shared
 * client exists, later streams' `session` options are silently ignored (recreating it would
 * defeat the whole point — every connection already in the pool would be dropped).
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
}

type Unsubscribe = () => void;

let nextStreamId = 0;

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

// Declared explicitly (rather than left to NativeEventEmitter's default generic) so
// addListener()'s callback parameter is typed as our actual payload shape instead of the
// permissive default `Object`.
type SSEBridgeEventArgs =
  | [NativeStreamPayload]
  | [NativeStreamPayload & NativeMessagePayload]
  | [NativeStreamPayload & { message: string }]
  | [NativeStreamPayload & SSEConnectionMetrics];

interface SSEBridgeEventMap {
  onOpen: [NativeStreamPayload];
  onMessage: [NativeStreamPayload & NativeMessagePayload];
  onError: [NativeStreamPayload & { message: string }];
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
 */
export class SSEStream {
  private readonly id = `sse-${nextStreamId++}`;
  private messageListeners = new Map<
    string,
    Set<(event: SSEMessageEvent) => void>
  >();
  private openListeners = new Set<() => void>();
  private errorListeners = new Set<(message: string) => void>();
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
        (body: NativeStreamPayload & { message: string }) => {
          if (body.streamId !== this.id) {
            return;
          }
          this.errorListeners.forEach((cb) => cb(body.message));
        },
      ),
      emitter.addListener(
        'onMetrics',
        (body: NativeStreamPayload & SSEConnectionMetrics) => {
          if (body.streamId !== this.id) {
            return;
          }
          const metrics: SSEConnectionMetrics = {
            phase: body.phase,
            ttfbMs: body.ttfbMs,
            connectMs: body.connectMs,
            tlsMs: body.tlsMs,
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
    this.connected = true;
    SSEBridgeClientNative.connect(this.id, url, {
      ...options,
      eventTypes: Array.from(this.messageListeners.keys()),
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

  onError(callback: (message: string) => void): Unsubscribe {
    this.errorListeners.add(callback);
    return () => this.errorListeners.delete(callback);
  }

  onMetrics(callback: (metrics: SSEConnectionMetrics) => void): Unsubscribe {
    this.metricsListeners.add(callback);
    return () => this.metricsListeners.delete(callback);
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
    this.metricsListeners.clear();
  }
}

export function createSSEStream(): SSEStream {
  return new SSEStream();
}
