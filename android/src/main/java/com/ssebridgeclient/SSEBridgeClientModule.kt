package com.ssebridgeclient

import android.os.Handler
import android.os.Looper
import android.util.Log
import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.ReactContextBaseJavaModule
import com.facebook.react.bridge.ReactMethod
import com.facebook.react.bridge.ReadableArray
import com.facebook.react.bridge.ReadableMap
import com.facebook.react.modules.core.DeviceEventManagerModule
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Connection
import okhttp3.Dispatcher
import okhttp3.EventListener
import okhttp3.Handshake
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okio.Buffer
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Proxy
import java.net.SocketTimeoutException
import java.util.concurrent.TimeUnit

private const val LOG_TAG = "BridgeSSE"
private const val DEFAULT_RECONNECT_INTERVAL_MS = 3000.0
private const val DEFAULT_MAX_RECONNECT_INTERVAL_MS = 30000.0
private const val DEFAULT_JITTER_FACTOR = 0.5

// Error responses (4xx/5xx) are typically small JSON/HTML bodies — bounded so a misbehaving
// server streaming an enormous error page can't grow this unboundedly before completion.
private const val MAX_ERROR_BODY_BYTES = 8192L

private val METHODS_REQUIRING_BODY = setOf("POST", "PUT", "PATCH", "PROPPATCH", "REPORT")

// If JS never calls provideRequestHeaders() back (a broken onBeforeRequest hook that never
// resolves/rejects, or a JS-side bug), the request fires anyway after this long rather than
// hanging the stream forever.
private const val BEFORE_REQUEST_TIMEOUT_MS = 10_000L

/**
 * 'idle': never connected, or destroy()ed — the initial state.
 * 'connecting': an explicit connect() call's first attempt is in flight.
 * 'open': the connection is live, after onOpen.
 * 'reconnecting': an automatic retry is pending (waiting out the backoff delay) or in flight.
 * 'closed': ended intentionally — disconnect(), or the connection ended while
 * reconnect.enabled was false.
 * 'failed': automatic reconnect gave up — a non-retryable error, or reconnect.maxAttempts was
 * reached. A fresh connect() is needed to try again.
 */
private enum class ConnectionState(val value: String) {
  IDLE("idle"),
  CONNECTING("connecting"),
  OPEN("open"),
  RECONNECTING("reconnecting"),
  CLOSED("closed"),
  FAILED("failed"),
}

private class CallTimings {
  var connectStart: Long? = null
  var connectEnd: Long? = null
  var secureConnectStart: Long? = null
  var secureConnectEnd: Long? = null
  var callStart: Long? = null
  var responseHeadersStart: Long? = null
  var connectionReused = false
}

// Tags the Request for a given connect() attempt so the shared client's EventListener (which
// only sees the OkHttp Call) can be correlated back to the same connection attempt when it
// closes. generation is globally unique (not per-stream) purely as a correlation key; streamId
// is what routes the resulting JS event back to the right SSEStream instance.
private data class ConnectionAttempt(val streamId: String, val generation: Int)

/**
 * Supports multiple concurrent streams. Classic Native Modules are singletons — JS can't get a
 * fresh native instance the way it can with Nitro's createHybridObject() — so instead every
 * method takes a `streamId` (generated in JS) and every emitted event carries it back, letting
 * the JS-side wrapper multiplex several independent logical streams over one native module. All
 * streams still share the same OkHttpClient, so the connection pool is shared across them too.
 */
class SSEBridgeClientModule(reactContext: ReactApplicationContext) :
  ReactContextBaseJavaModule(reactContext) {

  override fun getName(): String = "SSEBridgeClient"

  private class StreamState {
    var currentCall: Call? = null
    var connectStartedAt: Long? = null
    var firstByteLogged = false
    // Empty set = no filtering, forward every frame. Non-empty = only frames whose resolved
    // event type (explicit `event:` field, or "message" when absent) is in this set get
    // emitted — everything else (e.g. `event: ping` heartbeats) is dropped before it ever
    // reaches the bridge, so a chatty/unwanted event type never costs a JS-thread call.
    var eventFilter: Set<String> = emptySet()
    // false = no onMetrics() listener on the JS side for this stream, so the connectionReused
    // event is dropped before it crosses the bridge — same idea as eventFilter, just a single
    // flag since there's only one metrics event (fired once, on close) rather than a set of types.
    var metricsEnabled = false

    // Reconnect state. connectUrl/headers are remembered so an automatic reconnect can repeat
    // the same connect() call; reconnectEnabled/reconnectMaxAttempts come from the caller's
    // SSEReconnectOptions, resolved once per explicit connect(). reconnectIntervalMs starts at
    // options.intervalMs (default 3s) and is overridden per-stream by a `retry:` field from the
    // server; it resets back to the option's value on the next *explicit* connect().
    var connectUrl: String? = null
    var headers: Map<String, String>? = null
    var reconnectEnabled = true
    var reconnectIntervalMs = DEFAULT_RECONNECT_INTERVAL_MS
    var maxIntervalMs = DEFAULT_MAX_RECONNECT_INTERVAL_MS
    var jitterFactor = DEFAULT_JITTER_FACTOR
    var reconnectMaxAttempts: Double? = null
    var reconnectAttempts = 0
    var pendingReconnect: Runnable? = null
    var lastEventId: String? = null
    var currentState = ConnectionState.IDLE
    // By default, a 4xx response or a Content-Type mismatch does NOT trigger a reconnect (except
    // 429, always retried) — retrying an identical request usually just repeats the same failure.
    // Set true to retry every HTTP error, including 4xx.
    var retryOnClientError = false

    // Default "GET". "POST" (with `body`) is for SSE APIs that stream the response to a request
    // body, e.g. most LLM chat-completion endpoints.
    var method: String? = null
    var body: String? = null
    // Default true — a 2xx response whose Content-Type isn't text/event-stream is reported via
    // onError ('invalid-content-type') instead of being treated as an open stream.
    var validateContentType = true

    // onBeforeRequest support. Classic Native Modules have no built-in way to call into JS and
    // await a Promise result the way Nitro's HybridObject callbacks can — so this is a
    // hand-rolled round trip: performConnect() emits "onBeforeRequest" (with a globally unique
    // requestId, see SSEBridgeClientModule.nextBeforeRequestId) instead of firing the request
    // immediately; JS resolves its hook and calls back into provideRequestHeaders(streamId,
    // requestId, headers), which fires the request only if awaitingBeforeRequestId still matches
    // — i.e. this attempt hasn't been superseded by a newer connect()/reconnect (or disconnect())
    // in the meantime. pendingBeforeRequestIsReconnect remembers isReconnect across that gap,
    // since provideRequestHeaders needs it to decide whether to send Last-Event-ID.
    var hasBeforeRequestListener = false
    // null when not currently waiting on a round trip. requestId is handed out from a single
    // process-wide counter (never reset, never reused) specifically so a stale timeout/
    // provideRequestHeaders() callback from a superseded attempt can never coincidentally collide
    // with a legitimately-current one — unlike a per-stream counter, which would reset every time
    // a fresh StreamState replaces this one, e.g. across a disconnect() + reconnect().
    var awaitingBeforeRequestId: Int? = null
    var pendingBeforeRequestIsReconnect = false
    var pendingBeforeRequestTimeout: Runnable? = null
  }

  private val streams = mutableMapOf<String, StreamState>()
  private val timingsByGeneration = mutableMapOf<Int, CallTimings>()
  private var generationCounter = 0
  // Hands out globally unique onBeforeRequest round-trip IDs — see StreamState.awaitingBeforeRequestId.
  private var nextBeforeRequestId = 0
  private val mainHandler = Handler(Looper.getMainLooper())

  // Lazily created — see sharedClient(options:) below.
  private var client: OkHttpClient? = null

  // Shared by every stream and, once created, kept alive for the app's lifetime — that's what
  // lets a reconnect reuse the pooled HTTP/2 connection instead of re-handshaking. Because of
  // that, `options.session` can only take effect on the very first connect() call across ALL
  // streams; once the client exists, later streams' `session` options are silently ignored
  // (recreating it would defeat the whole point: every existing pooled connection would drop).
  private fun sharedClient(options: ReadableMap?): OkHttpClient {
    client?.let { return it }

    val sessionOptions = options?.getMap("session")
    val readTimeoutSeconds = if (sessionOptions?.hasKey("timeoutSeconds") == true) {
      sessionOptions.getDouble("timeoutSeconds")
    } else {
      0.0 // matches the previous hardcoded default: no timeout
    }
    // OkHttp has no direct equivalent of iOS's httpMaximumConnectionsPerHost; maxRequestsPerHost
    // is the closest analogue (concurrent requests to a single host), defaulting to OkHttp's own
    // built-in default when not specified.
    val maxRequestsPerHost = if (sessionOptions?.hasKey("maxConnectionsPerHost") == true) {
      sessionOptions.getInt("maxConnectionsPerHost")
    } else {
      Dispatcher().maxRequestsPerHost
    }

    val newClient = OkHttpClient.Builder()
      .readTimeout(readTimeoutSeconds.toLong(), TimeUnit.SECONDS)
      .dispatcher(Dispatcher().apply { this.maxRequestsPerHost = maxRequestsPerHost })
      .eventListenerFactory { call ->
        val attempt = call.request().tag(ConnectionAttempt::class.java)
        val timings = CallTimings()
        if (attempt != null) timingsByGeneration[attempt.generation] = timings

        object : EventListener() {
          override fun callStart(call: Call) {
            timings.callStart = System.nanoTime()
          }

          override fun connectStart(call: Call, inetSocketAddress: InetSocketAddress, proxy: Proxy) {
            timings.connectStart = System.nanoTime()
          }

          override fun secureConnectStart(call: Call) {
            timings.secureConnectStart = System.nanoTime()
          }

          override fun secureConnectEnd(call: Call, handshake: Handshake?) {
            timings.secureConnectEnd = System.nanoTime()
          }

          override fun connectEnd(call: Call, inetSocketAddress: InetSocketAddress, proxy: Proxy, protocol: Protocol?) {
            timings.connectEnd = System.nanoTime()
          }

          override fun connectionAcquired(call: Call, connection: Connection) {
            // If connectStart never fired for this call, OkHttp handed us an already-pooled
            // connection instead of opening a new one — that absence is the reuse signal.
            timings.connectionReused = timings.connectStart == null
          }

          override fun responseHeadersStart(call: Call) {
            timings.responseHeadersStart = System.nanoTime()
          }
        }
      }
      .build()

    client = newClient
    return newClient
  }

  @ReactMethod
  fun connect(streamId: String, url: String, options: ReadableMap?) {
    // Validated before touching any existing connection, so a bad URL on a reconnect attempt
    // doesn't tear down a connection that was working fine — and reported through onError like
    // any other connection failure, rather than left to crash as an uncaught IllegalArgumentException.
    try {
      Request.Builder().url(url)
    } catch (e: IllegalArgumentException) {
      Log.e(LOG_TAG, "[$streamId] invalid URL: $url", e)
      emitEvent(
        "onError",
        Arguments.createMap().apply {
          putString("streamId", streamId)
          putString("message", "Invalid URL: $url")
          putString("type", "exception")
        }
      )
      return
    }

    streams[streamId]?.pendingReconnect?.let { mainHandler.removeCallbacks(it) }
    streams[streamId]?.currentCall?.let { emitCloseMetrics(streamId, it) }
    streams[streamId]?.currentCall?.cancel()

    val state = StreamState()
    state.connectStartedAt = System.nanoTime()
    state.connectUrl = url
    state.headers = options?.getMap("headers")?.let { h ->
      val map = mutableMapOf<String, String>()
      val iterator = h.keySetIterator()
      while (iterator.hasNextKey()) {
        val key = iterator.nextKey()
        h.getString(key)?.let { map[key] = it }
      }
      map
    }
    val eventTypes = options?.getArray("eventTypes")
    if (eventTypes != null && eventTypes.size() > 0) {
      state.eventFilter = (0 until eventTypes.size()).mapNotNull { eventTypes.getString(it) }.toSet()
    }
    state.metricsEnabled = options?.hasKey("metricsEnabled") == true && options.getBoolean("metricsEnabled")

    val reconnectOptions = options?.getMap("reconnect")
    state.reconnectEnabled = if (reconnectOptions?.hasKey("enabled") == true) reconnectOptions.getBoolean("enabled") else true
    state.reconnectIntervalMs = if (reconnectOptions?.hasKey("intervalMs") == true) reconnectOptions.getDouble("intervalMs") else DEFAULT_RECONNECT_INTERVAL_MS
    state.maxIntervalMs = if (reconnectOptions?.hasKey("maxIntervalMs") == true) reconnectOptions.getDouble("maxIntervalMs") else DEFAULT_MAX_RECONNECT_INTERVAL_MS
    state.jitterFactor = if (reconnectOptions?.hasKey("jitterFactor") == true) reconnectOptions.getDouble("jitterFactor") else DEFAULT_JITTER_FACTOR
    state.reconnectMaxAttempts = if (reconnectOptions?.hasKey("maxAttempts") == true) reconnectOptions.getDouble("maxAttempts") else null
    state.retryOnClientError = reconnectOptions?.hasKey("retryOnClientError") == true && reconnectOptions.getBoolean("retryOnClientError")

    state.method = if (options?.hasKey("method") == true) options.getString("method") else null
    state.body = if (options?.hasKey("body") == true) options.getString("body") else null
    state.validateContentType = if (options?.hasKey("validateContentType") == true) options.getBoolean("validateContentType") else true

    state.hasBeforeRequestListener = options?.hasKey("hasBeforeRequestListener") == true &&
      options.getBoolean("hasBeforeRequestListener")

    streams[streamId] = state
    // `options` (session config) is only ever consulted on the very first connect() made across
    // ALL streams — see sharedClient(options:) — so it's fine that reconnects (which call
    // performConnect directly, not through here) don't have it to hand.
    sharedClient(options)

    setState(streamId, ConnectionState.CONNECTING)
    performConnect(streamId, isReconnect = false)
  }

  // Only fires onStateChange when the state actually changes — callers can transition through
  // the same state repeatedly (e.g. scheduleReconnectIfNeeded on every failed attempt) without
  // spamming duplicate events.
  private fun setState(streamId: String, newState: ConnectionState) {
    val state = streams[streamId] ?: return
    if (state.currentState == newState) return
    state.currentState = newState
    emitEvent(
      "onStateChange",
      Arguments.createMap().apply {
        putString("streamId", streamId)
        putString("state", newState.value)
      }
    )
  }

  private fun performConnect(streamId: String, isReconnect: Boolean) {
    val state = streams[streamId] ?: return

    // Invalidates any previous attempt still waiting on onBeforeRequest — see
    // provideRequestHeaders() and the timeout Runnable below.
    state.pendingBeforeRequestTimeout?.let { mainHandler.removeCallbacks(it) }
    state.pendingBeforeRequestTimeout = null
    state.awaitingBeforeRequestId = null

    if (!state.hasBeforeRequestListener) {
      // Common case: no hook registered — skip the round trip entirely and fire immediately.
      fireRequest(streamId, isReconnect, emptyMap())
      return
    }

    nextBeforeRequestId += 1
    val requestId = nextBeforeRequestId
    state.awaitingBeforeRequestId = requestId
    state.pendingBeforeRequestIsReconnect = isReconnect
    emitEvent(
      "onBeforeRequest",
      Arguments.createMap().apply {
        putString("streamId", streamId)
        putInt("requestId", requestId)
      }
    )

    val timeout = Runnable {
      if (streams[streamId]?.awaitingBeforeRequestId != requestId) return@Runnable
      streams[streamId]?.awaitingBeforeRequestId = null
      streams[streamId]?.pendingBeforeRequestTimeout = null
      fireRequest(streamId, isReconnect, emptyMap())
    }
    state.pendingBeforeRequestTimeout = timeout
    mainHandler.postDelayed(timeout, BEFORE_REQUEST_TIMEOUT_MS)
  }

  @ReactMethod
  fun setBeforeRequestEnabled(streamId: String, enabled: Boolean) {
    streams[streamId]?.hasBeforeRequestListener = enabled
  }

  // Called back by JS once its onBeforeRequest hook resolves (or throws — headers is null/empty
  // in that case, the request proceeds anyway rather than getting stuck). requestId must match
  // awaitingBeforeRequestId: if a newer connect()/reconnect (or a disconnect()) has since
  // superseded this attempt, this callback is stale and is dropped rather than firing an
  // out-of-date request. requestId is a process-wide, never-reused counter (see
  // nextBeforeRequestId), so a stale callback can never coincidentally match a legitimately
  // current one.
  @ReactMethod
  fun provideRequestHeaders(streamId: String, requestId: Int, headers: ReadableMap?) {
    val state = streams[streamId] ?: return
    if (state.awaitingBeforeRequestId != requestId) return
    state.awaitingBeforeRequestId = null
    state.pendingBeforeRequestTimeout?.let { mainHandler.removeCallbacks(it) }
    state.pendingBeforeRequestTimeout = null
    val resolvedHeaders = headers?.let { h ->
      val map = mutableMapOf<String, String>()
      val iterator = h.keySetIterator()
      while (iterator.hasNextKey()) {
        val key = iterator.nextKey()
        h.getString(key)?.let { map[key] = it }
      }
      map
    } ?: emptyMap()
    fireRequest(streamId, state.pendingBeforeRequestIsReconnect, resolvedHeaders)
  }

  private fun fireRequest(streamId: String, isReconnect: Boolean, extraHeaders: Map<String, String>) {
    val state = streams[streamId] ?: return
    val url = state.connectUrl ?: return
    val requestBuilder = Request.Builder().url(url)

    state.firstByteLogged = false
    state.connectStartedAt = System.nanoTime()
    generationCounter += 1
    val generation = generationCounter

    requestBuilder
      .header("Accept", "text/event-stream")
      .tag(ConnectionAttempt::class.java, ConnectionAttempt(streamId, generation))

    state.headers?.forEach { (key, value) -> requestBuilder.header(key, value) }
    // onBeforeRequest's result is applied last, so it can override anything above — e.g.
    // refreshing an Authorization header that connectHeaders set with a now-stale token.
    extraHeaders.forEach { (key, value) -> requestBuilder.header(key, value) }
    applyMethodAndBody(requestBuilder, state.method, state.body)

    // Only sent on an automatic reconnect that has actually seen an id: field — an explicit
    // connect() always starts a fresh logical session (lastEventId is unset on a fresh StreamState).
    if (isReconnect) {
      state.lastEventId?.let { requestBuilder.header("Last-Event-ID", it) }
    }

    val call = sharedClient(null).newCall(requestBuilder.build())
    state.currentCall = call

    call.enqueue(object : Callback {
      override fun onResponse(call: Call, response: Response) {
        // A superseding connect() may have already cancelled this call and started a new one
        // before this callback for the OLD call's response arrives — without this check, a late
        // response like this would fire onOpen()/onMessage() for a connection that's no longer
        // this stream's active one.
        if (streams[streamId]?.currentCall !== call) {
          response.close()
          return
        }

        if (!response.isSuccessful) {
          val statusCode = response.code
          val bodyString = readBoundedBody(response)
          response.close()
          Log.e(LOG_TAG, "[$streamId] HTTP error: $statusCode")
          emitEvent(
            "onError",
            Arguments.createMap().apply {
              putString("streamId", streamId)
              putString("message", bodyString)
              putString("type", "http")
              putDouble("statusCode", statusCode.toDouble())
            }
          )
          emitEvent("onClose", Arguments.createMap().apply { putString("streamId", streamId) })
          scheduleReconnectIfNeeded(streamId, httpStatus = statusCode)
          return
        }

        val shouldValidateContentType = streams[streamId]?.validateContentType ?: true
        if (shouldValidateContentType) {
          val contentType = response.header("Content-Type")?.lowercase() ?: ""
          if (!contentType.startsWith("text/event-stream")) {
            val bodyString = readBoundedBody(response)
            response.close()
            val message = bodyString.ifEmpty { "Response Content-Type was not text/event-stream" }
            Log.e(LOG_TAG, "[$streamId] invalid content-type")
            emitEvent(
              "onError",
              Arguments.createMap().apply {
                putString("streamId", streamId)
                putString("message", message)
                putString("type", "invalid-content-type")
              }
            )
            emitEvent("onClose", Arguments.createMap().apply { putString("streamId", streamId) })
            scheduleReconnectIfNeeded(streamId, wasContentTypeError = true)
            return
          }
        }

        streams[streamId]?.reconnectAttempts = 0
        setState(streamId, ConnectionState.OPEN)
        emitEvent("onOpen", Arguments.createMap().apply { putString("streamId", streamId) })
        try {
          val source = response.body?.source()
          if (source == null) {
            response.close()
            return
          }
          val eventBuffer = StringBuilder()
          var loggedFirstByte = false
          while (!source.exhausted()) {
            if (streams[streamId]?.currentCall !== call) break
            if (!loggedFirstByte) {
              maybeLogFirstByte(streamId)
              loggedFirstByte = true
            }
            val line = source.readUtf8Line() ?: break
            if (line.isEmpty()) {
              if (eventBuffer.isNotEmpty()) {
                parseAndEmit(streamId, eventBuffer.toString())
                eventBuffer.setLength(0)
              }
            } else {
              eventBuffer.append(line).append('\n')
            }
          }
        } catch (e: IOException) {
          // Cancelled by disconnect()/a superseding connect() — expected, already handled
          // elsewhere (disconnect() fires onClose synchronously; a superseding call's own
          // lifecycle governs). Anything else reaching here is a genuine mid-stream failure.
          if (!call.isCanceled() && streams[streamId]?.currentCall === call) {
            Log.e(LOG_TAG, "[$streamId] connection dropped: ${e.message}", e)
            val type = if (e is SocketTimeoutException) "timeout" else "network"
            emitEvent(
              "onError",
              Arguments.createMap().apply {
                putString("streamId", streamId)
                putString("message", e.message ?: e.toString())
                putString("type", type)
              }
            )
          }
        } finally {
          emitCloseMetrics(streamId, call)
          response.close()
        }

        if (streams[streamId]?.currentCall === call) {
          emitEvent("onClose", Arguments.createMap().apply { putString("streamId", streamId) })
          scheduleReconnectIfNeeded(streamId)
        }
      }

      override fun onFailure(call: Call, e: IOException) {
        emitCloseMetrics(streamId, call)
        if (call.isCanceled()) return
        // A genuine (non-cancellation) failure on a call a newer connect() has already
        // superseded shouldn't surface as "the current connection failed" — it isn't, anymore.
        if (streams[streamId]?.currentCall !== call) return
        Log.e(LOG_TAG, "[$streamId] connect failed: ${e.message}", e)
        val type = if (e is SocketTimeoutException) "timeout" else "network"
        emitEvent(
          "onError",
          Arguments.createMap().apply {
            putString("streamId", streamId)
            putString("message", e.message ?: e.toString())
            putString("type", type)
          }
        )
        emitEvent("onClose", Arguments.createMap().apply { putString("streamId", streamId) })
        scheduleReconnectIfNeeded(streamId)
      }
    })
  }

  private fun applyMethodAndBody(builder: Request.Builder, method: String?, body: String?) {
    val resolvedMethod = method?.uppercase() ?: "GET"
    if (resolvedMethod !in METHODS_REQUIRING_BODY && body == null) {
      builder.method(resolvedMethod, null)
      return
    }
    builder.method(resolvedMethod, (body ?: "").toRequestBody())
  }

  private fun readBoundedBody(response: Response): String {
    val source = response.body?.source() ?: return ""
    return try {
      val buffer = Buffer()
      while (buffer.size < MAX_ERROR_BODY_BYTES && !source.exhausted()) {
        val read = source.read(buffer, MAX_ERROR_BODY_BYTES - buffer.size)
        if (read == -1L) break
      }
      buffer.readUtf8()
    } catch (e: IOException) {
      ""
    }
  }

  // By default, a 4xx response or a Content-Type mismatch doesn't warrant a reconnect — retrying
  // an identical request usually just repeats the same failure. 429 is the one exception (always
  // retried), and retryOnClientError overrides this entirely. 5xx/network/timeout errors (no
  // httpStatus, not a content-type error) are always retryable here.
  private fun isRetryableByDefault(streamId: String, httpStatus: Int?, wasContentTypeError: Boolean): Boolean {
    val state = streams[streamId] ?: return true
    if (state.retryOnClientError) return true
    if (wasContentTypeError) return false
    if (httpStatus == null) return true
    if (httpStatus == 429) return true
    return httpStatus !in 400..499
  }

  // Presence of `streams[streamId]` is what distinguishes "still an active stream, just between
  // connections" from "disconnect() was called" — disconnect() removes the entry entirely, so a
  // stale reconnect Runnable finds nothing here and no-ops.
  private fun scheduleReconnectIfNeeded(streamId: String, httpStatus: Int? = null, wasContentTypeError: Boolean = false) {
    val state = streams[streamId] ?: return
    if (!state.reconnectEnabled) {
      setState(streamId, ConnectionState.CLOSED)
      return
    }
    if (!isRetryableByDefault(streamId, httpStatus, wasContentTypeError)) {
      setState(streamId, ConnectionState.FAILED)
      return
    }
    state.reconnectMaxAttempts?.let { max ->
      if (state.reconnectAttempts >= max) {
        setState(streamId, ConnectionState.FAILED)
        return
      }
    }

    val delayMs = nextReconnectDelayMs(state, state.reconnectAttempts)
    state.reconnectAttempts += 1
    setState(streamId, ConnectionState.RECONNECTING)

    val runnable = Runnable {
      if (streams[streamId] != null) performConnect(streamId, isReconnect = true)
    }
    state.pendingReconnect = runnable
    mainHandler.postDelayed(runnable, delayMs.toLong())
  }

  // Exponential backoff with jitter: delay doubles with each consecutive failed attempt, starting
  // from reconnectIntervalMs (the base interval, or the server's last `retry:` value) and capped
  // at maxIntervalMs, then randomized by jitterFactor to avoid many clients retrying in lockstep
  // after a shared outage. `attempt` is 0 for the first scheduled reconnect (so it starts at
  // exactly reconnectIntervalMs before jitter), 1 for the second (2x), 2 for the third (4x), etc.
  private fun nextReconnectDelayMs(state: StreamState, attempt: Int): Double {
    val exponential = minOf(state.reconnectIntervalMs * Math.pow(2.0, attempt.toDouble()), state.maxIntervalMs)
    if (state.jitterFactor <= 0) return exponential
    val spread = exponential * state.jitterFactor
    val jittered = exponential - spread / 2 + Math.random() * spread
    return jittered.coerceIn(0.0, state.maxIntervalMs)
  }

  @ReactMethod
  fun disconnect(streamId: String) {
    val state = streams[streamId] ?: return
    state.pendingReconnect?.let { mainHandler.removeCallbacks(it) }
    state.pendingBeforeRequestTimeout?.let { mainHandler.removeCallbacks(it) }
    state.currentCall?.let { emitCloseMetrics(streamId, it) }
    val hadActiveCall = state.currentCall != null
    state.currentCall?.cancel()
    setState(streamId, ConnectionState.CLOSED)
    streams.remove(streamId)
    if (hadActiveCall) {
      emitEvent("onClose", Arguments.createMap().apply { putString("streamId", streamId) })
    }
  }

  @ReactMethod
  fun setEventFilter(streamId: String, types: ReadableArray) {
    val state = streams[streamId] ?: return
    state.eventFilter = (0 until types.size()).mapNotNull { types.getString(it) }.toSet()
  }

  @ReactMethod
  fun setMetricsEnabled(streamId: String, enabled: Boolean) {
    streams[streamId]?.metricsEnabled = enabled
  }

  // Required no-ops: NativeEventEmitter on the JS side calls these to (un)register interest;
  // our events fire regardless, but RN warns if a module used with NativeEventEmitter is
  // missing them.
  @ReactMethod
  fun addListener(eventName: String) {}

  @ReactMethod
  fun removeListeners(count: Int) {}

  // Native-only diagnostic — not sent to JS. onMetrics is limited to connectionReused (see
  // SSEConnectionMetrics on the JS side), which isn't knowable until the connection closes.
  private fun maybeLogFirstByte(streamId: String) {
    val state = streams[streamId] ?: return
    if (state.firstByteLogged) return
    state.firstByteLogged = true
    val startedAt = state.connectStartedAt ?: return
    val ttfbMs = (System.nanoTime() - startedAt) / 1_000_000.0
    Log.d(LOG_TAG, "[$streamId] time to first data: ${"%.1f".format(ttfbMs)}ms")
  }

  private fun parseAndEmit(streamId: String, rawEvent: String) {
    var id: String? = null
    var eventName: String? = null
    val dataLines = mutableListOf<String>()

    for (line in rawEvent.split("\n")) {
      when {
        line.startsWith(":") -> Unit
        line.startsWith("id:") -> id = line.removePrefix("id:").trim()
        line.startsWith("event:") -> eventName = line.removePrefix("event:").trim()
        line.startsWith("data:") -> dataLines.add(line.removePrefix("data:").trim())
        line.startsWith("retry:") -> {
          line.removePrefix("retry:").trim().toDoubleOrNull()?.let { streams[streamId]?.reconnectIntervalMs = it }
        }
      }
    }

    // An `id:` field (even empty) updates lastEventId for the *next* reconnect's Last-Event-ID
    // header — empty resets it to unset, matching the SSE spec. Absent leaves it unchanged.
    if (id != null) {
      streams[streamId]?.lastEventId = id.ifEmpty { null }
    }

    if (dataLines.isEmpty()) return

    val resolvedType = eventName ?: "message"
    val state = streams[streamId] ?: return
    if (state.eventFilter.isNotEmpty() && !state.eventFilter.contains(resolvedType)) return

    emitEvent(
      "onMessage",
      Arguments.createMap().apply {
        putString("streamId", streamId)
        putString("data", dataLines.joinToString("\n"))
        if (id != null) putString("id", id)
        if (eventName != null) putString("event", eventName)
      }
    )
  }

  private fun emitCloseMetrics(streamId: String, call: Call) {
    val attempt = call.request().tag(ConnectionAttempt::class.java) ?: return
    val timings = timingsByGeneration.remove(attempt.generation) ?: return

    fun durationMs(start: Long?, end: Long?): Double? {
      if (start == null || end == null) return null
      return (end - start) / 1_000_000.0
    }

    val connectMs = durationMs(timings.connectStart, timings.connectEnd)
    val tlsMs = durationMs(timings.secureConnectStart, timings.secureConnectEnd)
    val ttfbMs = durationMs(timings.callStart, timings.responseHeadersStart)

    // Full breakdown stays native-only (log line) — only connectionReused crosses the bridge,
    // and only if this stream has an onMetrics() listener registered.
    Log.d(
      LOG_TAG,
      "[$streamId] connection closed reused=${timings.connectionReused} connect=${connectMs}ms tls=${tlsMs}ms ttfb=${ttfbMs}ms"
    )

    if (streams[streamId]?.metricsEnabled != true) return
    emitEvent(
      "onMetrics",
      Arguments.createMap().apply {
        putString("streamId", streamId)
        putBoolean("connectionReused", timings.connectionReused)
      }
    )
  }

  private fun emitEvent(name: String, body: Any?) {
    reactApplicationContext
      .getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter::class.java)
      .emit(name, body)
  }
}
