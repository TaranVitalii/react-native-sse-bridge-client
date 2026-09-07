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
import okhttp3.Response
import okio.Buffer
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Proxy
import java.net.SocketTimeoutException
import java.util.concurrent.TimeUnit

private const val LOG_TAG = "BridgeSSE"
private const val DEFAULT_RECONNECT_INTERVAL_MS = 3000.0

// Error responses (4xx/5xx) are typically small JSON/HTML bodies — bounded so a misbehaving
// server streaming an enormous error page can't grow this unboundedly before completion.
private const val MAX_ERROR_BODY_BYTES = 8192L

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
    var reconnectMaxAttempts: Double? = null
    var reconnectAttempts = 0
    var pendingReconnect: Runnable? = null
    var lastEventId: String? = null
  }

  private val streams = mutableMapOf<String, StreamState>()
  private val timingsByGeneration = mutableMapOf<Int, CallTimings>()
  private var generationCounter = 0
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
    state.reconnectMaxAttempts = if (reconnectOptions?.hasKey("maxAttempts") == true) reconnectOptions.getDouble("maxAttempts") else null

    streams[streamId] = state
    // `options` (session config) is only ever consulted on the very first connect() made across
    // ALL streams — see sharedClient(options:) — so it's fine that reconnects (which call
    // performConnect directly, not through here) don't have it to hand.
    sharedClient(options)

    performConnect(streamId, isReconnect = false)
  }

  private fun performConnect(streamId: String, isReconnect: Boolean) {
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
          scheduleReconnectIfNeeded(streamId)
          return
        }

        streams[streamId]?.reconnectAttempts = 0
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

  // Presence of `streams[streamId]` is what distinguishes "still an active stream, just between
  // connections" from "disconnect() was called" — disconnect() removes the entry entirely, so a
  // stale reconnect Runnable finds nothing here and no-ops.
  private fun scheduleReconnectIfNeeded(streamId: String) {
    val state = streams[streamId] ?: return
    if (!state.reconnectEnabled) return
    state.reconnectMaxAttempts?.let { max -> if (state.reconnectAttempts >= max) return }
    state.reconnectAttempts += 1

    val runnable = Runnable {
      if (streams[streamId] != null) performConnect(streamId, isReconnect = true)
    }
    state.pendingReconnect = runnable
    mainHandler.postDelayed(runnable, state.reconnectIntervalMs.toLong())
  }

  @ReactMethod
  fun disconnect(streamId: String) {
    val state = streams[streamId] ?: return
    state.pendingReconnect?.let { mainHandler.removeCallbacks(it) }
    state.currentCall?.let { emitCloseMetrics(streamId, it) }
    val hadActiveCall = state.currentCall != null
    state.currentCall?.cancel()
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
