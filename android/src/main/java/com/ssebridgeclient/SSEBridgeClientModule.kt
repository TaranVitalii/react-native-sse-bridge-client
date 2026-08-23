package com.ssebridgeclient

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
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Proxy
import java.util.concurrent.TimeUnit

private const val LOG_TAG = "BridgeSSE"

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
  }

  private val streams = mutableMapOf<String, StreamState>()
  private val timingsByGeneration = mutableMapOf<Int, CallTimings>()
  private var generationCounter = 0

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
    streams[streamId]?.currentCall?.let { emitCloseMetrics(streamId, it) }
    streams[streamId]?.currentCall?.cancel()

    val state = StreamState()
    state.connectStartedAt = System.nanoTime()
    val eventTypes = options?.getArray("eventTypes")
    if (eventTypes != null && eventTypes.size() > 0) {
      state.eventFilter = (0 until eventTypes.size()).mapNotNull { eventTypes.getString(it) }.toSet()
    }
    state.metricsEnabled = options?.hasKey("metricsEnabled") == true && options.getBoolean("metricsEnabled")
    streams[streamId] = state

    generationCounter += 1
    val generation = generationCounter

    val requestBuilder = Request.Builder()
      .url(url)
      .header("Accept", "text/event-stream")
      .tag(ConnectionAttempt::class.java, ConnectionAttempt(streamId, generation))

    val headers = options?.getMap("headers")
    headers?.let { h ->
      val iterator = h.keySetIterator()
      while (iterator.hasNextKey()) {
        val key = iterator.nextKey()
        val value = h.getString(key)
        if (value != null) requestBuilder.header(key, value)
      }
    }

    val call = sharedClient(options).newCall(requestBuilder.build())
    state.currentCall = call

    call.enqueue(object : Callback {
      override fun onResponse(call: Call, response: Response) {
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
          // Stream ended, or was cancelled by disconnect()/a superseding connect() — expected.
        } finally {
          emitCloseMetrics(streamId, call)
          response.close()
        }
      }

      override fun onFailure(call: Call, e: IOException) {
        emitCloseMetrics(streamId, call)
        if (call.isCanceled()) return
        Log.e(LOG_TAG, "[$streamId] connect failed: ${e.message}", e)
        emitEvent(
          "onError",
          Arguments.createMap().apply {
            putString("streamId", streamId)
            putString("message", e.message ?: e.toString())
          }
        )
      }
    })
  }

  @ReactMethod
  fun disconnect(streamId: String) {
    val state = streams[streamId] ?: return
    state.currentCall?.let { emitCloseMetrics(streamId, it) }
    state.currentCall?.cancel()
    streams.remove(streamId)
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
      }
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
