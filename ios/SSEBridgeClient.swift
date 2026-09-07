//
//  SSEBridgeClient.swift
//  Classic (pre-New-Architecture) bridge native module: same URLSession-based
//  connection-reuse approach as the Nitro version, exposed via RCTEventEmitter
//  instead of JSI, for RN apps that can't use Nitro Modules (New Arch unavailable
//  or explicitly disabled).
//
//  Supports multiple concurrent streams. Classic Native Modules are singletons — JS can't get
//  a fresh native instance the way it can with Nitro's createHybridObject() — so instead every
//  method takes a `streamId` (generated in JS) and every emitted event carries it back, letting
//  the JS-side wrapper multiplex several independent logical streams over one native module.
//  All streams still share the same URLSession, so the connection pool is shared across them too.
//

import Foundation
import React

private let defaultReconnectIntervalMs: Double = 3000
// Error responses (4xx/5xx) are typically small JSON/HTML bodies — bounded so a misbehaving
// server streaming an enormous error page can't grow this unboundedly before completion.
private let maxErrorBodyBytes = 8192

private struct StreamState {
  var task: URLSessionDataTask?
  // Raw bytes rather than a Swift String: String's range(of:)/+= are Unicode-grapheme-aware
  // and re-scan/re-copy the whole buffer on every call, which is far too slow to do on every
  // single network chunk for a chatty stream — this buffer is only ever decoded to a String
  // once a complete frame has been isolated by byte.
  var byteBuffer = Data()
  var connectStartedAt: Date?
  var firstByteLogged = false
  // Empty set = no filtering, forward every frame. Non-empty = only frames whose resolved
  // event type (explicit `event:` field, or "message" when absent) is in this set get emitted —
  // everything else (e.g. `event: ping` heartbeats) is dropped before it ever reaches the
  // bridge, so a chatty/unwanted event type never costs a JS-thread call.
  var eventFilter: Set<String> = []
  // false = no onMetrics() listener on the JS side for this stream, so the connectionReused
  // event is dropped before it crosses the bridge — same idea as eventFilter, just a single flag
  // since there's only one metrics event (fired once, on close) rather than a set of types.
  var metricsEnabled = false

  // Reconnect state. connectURL/headers are remembered so an automatic reconnect can repeat the
  // same connect() call; reconnectEnabled/reconnectMaxAttempts come from the caller's
  // SSEReconnectOptions, resolved once per explicit connect(). reconnectIntervalMs starts at
  // options.intervalMs (default 3s) and is overridden per-stream by a `retry:` field from the
  // server; it resets back to the option's value on the next *explicit* connect().
  var connectURL: URL?
  var headers: [String: String]?
  var reconnectEnabled = true
  var reconnectIntervalMs = defaultReconnectIntervalMs
  var reconnectMaxAttempts: Double?
  var reconnectAttempts = 0
  var pendingReconnect: DispatchWorkItem?
  var lastEventId: String?

  // Set when the response status isn't 2xx; handleData then buffers the body here (instead of
  // feeding it through the normal SSE byteBuffer/parser) so it can be reported as the onError
  // message once the response completes.
  var errorStatusCode: Int?
  var errorBodyData = Data()
}

@objc(SSEBridgeClient)
class SSEBridgeClient: RCTEventEmitter {
  // Lazily created — see sharedSession(options:) below.
  private var session: URLSession?
  private let streamDelegate = SSEStreamDelegate()
  private static let frameDelimiter = Data([0x0A, 0x0A]) // "\n\n"

  private var streams: [String: StreamState] = [:]
  private var taskIdToStreamId: [Int: String] = [:]
  private var hasListeners = false

  override init() {
    super.init()
    streamDelegate.client = self
  }

  // The URLSession is shared by every stream and, once created, kept alive for the app's
  // lifetime — that's what lets a reconnect reuse the pooled HTTP/2 connection instead of
  // re-handshaking. Because of that, `options.session` can only take effect on the very first
  // connect() call across ALL streams; once the session exists, later streams' `session` options
  // are silently ignored (recreating it would defeat the whole point: every existing connection
  // in the pool would be dropped).
  private func sharedSession(options: NSDictionary) -> URLSession {
    if let session { return session }

    let sessionOptions = options["session"] as? [String: Any]
    let config = URLSessionConfiguration.default
    config.timeoutIntervalForRequest = (sessionOptions?["timeoutSeconds"] as? NSNumber)?.doubleValue ?? 3600
    config.httpMaximumConnectionsPerHost = (sessionOptions?["maxConnectionsPerHost"] as? NSNumber)?.intValue ?? 6

    let newSession = URLSession(configuration: config, delegate: streamDelegate, delegateQueue: nil)
    session = newSession
    return newSession
  }

  override static func requiresMainQueueSetup() -> Bool {
    return false
  }

  override func supportedEvents() -> [String]! {
    return ["onOpen", "onMessage", "onError", "onClose", "onMetrics"]
  }

  override func startObserving() {
    hasListeners = true
  }

  override func stopObserving() {
    hasListeners = false
  }

  @objc(connect:url:options:)
  func connect(_ streamId: String, url: String, options: NSDictionary) {
    // Validated before touching any existing connection, so a bad URL on a reconnect attempt
    // doesn't tear down a connection that was working fine — and reported through onError like
    // any other connection failure, rather than left completely silent.
    guard let nsUrl = URL(string: url) else {
      if hasListeners {
        sendEvent(withName: "onError", body: [
          "streamId": streamId, "message": "Invalid URL: \(url)", "type": "exception",
        ])
      }
      return
    }

    streams[streamId]?.pendingReconnect?.cancel()
    endTask(for: streamId)

    var state = StreamState()
    state.connectStartedAt = Date()
    state.connectURL = nsUrl
    state.headers = options["headers"] as? [String: String]
    if let eventTypes = options["eventTypes"] as? [String], !eventTypes.isEmpty {
      state.eventFilter = Set(eventTypes)
    }
    state.metricsEnabled = (options["metricsEnabled"] as? NSNumber)?.boolValue ?? false

    let reconnectOptions = options["reconnect"] as? [String: Any]
    state.reconnectEnabled = (reconnectOptions?["enabled"] as? NSNumber)?.boolValue ?? true
    state.reconnectIntervalMs = (reconnectOptions?["intervalMs"] as? NSNumber)?.doubleValue ?? defaultReconnectIntervalMs
    state.reconnectMaxAttempts = (reconnectOptions?["maxAttempts"] as? NSNumber)?.doubleValue

    streams[streamId] = state
    // `options` (session config) is only ever consulted on the very first connect() made across
    // ALL streams — see sharedSession(options:) — so it's fine that reconnects (which call
    // performConnect directly, not through here) don't have it to hand.
    _ = sharedSession(options: options)

    performConnect(streamId: streamId, isReconnect: false)
  }

  private func performConnect(streamId: String, isReconnect: Bool) {
    guard let connectURL = streams[streamId]?.connectURL else { return }

    streams[streamId]?.byteBuffer.removeAll(keepingCapacity: false)
    streams[streamId]?.errorStatusCode = nil
    streams[streamId]?.errorBodyData.removeAll(keepingCapacity: false)
    streams[streamId]?.firstByteLogged = false
    streams[streamId]?.connectStartedAt = Date()

    var request = URLRequest(url: connectURL)
    request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
    if let headers = streams[streamId]?.headers {
      for (key, value) in headers {
        request.setValue(value, forHTTPHeaderField: key)
      }
    }
    // Only sent on an automatic reconnect that has actually seen an id: field — an explicit
    // connect() always starts a fresh logical session (lastEventId is unset on a fresh StreamState).
    if isReconnect, let lastEventId = streams[streamId]?.lastEventId {
      request.setValue(lastEventId, forHTTPHeaderField: "Last-Event-ID")
    }

    let session = sharedSession(options: [:])
    // URLSessionConfiguration.timeoutIntervalForRequest is unreliable once a request carries its
    // own timeoutInterval (which URLRequest always does, defaulting to 60s) — the request-level
    // value wins. Reading it back off the session's own configuration, rather than hardcoding a
    // separate constant here, keeps this in sync with whatever options.session set.
    request.timeoutInterval = session.configuration.timeoutIntervalForRequest

    let task = session.dataTask(with: request)
    taskIdToStreamId[task.taskIdentifier] = streamId
    streams[streamId]?.task = task
    task.resume()
  }

  @objc(disconnect:)
  func disconnect(_ streamId: String) {
    streams[streamId]?.pendingReconnect?.cancel()
    let hadActiveTask = streams[streamId]?.task != nil
    endTask(for: streamId)
    streams[streamId] = nil
    if hadActiveTask, hasListeners {
      sendEvent(withName: "onClose", body: ["streamId": streamId])
    }
  }

  @objc(setEventFilter:types:)
  func setEventFilter(_ streamId: String, types: [String]) {
    streams[streamId]?.eventFilter = Set(types)
  }

  @objc(setMetricsEnabled:enabled:)
  func setMetricsEnabled(_ streamId: String, enabled: Bool) {
    streams[streamId]?.metricsEnabled = enabled
  }

  private func endTask(for streamId: String) {
    guard let task = streams[streamId]?.task else { return }
    task.cancel()
  }

  fileprivate func handleResponse(taskId: Int, response: URLResponse) {
    guard let streamId = taskIdToStreamId[taskId] else { return }
    // A superseding connect() may have already replaced this stream's task and started a new
    // one before this callback for the OLD task's response arrives — without this check, a late
    // response like this would fire onOpen() for a connection that's no longer the active one.
    guard streams[streamId]?.task?.taskIdentifier == taskId else { return }

    guard let httpResponse = response as? HTTPURLResponse, !(200..<300).contains(httpResponse.statusCode) else {
      streams[streamId]?.reconnectAttempts = 0
      guard hasListeners else { return }
      sendEvent(withName: "onOpen", body: ["streamId": streamId])
      return
    }
    // Non-2xx: don't fire onOpen at all — handleData buffers the error body instead of treating
    // it as SSE frames, and handleCompletion reports it once the response finishes.
    streams[streamId]?.errorStatusCode = httpResponse.statusCode
  }

  fileprivate func handleData(taskId: Int, data: Data) {
    guard let streamId = taskIdToStreamId[taskId] else { return }
    // Same race as handleResponse(): reject bytes from a task that's no longer this stream's
    // current one, so a superseded connection's late-arriving data can't get appended into the
    // new connection's (already-reset) byteBuffer.
    guard streams[streamId]?.task?.taskIdentifier == taskId else { return }

    if streams[streamId]?.errorStatusCode != nil {
      let currentSize = streams[streamId]?.errorBodyData.count ?? 0
      let remaining = maxErrorBodyBytes - currentSize
      if remaining > 0 {
        streams[streamId]?.errorBodyData.append(data.prefix(remaining))
      }
      return
    }

    if streams[streamId]?.firstByteLogged == false, let startedAt = streams[streamId]?.connectStartedAt {
      streams[streamId]?.firstByteLogged = true
      // Native-only diagnostic — not sent to JS. onMetrics is limited to connectionReused (see
      // SSEConnectionMetrics), which isn't knowable until the connection closes.
      let ttfbMs = Date().timeIntervalSince(startedAt) * 1000
      NSLog("[Bridge SSE][iOS][%@] time to first data: %.1fms", streamId, ttfbMs)
    }

    // Drop bare CR bytes so "\r\n" collapses to "\n" (SSE line endings), without the cost of
    // decoding to a String just to normalize — same effect, byte-level, once per chunk.
    streams[streamId]?.byteBuffer.append(data.filter { $0 != 0x0D })
    drainBuffer(streamId: streamId)
  }

  fileprivate func handleCompletion(taskId: Int, error: Error?) {
    // taskIdToStreamId is intentionally not cleaned up here: didFinishCollecting(metrics:) can
    // fire before or after this callback (Apple doesn't guarantee an order), and it needs this
    // mapping too. The leaked Int->String entries are negligible even over many reconnects.
    guard let streamId = taskIdToStreamId[taskId] else { return }
    // A genuine (non-cancellation) failure on a task a newer connect() has already superseded
    // shouldn't surface as "the current connection failed" — it isn't, anymore. disconnect()
    // fires onClose synchronously itself, so a cancellation reaching here (task no longer
    // current) is always either that or a superseded task — nothing to report either way.
    guard streams[streamId]?.task?.taskIdentifier == taskId else { return }

    if let statusCode = streams[streamId]?.errorStatusCode {
      let bodyData = streams[streamId]?.errorBodyData ?? Data()
      let bodyString = String(data: bodyData, encoding: .utf8) ?? ""
      streams[streamId]?.errorStatusCode = nil
      streams[streamId]?.errorBodyData = Data()
      NSLog("[Bridge SSE][iOS][%@] HTTP error: %d", streamId, statusCode)
      if hasListeners {
        sendEvent(withName: "onError", body: [
          "streamId": streamId, "message": bodyString, "type": "http", "statusCode": statusCode,
        ])
        sendEvent(withName: "onClose", body: ["streamId": streamId])
      }
      scheduleReconnectIfNeeded(streamId: streamId)
      return
    }

    if let error = error as NSError? {
      if error.code == NSURLErrorCancelled { return }
      NSLog("[Bridge SSE][iOS][%@] error: %@", streamId, error.localizedDescription)
      let type = error.code == NSURLErrorTimedOut ? "timeout" : "network"
      if hasListeners {
        sendEvent(withName: "onError", body: [
          "streamId": streamId, "message": error.localizedDescription, "type": type,
        ])
        sendEvent(withName: "onClose", body: ["streamId": streamId])
      }
      scheduleReconnectIfNeeded(streamId: streamId)
      return
    }

    // No error, not an HTTP error, task is done: the server closed the stream normally.
    if hasListeners {
      sendEvent(withName: "onClose", body: ["streamId": streamId])
    }
    scheduleReconnectIfNeeded(streamId: streamId)
  }

  // Presence of `streams[streamId]` is what distinguishes "still an active stream, just between
  // connections" from "disconnect() was called" — disconnect() removes the entry entirely, so a
  // stale reconnect timer's closure finds nothing here and no-ops.
  private func scheduleReconnectIfNeeded(streamId: String) {
    guard var state = streams[streamId], state.reconnectEnabled else { return }
    if let maxAttempts = state.reconnectMaxAttempts, Double(state.reconnectAttempts) >= maxAttempts { return }
    state.reconnectAttempts += 1
    streams[streamId] = state

    let work = DispatchWorkItem { [weak self] in
      guard self?.streams[streamId] != nil else { return }
      self?.performConnect(streamId: streamId, isReconnect: true)
    }
    streams[streamId]?.pendingReconnect = work
    DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + state.reconnectIntervalMs / 1000, execute: work)
  }

  fileprivate func handleMetrics(taskId: Int, metrics: URLSessionTaskMetrics) {
    guard let streamId = taskIdToStreamId[taskId] else { return }
    guard let txn = metrics.transactionMetrics.last else { return }

    // Pooled/reused connections skip the connect+TLS phases entirely, so these dates come
    // back nil — that absence (not a separate flag) is the actual reuse signal.
    let reused = txn.connectStartDate == nil

    func durationMs(_ start: Date?, _ end: Date?) -> Double? {
      guard let start, let end else { return nil }
      return end.timeIntervalSince(start) * 1000
    }

    let connectMs = durationMs(txn.connectStartDate, txn.connectEndDate)
    let tlsMs = durationMs(txn.secureConnectionStartDate, txn.secureConnectionEndDate)
    let ttfbMs = durationMs(txn.fetchStartDate, txn.responseStartDate)

    // Full breakdown stays native-only (log line) — only connectionReused crosses the bridge,
    // and only if this stream has an onMetrics() listener registered.
    NSLog(
      "[Bridge SSE][iOS][%@] connection closed reused=%@ connect=%.1fms tls=%.1fms ttfb=%.1fms",
      streamId, reused ? "true" : "false", connectMs ?? 0, tlsMs ?? 0, ttfbMs ?? 0
    )

    guard hasListeners, streams[streamId]?.metricsEnabled == true else { return }
    sendEvent(withName: "onMetrics", body: ["streamId": streamId, "connectionReused": reused])
  }

  private func drainBuffer(streamId: String) {
    guard var state = streams[streamId] else { return }
    while let range = state.byteBuffer.range(of: Self.frameDelimiter) {
      let frameData = state.byteBuffer.subdata(in: state.byteBuffer.startIndex..<range.lowerBound)
      state.byteBuffer.removeSubrange(state.byteBuffer.startIndex..<range.upperBound)
      streams[streamId] = state
      if let rawEvent = String(data: frameData, encoding: .utf8) {
        parseAndEmit(streamId: streamId, rawEvent: rawEvent)
      }
      state = streams[streamId] ?? state
    }
    streams[streamId] = state
  }

  private func parseAndEmit(streamId: String, rawEvent: String) {
    var id: String?
    var eventName: String?
    var dataLines: [String] = []

    for line in rawEvent.split(separator: "\n", omittingEmptySubsequences: false) {
      if line.hasPrefix(":") { continue }
      if line.hasPrefix("id:") {
        id = String(line.dropFirst(3)).trimmingCharacters(in: .whitespaces)
      } else if line.hasPrefix("event:") {
        eventName = String(line.dropFirst(6)).trimmingCharacters(in: .whitespaces)
      } else if line.hasPrefix("data:") {
        dataLines.append(String(line.dropFirst(5)).trimmingCharacters(in: .whitespaces))
      } else if line.hasPrefix("retry:") {
        if let ms = Double(String(line.dropFirst(6)).trimmingCharacters(in: .whitespaces)) {
          streams[streamId]?.reconnectIntervalMs = ms
        }
      }
    }

    // An `id:` field (even empty) updates lastEventId for the *next* reconnect's Last-Event-ID
    // header — empty resets it to unset, matching the SSE spec. Absent leaves it unchanged.
    if let id {
      streams[streamId]?.lastEventId = id.isEmpty ? nil : id
    }

    guard !dataLines.isEmpty else { return }
    guard hasListeners else { return }

    let resolvedType = eventName ?? "message"
    guard let state = streams[streamId] else { return }
    if !state.eventFilter.isEmpty, !state.eventFilter.contains(resolvedType) {
      return
    }

    var body: [String: Any] = ["streamId": streamId, "data": dataLines.joined(separator: "\n")]
    if let id { body["id"] = id }
    if let eventName { body["event"] = eventName }
    sendEvent(withName: "onMessage", body: body)
  }
}

/// Forwards URLSession delegate callbacks to SSEBridgeClient, routed by task identifier since
/// one shared session now serves multiple concurrent streams.
private final class SSEStreamDelegate: NSObject, URLSessionDataDelegate {
  weak var client: SSEBridgeClient?

  func urlSession(
    _ session: URLSession,
    dataTask: URLSessionDataTask,
    didReceive response: URLResponse,
    completionHandler: @escaping (URLSession.ResponseDisposition) -> Void
  ) {
    client?.handleResponse(taskId: dataTask.taskIdentifier, response: response)
    completionHandler(.allow)
  }

  func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
    client?.handleData(taskId: dataTask.taskIdentifier, data: data)
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    client?.handleCompletion(taskId: task.taskIdentifier, error: error)
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didFinishCollecting metrics: URLSessionTaskMetrics) {
    client?.handleMetrics(taskId: task.taskIdentifier, metrics: metrics)
  }
}
