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
}

@objc(SSEBridgeClient)
class SSEBridgeClient: RCTEventEmitter {
  private let session: URLSession
  private let streamDelegate = SSEStreamDelegate()
  private static let frameDelimiter = Data([0x0A, 0x0A]) // "\n\n"

  private var streams: [String: StreamState] = [:]
  private var taskIdToStreamId: [Int: String] = [:]
  private var hasListeners = false

  override init() {
    let config = URLSessionConfiguration.default
    config.timeoutIntervalForRequest = 3600
    config.httpMaximumConnectionsPerHost = 6
    self.session = URLSession(configuration: config, delegate: streamDelegate, delegateQueue: nil)
    super.init()
    streamDelegate.client = self
  }

  override static func requiresMainQueueSetup() -> Bool {
    return false
  }

  override func supportedEvents() -> [String]! {
    return ["onOpen", "onMessage", "onError", "onMetrics"]
  }

  override func startObserving() {
    hasListeners = true
  }

  override func stopObserving() {
    hasListeners = false
  }

  @objc(connect:url:options:)
  func connect(_ streamId: String, url: String, options: NSDictionary) {
    guard let nsUrl = URL(string: url) else { return }

    endTask(for: streamId)

    var state = StreamState()
    state.connectStartedAt = Date()
    if let eventTypes = options["eventTypes"] as? [String], !eventTypes.isEmpty {
      state.eventFilter = Set(eventTypes)
    }
    streams[streamId] = state

    var request = URLRequest(url: nsUrl)
    request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
    if let headers = options["headers"] as? [String: String] {
      for (key, value) in headers {
        request.setValue(value, forHTTPHeaderField: key)
      }
    }
    request.timeoutInterval = 3600

    let task = session.dataTask(with: request)
    taskIdToStreamId[task.taskIdentifier] = streamId
    streams[streamId]?.task = task
    task.resume()
  }

  @objc(disconnect:)
  func disconnect(_ streamId: String) {
    endTask(for: streamId)
    streams[streamId] = nil
  }

  @objc(setEventFilter:types:)
  func setEventFilter(_ streamId: String, types: [String]) {
    streams[streamId]?.eventFilter = Set(types)
  }

  private func endTask(for streamId: String) {
    guard let task = streams[streamId]?.task else { return }
    task.cancel()
  }

  fileprivate func handleResponse(taskId: Int) {
    guard let streamId = taskIdToStreamId[taskId] else { return }
    guard hasListeners else { return }
    sendEvent(withName: "onOpen", body: ["streamId": streamId])
  }

  fileprivate func handleData(taskId: Int, data: Data) {
    guard let streamId = taskIdToStreamId[taskId] else { return }
    guard var state = streams[streamId] else { return }

    if !state.firstByteLogged, let startedAt = state.connectStartedAt {
      state.firstByteLogged = true
      let ttfbMs = Date().timeIntervalSince(startedAt) * 1000
      NSLog("[Bridge SSE][iOS][%@] time to first data: %.1fms", streamId, ttfbMs)
      if hasListeners {
        sendEvent(withName: "onMetrics", body: [
          "streamId": streamId,
          "phase": "ttfb",
          "ttfbMs": ttfbMs,
        ])
      }
    }

    // Drop bare CR bytes so "\r\n" collapses to "\n" (SSE line endings), without the cost of
    // decoding to a String just to normalize — same effect, byte-level, once per chunk.
    state.byteBuffer.append(data.filter { $0 != 0x0D })
    streams[streamId] = state
    drainBuffer(streamId: streamId)
  }

  fileprivate func handleCompletion(taskId: Int, error: Error?) {
    // taskIdToStreamId is intentionally not cleaned up here: didFinishCollecting(metrics:) can
    // fire before or after this callback (Apple doesn't guarantee an order), and it needs this
    // mapping too. The leaked Int->String entries are negligible even over many reconnects.
    guard let streamId = taskIdToStreamId[taskId] else { return }

    guard let error = error as NSError? else { return }
    if error.code == NSURLErrorCancelled { return }
    NSLog("[Bridge SSE][iOS][%@] error: %@", streamId, error.localizedDescription)
    if hasListeners {
      sendEvent(withName: "onError", body: ["streamId": streamId, "message": error.localizedDescription])
    }
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

    NSLog(
      "[Bridge SSE][iOS][%@] connection closed reused=%@ connect=%.1fms tls=%.1fms ttfb=%.1fms",
      streamId, reused ? "true" : "false", connectMs ?? 0, tlsMs ?? 0, ttfbMs ?? 0
    )

    if hasListeners {
      var body: [String: Any] = [
        "streamId": streamId,
        "phase": "closed",
        "connectionReused": reused,
      ]
      if let connectMs { body["connectMs"] = connectMs }
      if let tlsMs { body["tlsMs"] = tlsMs }
      if let ttfbMs { body["ttfbMs"] = ttfbMs }
      sendEvent(withName: "onMetrics", body: body)
    }
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
      }
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
    client?.handleResponse(taskId: dataTask.taskIdentifier)
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
