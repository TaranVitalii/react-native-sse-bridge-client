/**
 * Minimal usage example for react-native-sse-bridge-client.
 *
 * @format
 */

import React, { useCallback, useEffect, useRef, useState } from 'react'
import {
  FlatList,
  StatusBar,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
  useColorScheme,
} from 'react-native'
import { SafeAreaProvider, SafeAreaView } from 'react-native-safe-area-context'
import { createSSEStream } from 'react-native-sse-bridge-client'
import type {
  SSEConnectionMetrics,
  SSEConnectionState,
  SSEError,
  SSEMessageEvent,
  SSEStream,
} from 'react-native-sse-bridge-client'

const DEFAULT_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'

interface LogEntry {
  id: number
  text: string
}

function formatMetrics(metrics: SSEConnectionMetrics): string {
  return `closed — connection reused: ${metrics.connectionReused}`
}

function formatError(error: SSEError): string {
  return error.statusCode != null
    ? `error (${error.type}, HTTP ${error.statusCode}): ${error.message}`
    : `error (${error.type}): ${error.message}`
}

function App(): React.JSX.Element {
  const isDarkMode = useColorScheme() === 'dark'
  const [url, setUrl] = useState(DEFAULT_URL)
  const [connected, setConnected] = useState(false)
  const [state, setState] = useState<SSEConnectionState>('idle')
  const [entries, setEntries] = useState<LogEntry[]>([])
  const streamRef = useRef<SSEStream | null>(null)

  const log = useCallback((text: string) => {
    setEntries(prev => [{ id: prev.length, text }, ...prev].slice(0, 200))
  }, [])

  useEffect(() => {
    const stream = createSSEStream()
    streamRef.current = stream

    const unsubscribers = [
      stream.onOpen(() => log('opened')),
      stream.addEventListener('message', (event: SSEMessageEvent) => {
        log(`message: ${event.data.slice(0, 100)}`)
      }),
      stream.onError((error: SSEError) => log(formatError(error))),
      // Fires on every disconnect — including the automatic reconnect this library does by
      // default, so "closed" here doesn't mean the stream gave up, just that this particular
      // connection ended.
      stream.onClose(() =>
        log('closed (will auto-reconnect unless disconnect() was called)'),
      ),
      stream.onMetrics((metrics: SSEConnectionMetrics) =>
        log(formatMetrics(metrics)),
      ),
      stream.onStateChange((next: SSEConnectionState) => {
        setState(next)
        log(`state: ${next}`)
      }),
    ]

    return () => {
      unsubscribers.forEach(unsubscribe => unsubscribe())
      stream.destroy()
    }
  }, [log])

  const handleConnect = () => {
    log(`connecting to ${url}`)
    streamRef.current?.connect(url)
    setConnected(true)
  }

  const handleDisconnect = () => {
    streamRef.current?.disconnect()
    setConnected(false)
    log('disconnected')
  }

  return (
    <SafeAreaProvider>
      <StatusBar barStyle={isDarkMode ? 'light-content' : 'dark-content'} />
      <SafeAreaView style={styles.container} edges={['top', 'bottom']}>
        <Text style={styles.title}>react-native-sse-bridge-client</Text>
        <Text style={styles.state}>state: {state}</Text>
        <TextInput
          style={styles.input}
          value={url}
          onChangeText={setUrl}
          autoCapitalize="none"
          autoCorrect={false}
          editable={!connected}
        />
        <View style={styles.row}>
          <TouchableOpacity
            style={[styles.button, connected && styles.buttonDisabled]}
            onPress={handleConnect}
            disabled={connected}>
            <Text style={styles.buttonText}>Connect</Text>
          </TouchableOpacity>
          <TouchableOpacity
            style={[styles.button, !connected && styles.buttonDisabled]}
            onPress={handleDisconnect}
            disabled={!connected}>
            <Text style={styles.buttonText}>Disconnect</Text>
          </TouchableOpacity>
        </View>
        <FlatList
          style={styles.log}
          data={entries}
          keyExtractor={item => String(item.id)}
          renderItem={({ item }) => <Text style={styles.logLine}>{item.text}</Text>}
        />
      </SafeAreaView>
    </SafeAreaProvider>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    padding: 12,
  },
  title: {
    fontSize: 16,
    fontWeight: '700',
    marginBottom: 4,
  },
  state: {
    fontSize: 12,
    color: '#8b949e',
    marginBottom: 8,
  },
  input: {
    borderWidth: 1,
    borderColor: '#c4c9d0',
    borderRadius: 6,
    paddingHorizontal: 10,
    paddingVertical: 8,
    fontSize: 13,
    marginBottom: 8,
  },
  row: {
    flexDirection: 'row',
    gap: 8,
    marginBottom: 8,
  },
  button: {
    flex: 1,
    backgroundColor: '#1f6feb',
    borderRadius: 6,
    paddingVertical: 10,
    alignItems: 'center',
  },
  buttonDisabled: {
    backgroundColor: '#c4c9d0',
  },
  buttonText: {
    color: 'white',
    fontWeight: '600',
  },
  log: {
    flex: 1,
    backgroundColor: '#0d1117',
    borderRadius: 6,
    padding: 8,
  },
  logLine: {
    fontFamily: 'Menlo',
    fontSize: 11,
    color: '#c9d1d9',
    marginBottom: 2,
  },
})

export default App
