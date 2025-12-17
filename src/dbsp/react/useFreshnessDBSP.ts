/**
 * useFreshnessDBSP - React Hook for DBSP with Freshness Guarantees
 * 
 * Wraps useDBSP with circular buffer and freshness controls to ensure
 * real-time processing that NEVER lags behind incoming data.
 * 
 * Key features:
 * - Circular buffer with configurable capacity
 * - Automatic dropping of stale messages
 * - Lag tracking and reporting
 * - Batch processing for efficiency
 */

import { useState, useCallback, useRef, useMemo, useEffect } from 'react';
import { ZSet } from '../zset';
import { SQLCompiler } from '../sql/sql-compiler';
import { Circuit, StreamHandle } from '../circuit';
import { FreshnessQueue } from '../async-stream';

// ============ TYPES ============

export interface FreshnessDBSPOptions<TIn> {
  /** Primary key field name(s) or function to compute key */
  key: keyof TIn | (keyof TIn)[] | ((row: TIn) => string);
  /** Key function for output rows (for aggregations) */
  outputKey?: string | string[] | ((row: Record<string, unknown>) => string);
  /** Maximum buffer size for incoming data */
  maxBufferSize?: number;
  /** Maximum message age in ms before dropping */
  maxMessageAgeMs?: number;
  /** Maximum batch size for processing */
  maxBatchSize?: number;
  /** Processing interval in ms */
  processingIntervalMs?: number;
  /** Enable debug logging */
  debug?: boolean;
  /** Callback when data is dropped */
  onDrop?: (count: number, reason: 'overflow' | 'stale') => void;
}

export interface FreshnessDBSPResult<TIn extends Record<string, unknown>, TOut extends Record<string, unknown> = TIn> {
  /** Current query results */
  results: TOut[];
  
  /** Number of results */
  count: number;
  
  /** Total rows in source table */
  totalRows: number;
  
  /** Push data - may drop old data if buffer is full */
  push: (rows: TIn | TIn[]) => void;
  
  /** Remove rows by key value(s) */
  remove: (...keyValues: unknown[]) => void;
  
  /** Clear all data */
  clear: () => void;
  
  /** Check if ready */
  ready: boolean;
  
  /** Freshness stats */
  stats: {
    lastUpdateMs: number;
    totalUpdates: number;
    totalRows: number;
    avgUpdateMs: number;
    bufferSize: number;
    bufferCapacity: number;
    bufferUtilization: number;
    lagMs: number;
    droppedOverflow: number;
    droppedStale: number;
    totalDropped: number;
    isLagging: boolean;
  };
  
  /** Drop stale messages manually */
  dropStale: (maxAgeMs?: number) => number;
  
  /** Pause/resume processing */
  setPaused: (paused: boolean) => void;
  isPaused: boolean;
}

// ============ HOOK ============

export function useFreshnessDBSP<
  TIn extends Record<string, unknown>,
  TOut extends Record<string, unknown> = TIn
>(
  query: string,
  options: FreshnessDBSPOptions<TIn>
): FreshnessDBSPResult<TIn, TOut> {
  const {
    key,
    outputKey,
    maxBufferSize = 10000,
    maxMessageAgeMs,
    maxBatchSize = 500,
    processingIntervalMs = 16, // ~60fps
    debug = false,
    onDrop,
  } = options;
  
  // Get key function for input rows
  const getKey = useCallback((row: TIn): string => {
    if (typeof key === 'function') {
      return key(row);
    }
    const keys = Array.isArray(key) ? key : [key];
    return keys.map(k => String(row[k])).join('::');
  }, [key]);
  
  // Get key function for output rows
  const getOutputKey = useCallback((row: TOut): string => {
    if (outputKey) {
      if (typeof outputKey === 'function') {
        return outputKey(row);
      }
      const keys = Array.isArray(outputKey) ? outputKey : [outputKey];
      return keys.map(k => String(row[k])).join('::');
    }
    if (typeof key === 'function') {
      return key(row as unknown as TIn);
    }
    const keys = Array.isArray(key) ? key : [key];
    return keys.map(k => String(row[k as keyof TOut])).join('::');
  }, [key, outputKey]);
  
  // State
  const dataMapRef = useRef<Map<string, TIn>>(new Map());
  const [ready, setReady] = useState(false);
  const [dataVersion, setDataVersion] = useState(0);
  const [isPaused, setIsPaused] = useState(false);
  
  // Freshness tracking
  const droppedOverflowRef = useRef(0);
  const droppedStaleRef = useRef(0);
  
  // Freshness queue for incoming data
  const freshnessQueueRef = useRef<FreshnessQueue<{ type: 'push' | 'remove'; data: TIn | TIn[] | unknown[] }>>(
    new FreshnessQueue(
      maxBufferSize,
      maxMessageAgeMs,
      (count, reason) => {
        if (reason === 'overflow') {
          droppedOverflowRef.current += count;
        } else {
          droppedStaleRef.current += count;
        }
        if (onDrop) onDrop(count, reason);
        if (debug) {
          console.log(`[useFreshnessDBSP] Dropped ${count} messages: ${reason}`);
        }
      },
      debug
    )
  );
  
  // Stats
  const statsRef = useRef({
    lastUpdateMs: 0,
    totalUpdates: 0,
    totalRows: 0,
    avgUpdateMs: 0,
    updateTimes: new Float64Array(100),
    updateTimesIndex: 0,
    updateTimesCount: 0,
  });
  
  // Circuit
  const circuitRef = useRef<{
    circuit: Circuit;
    views: Record<string, StreamHandle<unknown>>;
    integratedData: Map<string, { row: TOut; weight: number; index: number }>;
    resultsArray: TOut[];
    freeIndices: number[];
    cachedResults: TOut[] | null;
    compactionCounter: number; // Track operations for periodic compaction
  } | null>(null);
  
  const schemaRef = useRef<string | null>(null);
  
  // Infer schema
  const inferSchema = useCallback((row: TIn): string => {
    const columns: string[] = [];
    for (const [colKey, value] of Object.entries(row)) {
      let type = 'VARCHAR';
      if (typeof value === 'number') {
        type = Number.isInteger(value) ? 'INT' : 'DECIMAL';
      } else if (typeof value === 'boolean') {
        type = 'BOOLEAN';
      }
      columns.push(`${colKey} ${type}`);
    }
    return columns.join(', ');
  }, []);
  
  // Initialize circuit
  const initCircuit = useCallback((schema: string) => {
    try {
      const sql = `
        CREATE TABLE data (${schema});
        CREATE VIEW result AS ${query};
      `;
      
      if (debug) {
        console.log('[useFreshnessDBSP] Compiling SQL:', sql);
      }
      
      const compiler = new SQLCompiler();
      const { circuit, views } = compiler.compile(sql);
      
      const integratedData = new Map<string, { row: TOut; weight: number; index: number }>();
      const resultsArray: TOut[] = [];
      const freeIndices: number[] = [];
      
      // Store circuit reference FIRST so output callback can access it
      circuitRef.current = {
        circuit,
        views,
        integratedData,
        resultsArray,
        freeIndices,
        cachedResults: null,
        compactionCounter: 0,
      };
      
      views['result'].output((delta) => {
        const zset = delta as ZSet<TOut>;
        let hasChanges = false;
        
        // Access arrays through circuitRef to ensure we're modifying the right objects
        const state = circuitRef.current!;
        
        for (const [row, weight] of zset.entries()) {
          const rowKey = getOutputKey(row);
          const existing = state.integratedData.get(rowKey);
          const oldWeight = existing?.weight || 0;
          const newWeight = oldWeight + weight;
          
          const wasPresent = oldWeight > 0;
          const isPresent = newWeight > 0;
          
          if (!wasPresent && isPresent) {
            let idx: number;
            if (state.freeIndices.length > 0) {
              idx = state.freeIndices.pop()!;
              state.resultsArray[idx] = row;
            } else {
              idx = state.resultsArray.length;
              state.resultsArray.push(row);
            }
            state.integratedData.set(rowKey, { row, weight: newWeight, index: idx });
            hasChanges = true;
          } else if (wasPresent && !isPresent) {
            const idx = existing!.index;
            // @ts-expect-error - tombstone
            state.resultsArray[idx] = undefined;
            state.freeIndices.push(idx);
            state.integratedData.delete(rowKey);
            hasChanges = true;
          } else if (wasPresent && isPresent) {
            const idx = existing!.index;
            state.resultsArray[idx] = row;
            state.integratedData.set(rowKey, { row, weight: newWeight, index: idx });
            hasChanges = true;
          }
        }
        
        if (hasChanges) {
          // Invalidate cache through circuitRef
          state.cachedResults = null;
          
          // MEMORY FIX: Periodic compaction when fragmentation is high
          // Compaction threshold: more than 50% of array is holes, and at least 1000 holes
          state.compactionCounter++;
          const arrayLength = state.resultsArray.length;
          const liveCount = state.integratedData.size;
          const holeCount = arrayLength - liveCount;
          
          if (state.compactionCounter >= 1000 && holeCount > 1000 && holeCount > liveCount) {
            // Compact: rebuild arrays without holes
            const newResultsArray: TOut[] = [];
            const newIntegratedData = new Map<string, { row: TOut; weight: number; index: number }>();
            
            for (const [key, entry] of state.integratedData.entries()) {
              const newIndex = newResultsArray.length;
              newResultsArray.push(entry.row);
              newIntegratedData.set(key, { row: entry.row, weight: entry.weight, index: newIndex });
            }
            
            // Replace arrays
            state.resultsArray.length = 0; // Clear existing
            for (const item of newResultsArray) {
              state.resultsArray.push(item);
            }
            state.freeIndices.length = 0; // Clear free indices
            state.integratedData.clear();
            for (const [k, v] of newIntegratedData.entries()) {
              state.integratedData.set(k, v);
            }
            
            state.compactionCounter = 0;
            if (debug) {
              console.log(`[useFreshnessDBSP] Compacted: ${arrayLength} -> ${newResultsArray.length} (removed ${holeCount} holes)`);
            }
          }
        }
      });
      
      setReady(true);
      
      if (debug) {
        console.log('[useFreshnessDBSP] Circuit initialized');
      }
    } catch (err) {
      console.error('[useFreshnessDBSP] Failed to compile SQL:', err);
    }
  }, [query, getOutputKey, debug]);
  
  // Process delta through circuit
  const processDelta = useCallback((delta: ZSet<TIn>) => {
    if (!circuitRef.current) return;
    
    const start = performance.now();
    
    circuitRef.current.circuit.step(new Map([
      ['data', delta as unknown as ZSet<unknown>]
    ]));
    
    const elapsed = performance.now() - start;
    
    statsRef.current.lastUpdateMs = elapsed;
    statsRef.current.totalUpdates++;
    statsRef.current.totalRows = dataMapRef.current.size;
    
    // Circular buffer stats
    statsRef.current.updateTimes[statsRef.current.updateTimesIndex] = elapsed;
    statsRef.current.updateTimesIndex = (statsRef.current.updateTimesIndex + 1) % 100;
    statsRef.current.updateTimesCount = Math.min(statsRef.current.updateTimesCount + 1, 100);
    
    let sum = 0;
    for (let i = 0; i < statsRef.current.updateTimesCount; i++) {
      sum += statsRef.current.updateTimes[i];
    }
    statsRef.current.avgUpdateMs = statsRef.current.updateTimesCount > 0 
      ? sum / statsRef.current.updateTimesCount 
      : 0;
  }, []);
  
  // Process queued messages
  const processQueue = useCallback(async () => {
    if (isPaused || !circuitRef.current) return;
    
    const queue = freshnessQueueRef.current;
    const messages = await queue.dequeue(maxBatchSize, 1);
    
    if (messages.length === 0) return;
    
    const entries: [TIn, number][] = [];
    const removeKeys: string[] = [];
    
    for (const msg of messages) {
      const { type, data } = msg.data;
      
      if (type === 'push') {
        const rows = Array.isArray(data) ? data as TIn[] : [data as TIn];
        for (const row of rows) {
          const rowKey = getKey(row);
          const existing = dataMapRef.current.get(rowKey);
          
          if (existing) {
            entries.push([existing, -1]);
          }
          entries.push([row, 1]);
          dataMapRef.current.set(rowKey, row);
        }
      } else if (type === 'remove') {
        const keys = data as unknown[];
        for (const keyVal of keys) {
          const keyStr = String(keyVal);
          removeKeys.push(keyStr);
        }
      }
    }
    
    // Process removes
    for (const keyStr of removeKeys) {
      const existing = dataMapRef.current.get(keyStr);
      if (existing) {
        entries.push([existing, -1]);
        dataMapRef.current.delete(keyStr);
      }
    }
    
    if (entries.length > 0) {
      // IMPORTANT: Use JSON.stringify as the key for ZSet delta (not the primary key).
      // This ensures that old_row with weight -1 and new_row with weight +1
      // are treated as different entries when their content differs.
      // The primary key (getKey) is used by dataMapRef to track current state,
      // but the ZSet delta must distinguish by full row content for correct DBSP semantics.
      const delta = ZSet.fromEntries(entries);
      processDelta(delta);
      setDataVersion(v => v + 1);
    }
  }, [isPaused, maxBatchSize, getKey, processDelta]);
  
  // Processing loop
  useEffect(() => {
    if (!ready) return;
    
    const interval = setInterval(processQueue, processingIntervalMs);
    return () => clearInterval(interval);
  }, [ready, processQueue, processingIntervalMs]);
  
  // Push data (queued with freshness)
  const push = useCallback((rows: TIn | TIn[]) => {
    const rowArray = Array.isArray(rows) ? rows : [rows];
    if (rowArray.length === 0) return;
    
    // Infer schema on first push
    if (!schemaRef.current) {
      schemaRef.current = inferSchema(rowArray[0]);
      initCircuit(schemaRef.current);
    }
    
    // Queue for processing (may drop if buffer full)
    freshnessQueueRef.current.enqueue({ type: 'push', data: rowArray });
  }, [inferSchema, initCircuit]);
  
  // Remove by key (queued)
  const remove = useCallback((...keyValues: unknown[]) => {
    freshnessQueueRef.current.enqueue({ type: 'remove', data: keyValues });
  }, []);
  
  // Clear all (immediate)
  const clear = useCallback(() => {
    if (!circuitRef.current) return;
    
    const entries: [TIn, number][] = [];
    for (const [, row] of dataMapRef.current) {
      entries.push([row, -1]);
    }
    
    dataMapRef.current.clear();
    freshnessQueueRef.current.clear();
    droppedOverflowRef.current = 0;
    droppedStaleRef.current = 0;
    
    if (entries.length > 0) {
      // Use JSON.stringify for ZSet delta (same reasoning as processQueue)
      const delta = ZSet.fromEntries(entries);
      processDelta(delta);
    }
    
    // Reset circuit state - both our tracking and circuit's internal operator state
    circuitRef.current.resultsArray.length = 0;
    circuitRef.current.freeIndices.length = 0;
    circuitRef.current.cachedResults = null;
    circuitRef.current.integratedData.clear();
    circuitRef.current.circuit.reset(); // Reset all stateful operators (GROUP BY, distinct, etc.)
    
    // Reset stats
    statsRef.current.lastUpdateMs = 0;
    statsRef.current.totalUpdates = 0;
    statsRef.current.totalRows = 0;
    statsRef.current.avgUpdateMs = 0;
    statsRef.current.updateTimesIndex = 0;
    statsRef.current.updateTimesCount = 0;
    statsRef.current.updateTimes.fill(0);
    
    setDataVersion(v => v + 1);
  }, [getKey, processDelta]);
  
  // Drop stale manually
  const dropStale = useCallback((maxAgeMs?: number): number => {
    return freshnessQueueRef.current.dropStale(maxAgeMs);
  }, []);
  
  // Computed values
  const totalRows = useMemo(() => {
    void dataVersion;
    return dataMapRef.current.size;
  }, [dataVersion]);
  
  const lazyResults = useMemo(() => {
    void dataVersion;
    if (!circuitRef.current) return [];
    
    if (circuitRef.current.cachedResults === null) {
      circuitRef.current.cachedResults = [];
      for (const item of circuitRef.current.resultsArray) {
        if (item !== undefined) {
          circuitRef.current.cachedResults.push(item);
        }
      }
    }
    return circuitRef.current.cachedResults;
  }, [dataVersion]);
  
  const lazyCount = useMemo(() => {
    void dataVersion;
    return circuitRef.current?.integratedData.size || 0;
  }, [dataVersion]);
  
  // Stats with freshness info - memoized to prevent unnecessary re-renders downstream
  const stats = useMemo(() => {
    void dataVersion; // Re-compute when data changes
    const queueStats = freshnessQueueRef.current.getStats();
    return {
      lastUpdateMs: statsRef.current.lastUpdateMs,
      totalUpdates: statsRef.current.totalUpdates,
      totalRows: statsRef.current.totalRows,
      avgUpdateMs: statsRef.current.avgUpdateMs,
      bufferSize: queueStats.size,
      bufferCapacity: queueStats.capacity,
      bufferUtilization: queueStats.utilization,
      lagMs: queueStats.lagMs,
      droppedOverflow: droppedOverflowRef.current,
      droppedStale: droppedStaleRef.current,
      totalDropped: droppedOverflowRef.current + droppedStaleRef.current,
      isLagging: queueStats.lagMs > 100,
    };
  }, [dataVersion]);
  
  return {
    results: lazyResults,
    count: lazyCount,
    totalRows,
    push,
    remove,
    clear,
    ready,
    stats,
    dropStale,
    setPaused: setIsPaused,
    isPaused,
  };
}

