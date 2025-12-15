/**
 * useDBSP - React Hook for Incremental SQL Transformations
 * 
 * ERGONOMIC API - Just push data and query with SQL:
 * 
 * ```tsx
 * const { results, push, stats } = useDBSP<Order>(
 *   "SELECT * FROM data WHERE status = 'pending'",
 *   { key: 'id' }
 * );
 * 
 * // Load initial data
 * push(orders);
 * 
 * // Stream updates (incremental!)
 * push([newOrder]);
 * ```
 */

import { useState, useCallback, useRef, useMemo } from 'react';
import { ZSet } from '../zset';
import { SQLCompiler } from '../sql/sql-compiler';
import { Circuit, StreamHandle } from '../circuit';

// ============ SIMPLE API ============

export interface DBSPOptions<TIn> {
  /** Primary key field name(s) or function to compute key */
  key: keyof TIn | (keyof TIn)[] | ((row: TIn) => string);
  /** Key function for output rows (for aggregations) */
  outputKey?: string | string[] | ((row: Record<string, unknown>) => string);
  /** Enable debug logging */
  debug?: boolean;
}

export interface DBSPResult<TIn extends Record<string, unknown>, TOut extends Record<string, unknown> = TIn> {
  /** Current query results (filtered/transformed data) */
  results: TOut[];
  
  /** Number of results */
  count: number;
  
  /** Total rows in source table */
  totalRows: number;
  
  /** Push data (initial load or updates) - automatically upserts by key */
  push: (rows: TIn | TIn[]) => void;
  
  /** Remove rows by key value(s) */
  remove: (...keyValues: unknown[]) => void;
  
  /** Remove rows matching predicate */
  removeWhere: (predicate: (row: TIn) => boolean) => void;
  
  /** Clear all data */
  clear: () => void;
  
  /** Check if ready (schema inferred) */
  ready: boolean;
  
  /** Processing stats */
  stats: {
    lastUpdateMs: number;
    totalUpdates: number;
    totalRows: number;
    avgUpdateMs: number;
  };
}

/**
 * Simple, ergonomic DBSP hook
 * 
 * @param query - SQL SELECT query on the "data" table
 * @param options - { key: 'id' } or { key: ['field1', 'field2'] }
 * 
 * @example
 * ```tsx
 * // Filter
 * const { results, push } = useDBSP<Order>(
 *   "SELECT * FROM data WHERE status = 'pending'",
 *   { key: 'orderId' }
 * );
 * 
 * // Aggregate (with different output type)
 * const { results } = useDBSP<Sale, RegionSummary>(
 *   "SELECT region, SUM(amount) as total FROM data GROUP BY region",
 *   { key: 'id', outputKey: 'region' }
 * );
 * ```
 */
export function useDBSP<
  TIn extends Record<string, unknown>,
  TOut extends Record<string, unknown> = TIn
>(
  query: string,
  options: DBSPOptions<TIn>
): DBSPResult<TIn, TOut> {
  const { key, outputKey, debug = false } = options;
  
  // Get key function for input rows
  const getKey = useCallback((row: TIn): string => {
    if (typeof key === 'function') {
      return key(row);
    }
    const keys = Array.isArray(key) ? key : [key];
    return keys.map(k => String(row[k])).join('::');
  }, [key]);
  
  // Get key function for output rows (different for aggregations)
  const getOutputKey = useCallback((row: TOut): string => {
    if (outputKey) {
      if (typeof outputKey === 'function') {
        return outputKey(row);
      }
      const keys = Array.isArray(outputKey) ? outputKey : [outputKey];
      return keys.map(k => String(row[k])).join('::');
    }
    // Fall back to input key logic
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
  
  // Stats - MEMORY FIX: Use circular buffer for updateTimes
  const statsRef = useRef({
    lastUpdateMs: 0,
    totalUpdates: 0,
    totalRows: 0,
    avgUpdateMs: 0,
    updateTimes: new Float64Array(100), // Fixed-size circular buffer
    updateTimesIndex: 0,
    updateTimesCount: 0,
  });
  
  // Circuit
  const circuitRef = useRef<{
    circuit: Circuit;
    views: Record<string, StreamHandle<unknown>>;
    integratedData: Map<string, { row: TOut; weight: number; index: number }>;
    getResults: () => TOut[];
    getCount: () => number;
    clearCache: () => void;
  } | null>(null);
  
  // Schema inferred from first data
  const schemaRef = useRef<string | null>(null);
  
  // Infer schema from data
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
  
  // Initialize circuit when schema is known
  const initCircuit = useCallback((schema: string) => {
    try {
      // Build SQL: CREATE TABLE data (...); CREATE VIEW result AS {query};
      const sql = `
        CREATE TABLE data (${schema});
        CREATE VIEW result AS ${query};
      `;
      
      if (debug) {
        console.log('[useDBSP] Compiling SQL:', sql);
      }
      
      const compiler = new SQLCompiler();
      const { circuit, views } = compiler.compile(sql);
      
      // Use Map for O(1) lookups
      const integratedData = new Map<string, { row: TOut; weight: number; index: number }>();
      
      // TRULY INCREMENTAL ARRAY: Maintain array incrementally
      let resultsArray: TOut[] = [];
      let freeIndices: number[] = [];
      // MEMORY FIX: Cache filtered results, only rebuild when dirty
      let cachedResults: TOut[] | null = null;
      
      // Listen for output - O(delta) not O(total)!
      views['result'].output((delta) => {
        const zset = delta as ZSet<TOut>;
        let hasChanges = false;
        
        if (debug) {
          console.log('[useDBSP] Output callback fired, zset size:', zset.size(), 'entries:', zset.entries().length);
          // Log first entry to debug
          const firstEntry = zset.entries()[0];
          if (firstEntry) {
            console.log('[useDBSP] First entry row:', JSON.stringify(firstEntry[0]).slice(0, 200), 'weight:', firstEntry[1]);
          }
        }
        
        // Apply delta to integrated state - O(delta)
        for (const [row, weight] of zset.entries()) {
          const rowKey = getOutputKey(row);
          if (debug && integratedData.size < 3) {
            console.log('[useDBSP] Processing row, key:', rowKey, 'weight:', weight, 'integratedData.size:', integratedData.size);
          }
          const existing = integratedData.get(rowKey);
          const oldWeight = existing?.weight || 0;
          const newWeight = oldWeight + weight;
          
          const wasPresent = oldWeight > 0;
          const isPresent = newWeight > 0;
          
          if (!wasPresent && isPresent) {
            // INSERT: Add new row
            let idx: number;
            if (freeIndices.length > 0) {
              idx = freeIndices.pop()!;
              resultsArray[idx] = row;
            } else {
              idx = resultsArray.length;
              resultsArray.push(row);
            }
            integratedData.set(rowKey, { row, weight: newWeight, index: idx });
            hasChanges = true;
          } else if (wasPresent && !isPresent) {
            // DELETE: Remove row
            const idx = existing!.index;
            // @ts-expect-error - using undefined as tombstone
            resultsArray[idx] = undefined;
            freeIndices.push(idx);
            integratedData.delete(rowKey);
            hasChanges = true;
          } else if (wasPresent && isPresent) {
            // UPDATE: Replace row in place
            const idx = existing!.index;
            resultsArray[idx] = row;
            integratedData.set(rowKey, { row, weight: newWeight, index: idx });
            hasChanges = true;
          }
        }
        
        if (hasChanges) {
          // MEMORY FIX: Invalidate cache on changes
          cachedResults = null;
          setDataVersion(v => v + 1);
        }
      });
      
      // Store reference with O(delta) accessors
      circuitRef.current = { 
        circuit, 
        views, 
        integratedData,
        // MEMORY FIX: Only rebuild results array when cache is invalidated
        getResults: () => {
          if (cachedResults === null) {
            // Rebuild only when dirty - use compact instead of filter for better perf
            cachedResults = [];
            for (let i = 0; i < resultsArray.length; i++) {
              if (resultsArray[i] !== undefined) {
                cachedResults.push(resultsArray[i]);
              }
            }
          }
          return cachedResults;
        },
        getCount: () => integratedData.size,
        clearCache: () => {
          resultsArray = [];
          freeIndices = [];
          cachedResults = null;
        }
      };
      setReady(true);
      
      if (debug) {
        console.log('[useDBSP] Circuit initialized');
      }
    } catch (err) {
      console.error('[useDBSP] Failed to compile SQL:', err);
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
    
    // Update stats - MEMORY FIX: Use circular buffer (no array allocation)
    statsRef.current.lastUpdateMs = elapsed;
    statsRef.current.totalUpdates++;
    statsRef.current.totalRows = dataMapRef.current.size;
    
    // Circular buffer for update times
    statsRef.current.updateTimes[statsRef.current.updateTimesIndex] = elapsed;
    statsRef.current.updateTimesIndex = (statsRef.current.updateTimesIndex + 1) % 100;
    statsRef.current.updateTimesCount = Math.min(statsRef.current.updateTimesCount + 1, 100);
    
    // Calculate average without creating new arrays
    let sum = 0;
    const count = statsRef.current.updateTimesCount;
    for (let i = 0; i < count; i++) {
      sum += statsRef.current.updateTimes[i];
    }
    statsRef.current.avgUpdateMs = count > 0 ? sum / count : 0;
    
    if (debug) {
      console.log(`[useDBSP] Processed in ${elapsed.toFixed(2)}ms`);
    }
    
    setDataVersion(v => v + 1);
  }, [debug]);
  
  // Push data (main API)
  const push = useCallback((rows: TIn | TIn[]) => {
    const rowArray = Array.isArray(rows) ? rows : [rows];
    if (rowArray.length === 0) return;
    
    // Infer schema on first push
    if (!schemaRef.current) {
      schemaRef.current = inferSchema(rowArray[0]);
      initCircuit(schemaRef.current);
    }
    
    // Wait for circuit to be ready
    if (!circuitRef.current) {
      setTimeout(() => push(rows), 0);
      return;
    }
    
    const entries: [TIn, number][] = [];
    
    for (const row of rowArray) {
      const rowKey = getKey(row);
      const existing = dataMapRef.current.get(rowKey);
      
      // Remove old version if exists
      if (existing) {
        entries.push([existing, -1]);
      }
      
      // Add new version
      entries.push([row, 1]);
      dataMapRef.current.set(rowKey, row);
    }
    
    const delta = ZSet.fromEntries(entries, getKey);
    processDelta(delta);
  }, [getKey, inferSchema, initCircuit, processDelta]);
  
  // Remove by key
  const remove = useCallback((...keyValues: unknown[]) => {
    const keyStr = keyValues.join('::');
    const existing = dataMapRef.current.get(keyStr);
    
    if (!existing || !circuitRef.current) return;
    
    dataMapRef.current.delete(keyStr);
    const delta = ZSet.fromEntries([[existing, -1]], getKey);
    processDelta(delta);
  }, [getKey, processDelta]);
  
  // Remove where
  const removeWhere = useCallback((predicate: (row: TIn) => boolean) => {
    if (!circuitRef.current) return;
    
    const entries: [TIn, number][] = [];
    
    for (const [rowKey, row] of dataMapRef.current) {
      if (predicate(row)) {
        dataMapRef.current.delete(rowKey);
        entries.push([row, -1]);
      }
    }
    
    if (entries.length > 0) {
      const delta = ZSet.fromEntries(entries, getKey);
      processDelta(delta);
    }
  }, [getKey, processDelta]);
  
  // Clear all
  const clear = useCallback(() => {
    if (!circuitRef.current) return;
    
    const entries: [TIn, number][] = [];
    
    for (const [, row] of dataMapRef.current) {
      entries.push([row, -1]);
    }
    
    dataMapRef.current.clear();
    
    if (entries.length > 0) {
      const delta = ZSet.fromEntries(entries, getKey);
      processDelta(delta);
    }
    
    circuitRef.current?.clearCache();
    
    // Reset stats with circular buffer
    statsRef.current.lastUpdateMs = 0;
    statsRef.current.totalUpdates = 0;
    statsRef.current.totalRows = 0;
    statsRef.current.avgUpdateMs = 0;
    statsRef.current.updateTimesIndex = 0;
    statsRef.current.updateTimesCount = 0;
    statsRef.current.updateTimes.fill(0);
    setDataVersion(v => v + 1);
  }, [getKey, processDelta]);
  
  // Computed total rows
  const totalRows = useMemo(() => {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const _ = dataVersion;
    return dataMapRef.current.size;
  }, [dataVersion]);
  
  // LAZY RESULTS: Only build array when actually accessed
  const lazyResults = useMemo(() => {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const _ = dataVersion;
    return circuitRef.current?.getResults() || [];
  }, [dataVersion]);
  
  // LAZY COUNT: Get count without building array
  const lazyCount = useMemo(() => {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const _ = dataVersion;
    return circuitRef.current?.getCount() || 0;
  }, [dataVersion]);
  
  return {
    results: lazyResults,
    count: lazyCount,
    totalRows,
    push,
    remove,
    removeWhere,
    clear,
    ready,
    stats: {
      lastUpdateMs: statsRef.current.lastUpdateMs,
      totalUpdates: statsRef.current.totalUpdates,
      totalRows: statsRef.current.totalRows,
      avgUpdateMs: statsRef.current.avgUpdateMs,
    },
  };
}
