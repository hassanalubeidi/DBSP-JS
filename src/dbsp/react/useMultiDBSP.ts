/**
 * useMultiDBSP - Single source, multiple views
 * 
 * MEMORY EFFICIENT: Stores source data ONCE, shares circuit state
 * 
 * Based on DBSP paper Section 8: "shared arrangements"
 * 
 * Instead of:
 *   const view1 = useDBSP("SELECT * FROM data WHERE ...", { key: 'id' });
 *   const view2 = useDBSP("SELECT * FROM data WHERE ...", { key: 'id' });
 *   // Each stores 100K rows = 200K total!
 * 
 * Use:
 *   const { views, push } = useMultiDBSP({ key: 'id', views: { view1: "...", view2: "..." }});
 *   // Stores 100K rows ONCE
 */

import { useState, useCallback, useRef, useMemo } from 'react';
import { ZSet } from '../zset';
import { SQLCompiler } from '../sql/sql-compiler';
import { Circuit, StreamHandle } from '../circuit';

export interface MultiDBSPOptions<TIn> {
  /** Primary key field(s) for source data */
  key: keyof TIn | (keyof TIn)[] | ((row: TIn) => string);
  /** Named SQL views */
  views: Record<string, {
    sql: string;
    outputKey?: string | string[] | ((row: Record<string, unknown>) => string);
  }>;
  /** Debug mode */
  debug?: boolean;
}

export interface ViewResult<TOut> {
  results: TOut[];
  count: number;
}

export interface MultiDBSPResult<TIn extends Record<string, unknown>> {
  /** Access view results by name */
  views: Record<string, ViewResult<Record<string, unknown>>>;
  
  /** Total source rows */
  totalRows: number;
  
  /** Push data (initial load or updates) */
  push: (rows: TIn | TIn[]) => void;
  
  /** Remove by key */
  remove: (...keyValues: unknown[]) => void;
  
  /** Clear all data */
  clear: () => void;
  
  /** Ready state */
  ready: boolean;
  
  /** Stats */
  stats: {
    lastUpdateMs: number;
    totalUpdates: number;
    avgUpdateMs: number;
  };
}

export function useMultiDBSP<TIn extends Record<string, unknown>>(
  options: MultiDBSPOptions<TIn>
): MultiDBSPResult<TIn> {
  const { key, views: viewConfigs, debug = false } = options;
  
  // Get key function for input rows
  const getKey = useCallback((row: TIn): string => {
    if (typeof key === 'function') {
      return key(row);
    }
    const keys = Array.isArray(key) ? key : [key];
    return keys.map(k => String(row[k])).join('::');
  }, [key]);
  
  // State - SINGLE source data map (not per-view!)
  const dataMapRef = useRef<Map<string, TIn>>(new Map());
  const [ready, setReady] = useState(false);
  const [dataVersion, setDataVersion] = useState(0);
  
  // Stats - using circular buffer
  const statsRef = useRef({
    lastUpdateMs: 0,
    totalUpdates: 0,
    avgUpdateMs: 0,
    updateTimes: new Float64Array(100),
    updateTimesIndex: 0,
    updateTimesCount: 0,
  });
  
  // SINGLE circuit for ALL views
  const circuitRef = useRef<{
    circuit: Circuit;
    viewHandles: Record<string, StreamHandle<unknown>>;
    viewStates: Record<string, {
      integratedData: Map<string, { row: Record<string, unknown>; weight: number; index: number }>;
      resultsArray: Record<string, unknown>[];
      freeIndices: number[];
      cachedResults: Record<string, unknown>[] | null;
      getOutputKey: (row: Record<string, unknown>) => string;
    }>;
  } | null>(null);
  
  // Schema
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
  
  // Initialize circuit with ALL views in ONE compilation
  const initCircuit = useCallback((schema: string) => {
    try {
      // Build SQL with SINGLE table and ALL views
      const viewNames = Object.keys(viewConfigs);
      const viewSQLs = viewNames.map(name => 
        `CREATE VIEW ${name} AS ${viewConfigs[name].sql};`
      ).join('\n');
      
      const sql = `
        CREATE TABLE data (${schema});
        ${viewSQLs}
      `;
      
      if (debug) {
        console.log('[useMultiDBSP] Compiling combined SQL:', sql);
      }
      
      const compiler = new SQLCompiler();
      const { circuit, views } = compiler.compile(sql);
      
      // Initialize state for each view - use single object per view for closure sharing
      const viewStates: Record<string, {
        integratedData: Map<string, { row: Record<string, unknown>; weight: number; index: number }>;
        resultsArray: Record<string, unknown>[];
        freeIndices: number[];
        cachedResults: Record<string, unknown>[] | null;
        getOutputKey: (row: Record<string, unknown>) => string;
      }> = {};
      
      for (const viewName of viewNames) {
        const config = viewConfigs[viewName];
        
        // Output key function for this view
        const getOutputKey = (row: Record<string, unknown>): string => {
          if (config.outputKey) {
            if (typeof config.outputKey === 'function') {
              return config.outputKey(row);
            }
            const keys = Array.isArray(config.outputKey) ? config.outputKey : [config.outputKey];
            return keys.map(k => String(row[k])).join('::');
          }
          // Fall back to input key
          if (typeof key === 'function') {
            return key(row as unknown as TIn);
          }
          const keys = Array.isArray(key) ? key : [key];
          return keys.map(k => String(row[k as keyof typeof row])).join('::');
        };
        
        // Create view state object FIRST - closure will reference this same object
        const viewState = {
          integratedData: new Map<string, { row: Record<string, unknown>; weight: number; index: number }>(),
          resultsArray: [] as Record<string, unknown>[],
          freeIndices: [] as number[],
          cachedResults: null as Record<string, unknown>[] | null,
          getOutputKey,
        };
        
        viewStates[viewName] = viewState;
        
        // Subscribe to view output - closure references viewState directly
        views[viewName].output((delta) => {
          const zset = delta as ZSet<Record<string, unknown>>;
          let hasChanges = false;
          
          const entries = zset.entries();
          if (debug && entries.length > 0) {
            console.log(`[useMultiDBSP] View "${viewName}" received delta:`, {
              deltaSize: zset.size(),
              entriesCount: entries.length,
              currentIntegratedSize: viewState.integratedData.size,
              allEntries: entries.slice(0, 10).map(([r, w]) => ({ row: r, weight: w })),
            });
          }
          
          for (const [row, weight] of entries) {
            const rowKey = viewState.getOutputKey(row);
            const existing = viewState.integratedData.get(rowKey);
            const oldWeight = existing?.weight || 0;
            const newWeight = oldWeight + weight;
            
            const wasPresent = oldWeight > 0;
            const isPresent = newWeight > 0;
            
            if (!wasPresent && isPresent) {
              // INSERT: Add new row
              let idx: number;
              if (viewState.freeIndices.length > 0) {
                idx = viewState.freeIndices.pop()!;
                viewState.resultsArray[idx] = row;
              } else {
                idx = viewState.resultsArray.length;
                viewState.resultsArray.push(row);
              }
              viewState.integratedData.set(rowKey, { row, weight: newWeight, index: idx });
              hasChanges = true;
            } else if (wasPresent && !isPresent) {
              // DELETE: Remove row
              const idx = existing!.index;
              // @ts-expect-error - using undefined as tombstone
              viewState.resultsArray[idx] = undefined;
              viewState.freeIndices.push(idx);
              viewState.integratedData.delete(rowKey);
              hasChanges = true;
            } else if (wasPresent && isPresent) {
              // UPDATE: Replace row in place
              const idx = existing!.index;
              viewState.resultsArray[idx] = row;
              viewState.integratedData.set(rowKey, { row, weight: newWeight, index: idx });
              hasChanges = true;
            }
          }
          
          if (hasChanges) {
            viewState.cachedResults = null; // Invalidate cache
            if (debug) {
              console.log(`[useMultiDBSP] View "${viewName}" after processing:`, {
                integratedSize: viewState.integratedData.size,
                resultsArrayLength: viewState.resultsArray.length,
                sampleResults: Array.from(viewState.integratedData.values()).slice(0, 3).map(v => v.row),
              });
            }
          }
        });
      }
      
      circuitRef.current = {
        circuit,
        viewHandles: views,
        viewStates,
      };
      
      setReady(true);
      
      if (debug) {
        console.log('[useMultiDBSP] Circuit initialized with', viewNames.length, 'views');
      }
    } catch (err) {
      console.error('[useMultiDBSP] Failed to compile SQL:', err);
    }
  }, [viewConfigs, key, debug]);
  
  // Process delta through circuit
  const processDelta = useCallback((delta: ZSet<TIn>) => {
    if (!circuitRef.current) return;
    
    const start = performance.now();
    
    // SINGLE circuit step for ALL views
    circuitRef.current.circuit.step(new Map([
      ['data', delta as unknown as ZSet<unknown>]
    ]));
    
    const elapsed = performance.now() - start;
    
    // Update stats
    statsRef.current.lastUpdateMs = elapsed;
    statsRef.current.totalUpdates++;
    
    // Circular buffer
    statsRef.current.updateTimes[statsRef.current.updateTimesIndex] = elapsed;
    statsRef.current.updateTimesIndex = (statsRef.current.updateTimesIndex + 1) % 100;
    statsRef.current.updateTimesCount = Math.min(statsRef.current.updateTimesCount + 1, 100);
    
    let sum = 0;
    const count = statsRef.current.updateTimesCount;
    for (let i = 0; i < count; i++) {
      sum += statsRef.current.updateTimes[i];
    }
    statsRef.current.avgUpdateMs = count > 0 ? sum / count : 0;
    
    if (debug) {
      console.log(`[useMultiDBSP] Processed in ${elapsed.toFixed(2)}ms`);
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
    
    // Wait for circuit
    if (!circuitRef.current) {
      setTimeout(() => push(rows), 0);
      return;
    }
    
    const entries: [TIn, number][] = [];
    
    for (const row of rowArray) {
      const rowKey = getKey(row);
      const existing = dataMapRef.current.get(rowKey);
      
      if (existing) {
        entries.push([existing, -1]);
      }
      
      entries.push([row, 1]);
      dataMapRef.current.set(rowKey, row);
    }
    
    // IMPORTANT: Use JSON.stringify as the key for ZSet delta (not the primary key).
    // This ensures that old_row with weight -1 and new_row with weight +1
    // are treated as different entries when their content differs.
    const delta = ZSet.fromEntries(entries);
    processDelta(delta);
  }, [getKey, inferSchema, initCircuit, processDelta]);
  
  // Remove by key
  const remove = useCallback((...keyValues: unknown[]) => {
    const keyStr = keyValues.join('::');
    const existing = dataMapRef.current.get(keyStr);
    
    if (!existing || !circuitRef.current) return;
    
    dataMapRef.current.delete(keyStr);
    const delta = ZSet.fromEntries([[existing, -1]]);
    processDelta(delta);
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
      const delta = ZSet.fromEntries(entries);
      processDelta(delta);
    }
    
    // Clear view caches
    for (const viewState of Object.values(circuitRef.current.viewStates)) {
      viewState.integratedData.clear();
      viewState.resultsArray = [];
      viewState.freeIndices = [];
      viewState.cachedResults = null;
    }
    
    // Reset stats
    statsRef.current.lastUpdateMs = 0;
    statsRef.current.totalUpdates = 0;
    statsRef.current.avgUpdateMs = 0;
    statsRef.current.updateTimesIndex = 0;
    statsRef.current.updateTimesCount = 0;
    statsRef.current.updateTimes.fill(0);
    
    setDataVersion(v => v + 1);
  }, [getKey, processDelta]);
  
  // Total rows
  const totalRows = useMemo(() => {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const _ = dataVersion;
    return dataMapRef.current.size;
  }, [dataVersion]);
  
  // View results - computed lazily
  const viewResults = useMemo(() => {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const _ = dataVersion;
    
    if (!circuitRef.current) {
      const empty: Record<string, ViewResult<Record<string, unknown>>> = {};
      for (const name of Object.keys(viewConfigs)) {
        empty[name] = { results: [], count: 0 };
      }
      return empty;
    }
    
    const results: Record<string, ViewResult<Record<string, unknown>>> = {};
    
    for (const [name, state] of Object.entries(circuitRef.current.viewStates)) {
      // Lazy cache rebuild
      if (state.cachedResults === null) {
        state.cachedResults = [];
        for (let i = 0; i < state.resultsArray.length; i++) {
          if (state.resultsArray[i] !== undefined) {
            state.cachedResults.push(state.resultsArray[i]);
          }
        }
        if (debug) {
          console.log(`[useMultiDBSP] Rebuilt cache for "${name}":`, {
            cachedResultsLength: state.cachedResults.length,
            integratedDataSize: state.integratedData.size,
            sample: state.cachedResults.slice(0, 3),
          });
        }
      }
      
      results[name] = {
        results: state.cachedResults,
        count: state.integratedData.size,
      };
    }
    
    return results;
  }, [dataVersion, viewConfigs, debug]);
  
  return {
    views: viewResults,
    totalRows,
    push,
    remove,
    clear,
    ready,
    stats: {
      lastUpdateMs: statsRef.current.lastUpdateMs,
      totalUpdates: statsRef.current.totalUpdates,
      avgUpdateMs: statsRef.current.avgUpdateMs,
    },
  };
}

