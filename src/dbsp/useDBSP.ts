/**
 * useDBSP - React Hook for Incremental Data Processing
 * 
 * Provides a reactive interface to DBSP for real-time analytics in React apps.
 * 
 * Features:
 * - SQL-based transformations with automatic incrementalization
 * - Support for upserts with composite primary keys
 * - Insert, update, delete operations
 * - Automatic re-rendering on data changes
 * 
 * @example
 * ```tsx
 * const { data, insert, upsert, remove } = useDBSP({
 *   tableName: 'orders',
 *   initialData: orders,
 *   sql: 'SELECT * FROM orders WHERE status = "pending" AND amount > 100',
 *   primaryKey: ['orderId'],
 * });
 * ```
 */

import { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import { ZSet } from './zset';
import { SQLCompiler } from './sql/sql-compiler';
import { Circuit, StreamHandle } from './circuit';

// ============ TYPES ============

export interface UseDBSPOptions<T extends Record<string, unknown>> {
  /** Name of the table (used in SQL) */
  tableName: string;
  
  /** Initial data to populate the table */
  initialData?: T[];
  
  /** SQL query for the view (e.g., "SELECT * FROM orders WHERE status = 'pending'") */
  sql?: string;
  
  /** Primary key field(s) for upsert operations */
  primaryKey: (keyof T)[];
  
  /** Optional: Custom key function (defaults to using primaryKey) */
  keyFn?: (row: T) => string;
}

export interface UseDBSPResult<T extends Record<string, unknown>> {
  /** Current transformed data (result of SQL query) */
  data: T[];
  
  /** Raw data (before transformation) */
  rawData: T[];
  
  /** Number of rows in raw data */
  count: number;
  
  /** Insert a new row (fails if key exists) */
  insert: (row: T) => void;
  
  /** Insert multiple rows */
  insertMany: (rows: T[]) => void;
  
  /** Upsert a row (insert or update based on primary key) */
  upsert: (row: T) => void;
  
  /** Upsert multiple rows */
  upsertMany: (rows: T[]) => void;
  
  /** Update a row by primary key */
  update: (key: Partial<T>, changes: Partial<T>) => void;
  
  /** Remove a row by primary key */
  remove: (key: Partial<T>) => void;
  
  /** Remove rows matching a predicate */
  removeWhere: (predicate: (row: T) => boolean) => void;
  
  /** Clear all data */
  clear: () => void;
  
  /** Replace all data */
  setData: (rows: T[]) => void;
  
  /** Check if a row with the given key exists */
  exists: (key: Partial<T>) => boolean;
  
  /** Get a row by primary key */
  get: (key: Partial<T>) => T | undefined;
  
  /** Performance stats */
  stats: {
    lastUpdateMs: number;
    totalUpdates: number;
  };
}

// ============ HELPER FUNCTIONS ============

/**
 * Generate a composite key string from a row
 */
function generateKey<T extends Record<string, unknown>>(
  row: T,
  primaryKey: (keyof T)[]
): string {
  return primaryKey.map(k => String(row[k])).join('::');
}

/**
 * Generate a key from a partial key object
 */
function generateKeyFromPartial<T extends Record<string, unknown>>(
  partial: Partial<T>,
  primaryKey: (keyof T)[]
): string {
  return primaryKey.map(k => String(partial[k])).join('::');
}

/**
 * Infer SQL schema from data
 */
function inferSchema<T extends Record<string, unknown>>(
  sample: T,
  tableName: string
): string {
  const columns = Object.entries(sample).map(([name, value]) => {
    let type = 'VARCHAR';
    if (typeof value === 'number') {
      type = Number.isInteger(value) ? 'INT' : 'DECIMAL';
    } else if (typeof value === 'boolean') {
      type = 'BOOLEAN';
    }
    return `${name} ${type}`;
  });
  
  return `CREATE TABLE ${tableName} (${columns.join(', ')});`;
}

// ============ THE HOOK ============

export function useDBSP<T extends Record<string, unknown>>(
  options: UseDBSPOptions<T>
): UseDBSPResult<T> {
  const { tableName, initialData = [], sql, primaryKey } = options;
  
  // Generate key function
  const keyFn = useCallback(
    options.keyFn || ((row: T) => generateKey(row, primaryKey)),
    [primaryKey, options.keyFn]
  );
  
  // State: raw data indexed by key
  const [dataMap, setDataMap] = useState<Map<string, T>>(() => {
    const map = new Map<string, T>();
    for (const row of initialData) {
      map.set(keyFn(row), row);
    }
    return map;
  });
  
  // State: transformed output
  const [output, setOutput] = useState<T[]>([]);
  
  // Stats
  const statsRef = useRef({ lastUpdateMs: 0, totalUpdates: 0 });
  
  // DBSP circuit (memoized)
  const dbspContext = useMemo(() => {
    if (!sql) return null;
    
    // Get sample row for schema inference
    const sampleRow = initialData[0] || {} as T;
    if (Object.keys(sampleRow).length === 0) return null;
    
    try {
      const schemaSql = inferSchema(sampleRow, tableName);
      const viewName = `${tableName}_view`;
      const fullSql = `${schemaSql}\nCREATE VIEW ${viewName} AS ${sql};`;
      
      const compiler = new SQLCompiler();
      const { circuit, views } = compiler.compile(fullSql);
      
      return { circuit, view: views[viewName], viewName };
    } catch (e) {
      console.error('DBSP SQL compilation error:', e);
      return null;
    }
  }, [sql, tableName, initialData]);
  
  // Process data through DBSP circuit
  const processData = useCallback((data: Map<string, T>, isInitial: boolean = false) => {
    const start = performance.now();
    
    if (!dbspContext) {
      // No SQL transformation, just return raw data
      setOutput(Array.from(data.values()));
      return;
    }
    
    const { circuit, view, viewName } = dbspContext;
    
    // Convert to ZSet
    const zset = ZSet.fromValues(Array.from(data.values()), keyFn as (v: T) => string);
    
    // Setup output if first time
    if (isInitial) {
      view.integrate().output((result: unknown) => {
        const resultZset = result as ZSet<T>;
        setOutput(resultZset.values());
      });
    }
    
    // Step the circuit
    circuit.step(new Map([[tableName, zset]]));
    
    statsRef.current.lastUpdateMs = performance.now() - start;
    statsRef.current.totalUpdates++;
  }, [dbspContext, tableName, keyFn]);
  
  // Initial processing
  useEffect(() => {
    processData(dataMap, true);
  }, [dbspContext]); // Only on mount or SQL change
  
  // ============ OPERATIONS ============
  
  const insert = useCallback((row: T) => {
    const key = keyFn(row);
    setDataMap(prev => {
      if (prev.has(key)) {
        console.warn(`Row with key ${key} already exists. Use upsert instead.`);
        return prev;
      }
      const next = new Map(prev);
      next.set(key, row);
      processData(next);
      return next;
    });
  }, [keyFn, processData]);
  
  const insertMany = useCallback((rows: T[]) => {
    setDataMap(prev => {
      const next = new Map(prev);
      for (const row of rows) {
        const key = keyFn(row);
        if (!next.has(key)) {
          next.set(key, row);
        }
      }
      processData(next);
      return next;
    });
  }, [keyFn, processData]);
  
  const upsert = useCallback((row: T) => {
    const key = keyFn(row);
    setDataMap(prev => {
      const next = new Map(prev);
      next.set(key, row);
      processData(next);
      return next;
    });
  }, [keyFn, processData]);
  
  const upsertMany = useCallback((rows: T[]) => {
    setDataMap(prev => {
      const next = new Map(prev);
      for (const row of rows) {
        next.set(keyFn(row), row);
      }
      processData(next);
      return next;
    });
  }, [keyFn, processData]);
  
  const update = useCallback((key: Partial<T>, changes: Partial<T>) => {
    const keyStr = generateKeyFromPartial(key, primaryKey);
    setDataMap(prev => {
      const existing = prev.get(keyStr);
      if (!existing) {
        console.warn(`Row with key ${keyStr} not found`);
        return prev;
      }
      const next = new Map(prev);
      next.set(keyStr, { ...existing, ...changes });
      processData(next);
      return next;
    });
  }, [primaryKey, processData]);
  
  const remove = useCallback((key: Partial<T>) => {
    const keyStr = generateKeyFromPartial(key, primaryKey);
    setDataMap(prev => {
      if (!prev.has(keyStr)) return prev;
      const next = new Map(prev);
      next.delete(keyStr);
      processData(next);
      return next;
    });
  }, [primaryKey, processData]);
  
  const removeWhere = useCallback((predicate: (row: T) => boolean) => {
    setDataMap(prev => {
      const next = new Map<string, T>();
      for (const [key, row] of prev) {
        if (!predicate(row)) {
          next.set(key, row);
        }
      }
      if (next.size === prev.size) return prev;
      processData(next);
      return next;
    });
  }, [processData]);
  
  const clear = useCallback(() => {
    setDataMap(new Map());
    setOutput([]);
  }, []);
  
  const setData = useCallback((rows: T[]) => {
    const next = new Map<string, T>();
    for (const row of rows) {
      next.set(keyFn(row), row);
    }
    setDataMap(next);
    processData(next);
  }, [keyFn, processData]);
  
  const exists = useCallback((key: Partial<T>): boolean => {
    const keyStr = generateKeyFromPartial(key, primaryKey);
    return dataMap.has(keyStr);
  }, [dataMap, primaryKey]);
  
  const get = useCallback((key: Partial<T>): T | undefined => {
    const keyStr = generateKeyFromPartial(key, primaryKey);
    return dataMap.get(keyStr);
  }, [dataMap, primaryKey]);
  
  // ============ RETURN ============
  
  return {
    data: output,
    rawData: useMemo(() => Array.from(dataMap.values()), [dataMap]),
    count: dataMap.size,
    insert,
    insertMany,
    upsert,
    upsertMany,
    update,
    remove,
    removeWhere,
    clear,
    setData,
    exists,
    get,
    stats: statsRef.current,
  };
}

// ============ SIMPLIFIED HOOKS ============

/**
 * useDBSPQuery - Simplified hook for read-only queries
 */
export function useDBSPQuery<T extends Record<string, unknown>>(
  data: T[],
  sql: string,
  primaryKey: (keyof T)[]
): T[] {
  const result = useDBSP({
    tableName: 'data',
    initialData: data,
    sql: sql.replace(/FROM\s+\w+/i, 'FROM data'),
    primaryKey,
  });
  
  return result.data;
}

/**
 * useDBSPFilter - Even simpler: just filter data
 */
export function useDBSPFilter<T extends Record<string, unknown>>(
  data: T[],
  predicate: (row: T) => boolean,
  primaryKey: (keyof T)[]
): T[] {
  const [output, setOutput] = useState<T[]>([]);
  
  useEffect(() => {
    setOutput(data.filter(predicate));
  }, [data, predicate]);
  
  return output;
}

// ============ TYPES EXPORT ============

export type { UseDBSPOptions, UseDBSPResult };

