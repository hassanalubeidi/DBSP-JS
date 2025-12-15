/**
 * Columnar Storage for DBSP
 * 
 * Optimized for OLAP workloads using:
 * - Column-major storage for cache-friendly aggregations
 * - TypedArrays for numeric columns (10-100x faster)
 * - Bitmap masks for efficient filtering
 * - Vectorized operations without eval/Function
 * 
 * Key insight: For aggregations like SUM(price), we only need to read
 * the price column. Row-based storage forces us to read entire objects.
 * 
 * Row format: [{id:1, price:10}, {id:2, price:20}] - poor cache locality
 * Columnar:   {id: Int32Array[1,2], price: Float64Array[10,20]} - perfect locality
 */

import { ZSet, type Weight } from './zset';

// ============ TYPE DEFINITIONS ============

export type ColumnType = 'int32' | 'float64' | 'string' | 'boolean';

export interface ColumnSchema {
  name: string;
  type: ColumnType;
}

export interface TableSchema {
  columns: ColumnSchema[];
}

// TypedArray types we use
type NumericArray = Int32Array | Float64Array;
type ColumnData = NumericArray | string[] | Uint8Array; // Uint8Array for booleans

// ============ COLUMNAR TABLE ============

/**
 * ColumnarTable - Column-major storage optimized for OLAP
 * 
 * Performance characteristics:
 * - Aggregations: O(n) with optimal cache locality
 * - Filter: O(n) using bitmap masks
 * - Project: O(1) - just reference columns
 * - Insert: O(1) amortized (may need resize)
 */
export class ColumnarTable {
  private columns: Map<string, ColumnData> = new Map();
  private columnTypes: Map<string, ColumnType> = new Map();
  private weights: Int32Array;
  private _size: number = 0;
  private capacity: number;

  constructor(schema: TableSchema, initialCapacity: number = 1024) {
    this.capacity = initialCapacity;
    this.weights = new Int32Array(initialCapacity);

    for (const col of schema.columns) {
      this.columnTypes.set(col.name, col.type);
      this.columns.set(col.name, this.createColumn(col.type, initialCapacity));
    }
  }

  private createColumn(type: ColumnType, size: number): ColumnData {
    switch (type) {
      case 'int32': return new Int32Array(size);
      case 'float64': return new Float64Array(size);
      case 'boolean': return new Uint8Array(size);
      case 'string': return new Array(size).fill('');
    }
  }

  // ============ BASIC OPERATIONS ============

  get size(): number {
    return this._size;
  }

  /**
   * Insert a row with weight
   */
  insert(row: Record<string, unknown>, weight: Weight = 1): void {
    if (this._size >= this.capacity) {
      this.grow();
    }

    const idx = this._size;
    this.weights[idx] = weight;

    for (const [name, data] of this.columns) {
      const value = row[name];
      const type = this.columnTypes.get(name)!;
      
      switch (type) {
        case 'int32':
          (data as Int32Array)[idx] = value as number;
          break;
        case 'float64':
          (data as Float64Array)[idx] = value as number;
          break;
        case 'boolean':
          (data as Uint8Array)[idx] = value ? 1 : 0;
          break;
        case 'string':
          (data as string[])[idx] = value as string;
          break;
      }
    }

    this._size++;
  }

  /**
   * Bulk insert from array of rows - much faster than individual inserts
   */
  bulkInsert(rows: Record<string, unknown>[], weight: Weight = 1): void {
    const needed = this._size + rows.length;
    while (this.capacity < needed) {
      this.grow();
    }

    for (let i = 0; i < rows.length; i++) {
      const idx = this._size + i;
      const row = rows[i];
      this.weights[idx] = weight;

      for (const [name, data] of this.columns) {
        const value = row[name];
        const type = this.columnTypes.get(name)!;
        
        switch (type) {
          case 'int32':
            (data as Int32Array)[idx] = value as number;
            break;
          case 'float64':
            (data as Float64Array)[idx] = value as number;
            break;
          case 'boolean':
            (data as Uint8Array)[idx] = value ? 1 : 0;
            break;
          case 'string':
            (data as string[])[idx] = value as string;
            break;
        }
      }
    }

    this._size += rows.length;
  }

  private grow(): void {
    const newCapacity = this.capacity * 2;
    
    // Grow weights
    const newWeights = new Int32Array(newCapacity);
    newWeights.set(this.weights);
    this.weights = newWeights;

    // Grow each column
    for (const [name, data] of this.columns) {
      const type = this.columnTypes.get(name)!;
      const newData = this.createColumn(type, newCapacity);
      
      if (data instanceof Int32Array || data instanceof Float64Array || data instanceof Uint8Array) {
        (newData as NumericArray | Uint8Array).set(data);
      } else {
        // String array
        for (let i = 0; i < this._size; i++) {
          (newData as string[])[i] = (data as string[])[i];
        }
      }
      
      this.columns.set(name, newData);
    }

    this.capacity = newCapacity;
  }

  // ============ COLUMN ACCESS ============

  /**
   * Get a column by name (direct access for maximum performance)
   */
  getColumn<T extends ColumnData>(name: string): T {
    return this.columns.get(name) as T;
  }

  getWeights(): Int32Array {
    return this.weights;
  }

  getColumnType(name: string): ColumnType | undefined {
    return this.columnTypes.get(name);
  }

  // ============ VECTORIZED AGGREGATIONS ============

  /**
   * COUNT(*) - Sum of all weights
   * Uses tight loop over TypedArray - V8 optimizes this heavily
   */
  count(): number {
    const weights = this.weights;
    const n = this._size;
    let sum = 0;
    
    // Unrolled loop for better performance
    const limit = n - (n % 4);
    for (let i = 0; i < limit; i += 4) {
      sum += weights[i] + weights[i + 1] + weights[i + 2] + weights[i + 3];
    }
    for (let i = limit; i < n; i++) {
      sum += weights[i];
    }
    
    return sum;
  }

  /**
   * SUM(column) - Vectorized sum with weights
   * 10-100x faster than object iteration
   */
  sum(columnName: string): number {
    const column = this.columns.get(columnName);
    if (!column || !(column instanceof Float64Array || column instanceof Int32Array)) {
      throw new Error(`Column ${columnName} is not numeric`);
    }

    const weights = this.weights;
    const n = this._size;
    let sum = 0;

    // Tight loop - V8 will optimize this to near-native speed
    for (let i = 0; i < n; i++) {
      sum += column[i] * weights[i];
    }

    return sum;
  }

  /**
   * AVG(column) - Vectorized average
   */
  avg(columnName: string): number {
    const totalWeight = this.count();
    if (totalWeight === 0) return 0;
    return this.sum(columnName) / totalWeight;
  }

  /**
   * MIN(column) - Vectorized minimum (considering positive weights only)
   */
  min(columnName: string): number {
    const column = this.columns.get(columnName);
    if (!column || !(column instanceof Float64Array || column instanceof Int32Array)) {
      throw new Error(`Column ${columnName} is not numeric`);
    }

    const weights = this.weights;
    const n = this._size;
    let min = Infinity;

    for (let i = 0; i < n; i++) {
      if (weights[i] > 0 && column[i] < min) {
        min = column[i];
      }
    }

    return min === Infinity ? 0 : min;
  }

  /**
   * MAX(column) - Vectorized maximum (considering positive weights only)
   */
  max(columnName: string): number {
    const column = this.columns.get(columnName);
    if (!column || !(column instanceof Float64Array || column instanceof Int32Array)) {
      throw new Error(`Column ${columnName} is not numeric`);
    }

    const weights = this.weights;
    const n = this._size;
    let max = -Infinity;

    for (let i = 0; i < n; i++) {
      if (weights[i] > 0 && column[i] > max) {
        max = column[i];
      }
    }

    return max === -Infinity ? 0 : max;
  }

  // ============ BITMAP-BASED FILTERING ============

  /**
   * Create a bitmap mask from a predicate on a numeric column
   * Returns Uint8Array where 1 = matches, 0 = doesn't
   */
  createMaskNumeric(
    columnName: string, 
    op: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'between',
    value: number,
    value2?: number
  ): Uint8Array {
    const column = this.columns.get(columnName);
    if (!column || !(column instanceof Float64Array || column instanceof Int32Array)) {
      throw new Error(`Column ${columnName} is not numeric`);
    }

    const n = this._size;
    const mask = new Uint8Array(n);

    switch (op) {
      case '=':
        for (let i = 0; i < n; i++) mask[i] = column[i] === value ? 1 : 0;
        break;
      case '!=':
        for (let i = 0; i < n; i++) mask[i] = column[i] !== value ? 1 : 0;
        break;
      case '<':
        for (let i = 0; i < n; i++) mask[i] = column[i] < value ? 1 : 0;
        break;
      case '>':
        for (let i = 0; i < n; i++) mask[i] = column[i] > value ? 1 : 0;
        break;
      case '<=':
        for (let i = 0; i < n; i++) mask[i] = column[i] <= value ? 1 : 0;
        break;
      case '>=':
        for (let i = 0; i < n; i++) mask[i] = column[i] >= value ? 1 : 0;
        break;
      case 'between':
        for (let i = 0; i < n; i++) mask[i] = column[i] >= value && column[i] <= value2! ? 1 : 0;
        break;
    }

    return mask;
  }

  /**
   * Create a bitmap mask from a string column predicate
   */
  createMaskString(
    columnName: string,
    op: '=' | '!=' | 'in' | 'like',
    value: string | string[]
  ): Uint8Array {
    const column = this.columns.get(columnName);
    if (!column || !Array.isArray(column)) {
      throw new Error(`Column ${columnName} is not a string column`);
    }

    const n = this._size;
    const mask = new Uint8Array(n);

    switch (op) {
      case '=':
        for (let i = 0; i < n; i++) mask[i] = column[i] === value ? 1 : 0;
        break;
      case '!=':
        for (let i = 0; i < n; i++) mask[i] = column[i] !== value ? 1 : 0;
        break;
      case 'in': {
        const valueSet = new Set(value as string[]);
        for (let i = 0; i < n; i++) mask[i] = valueSet.has(column[i]) ? 1 : 0;
        break;
      }
      case 'like': {
        // Simple LIKE implementation: % at start/end
        const pattern = value as string;
        const startsWith = pattern.endsWith('%') && !pattern.startsWith('%');
        const endsWith = pattern.startsWith('%') && !pattern.endsWith('%');
        const contains = pattern.startsWith('%') && pattern.endsWith('%');
        const exact = !pattern.includes('%');
        
        const core = pattern.replace(/%/g, '');
        
        if (exact) {
          for (let i = 0; i < n; i++) mask[i] = column[i] === core ? 1 : 0;
        } else if (startsWith) {
          for (let i = 0; i < n; i++) mask[i] = column[i].startsWith(core) ? 1 : 0;
        } else if (endsWith) {
          for (let i = 0; i < n; i++) mask[i] = column[i].endsWith(core) ? 1 : 0;
        } else if (contains) {
          for (let i = 0; i < n; i++) mask[i] = column[i].includes(core) ? 1 : 0;
        }
        break;
      }
    }

    return mask;
  }

  /**
   * Combine masks with AND
   */
  static andMasks(a: Uint8Array, b: Uint8Array): Uint8Array {
    const n = a.length;
    const result = new Uint8Array(n);
    
    // Process 4 elements at a time for better performance
    const limit = n - (n % 4);
    for (let i = 0; i < limit; i += 4) {
      result[i] = a[i] & b[i];
      result[i + 1] = a[i + 1] & b[i + 1];
      result[i + 2] = a[i + 2] & b[i + 2];
      result[i + 3] = a[i + 3] & b[i + 3];
    }
    for (let i = limit; i < n; i++) {
      result[i] = a[i] & b[i];
    }
    
    return result;
  }

  /**
   * Combine masks with OR
   */
  static orMasks(a: Uint8Array, b: Uint8Array): Uint8Array {
    const n = a.length;
    const result = new Uint8Array(n);
    
    const limit = n - (n % 4);
    for (let i = 0; i < limit; i += 4) {
      result[i] = a[i] | b[i];
      result[i + 1] = a[i + 1] | b[i + 1];
      result[i + 2] = a[i + 2] | b[i + 2];
      result[i + 3] = a[i + 3] | b[i + 3];
    }
    for (let i = limit; i < n; i++) {
      result[i] = a[i] | b[i];
    }
    
    return result;
  }

  /**
   * Negate a mask (NOT)
   */
  static notMask(a: Uint8Array): Uint8Array {
    const n = a.length;
    const result = new Uint8Array(n);
    
    for (let i = 0; i < n; i++) {
      result[i] = a[i] ? 0 : 1;
    }
    
    return result;
  }

  // ============ MASKED AGGREGATIONS ============

  /**
   * COUNT with mask - only count rows where mask[i] = 1
   */
  countMasked(mask: Uint8Array): number {
    const weights = this.weights;
    const n = this._size;
    let sum = 0;
    
    for (let i = 0; i < n; i++) {
      sum += weights[i] * mask[i];
    }
    
    return sum;
  }

  /**
   * SUM with mask - only sum rows where mask[i] = 1
   */
  sumMasked(columnName: string, mask: Uint8Array): number {
    const column = this.columns.get(columnName);
    if (!column || !(column instanceof Float64Array || column instanceof Int32Array)) {
      throw new Error(`Column ${columnName} is not numeric`);
    }

    const weights = this.weights;
    const n = this._size;
    let sum = 0;

    for (let i = 0; i < n; i++) {
      sum += column[i] * weights[i] * mask[i];
    }

    return sum;
  }

  /**
   * AVG with mask
   */
  avgMasked(columnName: string, mask: Uint8Array): number {
    const count = this.countMasked(mask);
    if (count === 0) return 0;
    return this.sumMasked(columnName, mask) / count;
  }

  // ============ CONVERSION ============

  /**
   * Convert from ZSet (row-based) to ColumnarTable
   */
  static fromZSet<T extends Record<string, unknown>>(
    zset: ZSet<T>,
    schema: TableSchema
  ): ColumnarTable {
    const entries = Array.from(zset.entries());
    const table = new ColumnarTable(schema, Math.max(entries.length, 1024));

    for (const [row, weight] of entries) {
      table.insert(row, weight);
    }

    return table;
  }

  /**
   * Convert to ZSet (row-based)
   */
  toZSet<T extends Record<string, unknown>>(keyFn?: (row: T) => string): ZSet<T> {
    const zset = new ZSet<T>(keyFn);
    const columnNames = Array.from(this.columns.keys());

    for (let i = 0; i < this._size; i++) {
      const row: Record<string, unknown> = {};
      
      for (const name of columnNames) {
        const data = this.columns.get(name)!;
        const type = this.columnTypes.get(name)!;
        
        switch (type) {
          case 'int32':
          case 'float64':
            row[name] = (data as NumericArray)[i];
            break;
          case 'boolean':
            row[name] = (data as Uint8Array)[i] === 1;
            break;
          case 'string':
            row[name] = (data as string[])[i];
            break;
        }
      }
      
      zset.insert(row as T, this.weights[i]);
    }

    return zset;
  }

  /**
   * Create filtered table using mask - returns new ColumnarTable
   */
  filter(mask: Uint8Array): ColumnarTable {
    // Count matching rows
    let matchCount = 0;
    for (let i = 0; i < this._size; i++) {
      if (mask[i]) matchCount++;
    }

    // Create schema from current columns
    const schema: TableSchema = {
      columns: Array.from(this.columnTypes.entries()).map(([name, type]) => ({ name, type }))
    };

    const result = new ColumnarTable(schema, Math.max(matchCount, 64));

    // Copy matching rows
    let outIdx = 0;
    for (let i = 0; i < this._size; i++) {
      if (mask[i]) {
        result.weights[outIdx] = this.weights[i];
        
        for (const [name, data] of this.columns) {
          const outData = result.columns.get(name)!;
          const type = this.columnTypes.get(name)!;
          
          switch (type) {
            case 'int32':
              (outData as Int32Array)[outIdx] = (data as Int32Array)[i];
              break;
            case 'float64':
              (outData as Float64Array)[outIdx] = (data as Float64Array)[i];
              break;
            case 'boolean':
              (outData as Uint8Array)[outIdx] = (data as Uint8Array)[i];
              break;
            case 'string':
              (outData as string[])[outIdx] = (data as string[])[i];
              break;
          }
        }
        
        outIdx++;
      }
    }

    result._size = matchCount;
    return result;
  }
}

// ============ COLUMNAR ZSET (Incremental) ============

/**
 * ColumnarZSet - Columnar storage with ZSet semantics for incremental processing
 * 
 * Combines the performance of columnar storage with DBSP's incremental model.
 */
export class ColumnarZSet<T extends Record<string, unknown>> {
  private table: ColumnarTable;
  private schema: TableSchema;
  private keyFn?: (row: T) => string;

  constructor(schema: TableSchema, keyFn?: (row: T) => string, initialCapacity?: number) {
    this.schema = schema;
    this.keyFn = keyFn;
    this.table = new ColumnarTable(schema, initialCapacity);
  }

  // ============ ZSET INTERFACE ============

  insert(row: T, weight: Weight = 1): void {
    this.table.insert(row, weight);
  }

  bulkInsert(rows: T[], weight: Weight = 1): void {
    this.table.bulkInsert(rows, weight);
  }

  size(): number {
    return this.table.size;
  }

  // ============ AGGREGATIONS (Optimized) ============

  count(): number {
    return this.table.count();
  }

  sum(columnName: string): number {
    return this.table.sum(columnName);
  }

  avg(columnName: string): number {
    return this.table.avg(columnName);
  }

  min(columnName: string): number {
    return this.table.min(columnName);
  }

  max(columnName: string): number {
    return this.table.max(columnName);
  }

  // ============ FILTERING (Optimized) ============

  filter(predicate: (row: T) => boolean): ColumnarZSet<T> {
    // For complex predicates, fall back to row-by-row
    // For simple predicates, use createMask* methods for 10x+ speedup
    const zset = this.table.toZSet<T>(this.keyFn);
    const filtered = zset.filter(predicate);
    return ColumnarZSet.fromZSet(filtered, this.schema, this.keyFn);
  }

  /**
   * Optimized filter using column predicates
   */
  filterNumeric(
    columnName: string,
    op: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'between',
    value: number,
    value2?: number
  ): ColumnarZSet<T> {
    const mask = this.table.createMaskNumeric(columnName, op, value, value2);
    const filteredTable = this.table.filter(mask);
    const result = new ColumnarZSet<T>(this.schema, this.keyFn);
    result.table = filteredTable;
    return result;
  }

  filterString(
    columnName: string,
    op: '=' | '!=' | 'in' | 'like',
    value: string | string[]
  ): ColumnarZSet<T> {
    const mask = this.table.createMaskString(columnName, op, value);
    const filteredTable = this.table.filter(mask);
    const result = new ColumnarZSet<T>(this.schema, this.keyFn);
    result.table = filteredTable;
    return result;
  }

  // ============ CONVERSION ============

  static fromZSet<T extends Record<string, unknown>>(
    zset: ZSet<T>,
    schema: TableSchema,
    keyFn?: (row: T) => string
  ): ColumnarZSet<T> {
    const result = new ColumnarZSet<T>(schema, keyFn);
    result.table = ColumnarTable.fromZSet(zset, schema);
    return result;
  }

  toZSet(): ZSet<T> {
    return this.table.toZSet<T>(this.keyFn);
  }

  getTable(): ColumnarTable {
    return this.table;
  }
}

// ============ SCHEMA INFERENCE ============

/**
 * Infer schema from a sample row
 */
export function inferSchema(sample: Record<string, unknown>): TableSchema {
  const columns: ColumnSchema[] = [];
  
  for (const [name, value] of Object.entries(sample)) {
    let type: ColumnType;
    
    if (typeof value === 'number') {
      type = Number.isInteger(value) ? 'int32' : 'float64';
    } else if (typeof value === 'boolean') {
      type = 'boolean';
    } else {
      type = 'string';
    }
    
    columns.push({ name, type });
  }
  
  return { columns };
}

