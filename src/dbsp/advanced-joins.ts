/**
 * Advanced Join Implementations
 * 
 * Implements Feldera-style optimizations:
 * 1. ASOF Joins (temporal joins)
 * 2. State Pruning (garbage collection for time-windowed joins)
 * 3. Semi-joins and Anti-joins
 */

// ============ ASOF JOIN ============

/**
 * ASOF Join State
 * 
 * Matches each row from the left stream with the most recent row from the right
 * stream that has:
 * 1. The same join key
 * 2. A timestamp <= the left row's timestamp
 * 
 * Use cases:
 * - Match orders with the latest known price
 * - Match events with the most recent state snapshot
 * - Time-series alignment
 */
export class AsofJoinState<
  TLeft extends Record<string, unknown>,
  TRight extends Record<string, unknown>
> {
  // Left data stored by join key, sorted by timestamp
  private leftByKey = new Map<string, TLeft[]>();
  
  // Right data stored by join key, sorted by timestamp (newest last)
  private rightByKey = new Map<string, TRight[]>();
  
  // Results: [left, right | null]
  private results: Array<[TLeft, TRight | null]> = [];
  
  constructor(
    private getLeftJoinKey: (row: TLeft) => string,
    private getRightJoinKey: (row: TRight) => string,
    private getLeftTimestamp: (row: TLeft) => number,
    private getRightTimestamp: (row: TRight) => number,
    private matchType: 'backward' | 'forward' = 'backward'
  ) {}
  
  /**
   * Insert a left row and find matching right row
   * O(log N) due to binary search
   */
  insertLeft(row: TLeft): TRight | null {
    const jk = this.getLeftJoinKey(row);
    const ts = this.getLeftTimestamp(row);
    
    // Store in left index (maintain sorted order)
    let arr = this.leftByKey.get(jk);
    if (!arr) {
      arr = [];
      this.leftByKey.set(jk, arr);
    }
    // Insert sorted by timestamp
    const insertIdx = this.binarySearchInsertPosition(arr, ts, this.getLeftTimestamp);
    arr.splice(insertIdx, 0, row);
    
    // Find matching right row
    const rightArr = this.rightByKey.get(jk);
    const match = this.findAsofMatch(rightArr, ts);
    
    this.results.push([row, match]);
    return match;
  }
  
  /**
   * Insert a right row
   * May update matches for existing left rows
   */
  insertRight(row: TRight): void {
    const jk = this.getRightJoinKey(row);
    const ts = this.getRightTimestamp(row);
    
    // Store in right index (maintain sorted order)
    let arr = this.rightByKey.get(jk);
    if (!arr) {
      arr = [];
      this.rightByKey.set(jk, arr);
    }
    const insertIdx = this.binarySearchInsertPosition(arr, ts, this.getRightTimestamp);
    arr.splice(insertIdx, 0, row);
    
    // Note: In a full implementation, we'd need to update any left rows
    // that were previously unmatched but now have a match.
    // For simplicity, we assume right data arrives before left data.
  }
  
  private binarySearchInsertPosition<T>(
    arr: T[],
    ts: number,
    getTsFn: (row: T) => number
  ): number {
    let low = 0;
    let high = arr.length;
    
    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      if (getTsFn(arr[mid]) < ts) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    
    return low;
  }
  
  /**
   * Find the right row with the largest timestamp <= targetTs
   */
  private findAsofMatch(rightArr: TRight[] | undefined, targetTs: number): TRight | null {
    if (!rightArr || rightArr.length === 0) return null;
    
    if (this.matchType === 'backward') {
      // Find largest timestamp <= targetTs
      let result: TRight | null = null;
      
      // Binary search for the rightmost element <= targetTs
      let low = 0;
      let high = rightArr.length - 1;
      
      while (low <= high) {
        const mid = Math.floor((low + high) / 2);
        const midTs = this.getRightTimestamp(rightArr[mid]);
        
        if (midTs <= targetTs) {
          result = rightArr[mid];
          low = mid + 1; // Look for a larger valid timestamp
        } else {
          high = mid - 1;
        }
      }
      
      return result;
    } else {
      // Forward match: find smallest timestamp >= targetTs
      let low = 0;
      let high = rightArr.length;
      
      while (low < high) {
        const mid = Math.floor((low + high) / 2);
        if (this.getRightTimestamp(rightArr[mid]) < targetTs) {
          low = mid + 1;
        } else {
          high = mid;
        }
      }
      
      return low < rightArr.length ? rightArr[low] : null;
    }
  }
  
  getResults(): Array<[TLeft, TRight | null]> {
    return this.results;
  }
  
  get count(): number {
    return this.results.length;
  }
  
  clear(): void {
    this.leftByKey.clear();
    this.rightByKey.clear();
    this.results = [];
  }
}

// ============ STATE PRUNING ============

/**
 * State-Pruned Join State
 * 
 * Implements garbage collection for time-windowed joins.
 * When joining on monotonic columns (timestamps), we can prune
 * old state that can never match future rows.
 * 
 * Example: If we're joining orders with customers where
 * order.timestamp >= customer.created_at, and the newest order
 * is at t=1000, we can GC all customers created before t=1000 - window
 */
export class StatePrunedJoinState<
  TLeft extends Record<string, unknown>,
  TRight extends Record<string, unknown>
> {
  // Same structure as OptimizedJoinState but with GC
  private leftValues = new Map<string, TLeft>();
  private leftIndex = new Map<string, Set<string>>();
  private leftTimestamps = new Map<string, number>(); // pk -> timestamp
  
  private rightValues = new Map<string, TRight>();
  private rightIndex = new Map<string, Set<string>>();
  private rightTimestamps = new Map<string, number>();
  
  private results = new Map<string, { left: TLeft; right: TRight }>();
  
  // Watermarks for GC
  private leftWatermark: number = -Infinity;
  private rightWatermark: number = -Infinity;
  
  // GC stats
  private gcStats = {
    leftRowsPruned: 0,
    rightRowsPruned: 0,
    lastGcTime: 0,
  };
  
  constructor(
    private getLeftKey: (row: TLeft) => string,
    private getRightKey: (row: TRight) => string,
    private getLeftJoinKey: (row: TLeft) => string,
    private getRightJoinKey: (row: TRight) => string,
    private getLeftTimestamp: (row: TLeft) => number,
    private getRightTimestamp: (row: TRight) => number,
    private retentionWindow: number = 0 // How long to keep old data
  ) {}
  
  /**
   * Insert a left row with automatic GC
   */
  insertLeft(row: TLeft): void {
    const pk = this.getLeftKey(row);
    const jk = this.getLeftJoinKey(row);
    const ts = this.getLeftTimestamp(row);
    
    // Update watermark
    if (ts > this.leftWatermark) {
      this.leftWatermark = ts;
      this.maybeGcRight(); // Left watermark can GC right side
    }
    
    // Store value and timestamp
    this.leftValues.set(pk, row);
    this.leftTimestamps.set(pk, ts);
    
    // Update join index
    let indexSet = this.leftIndex.get(jk);
    if (!indexSet) {
      indexSet = new Set();
      this.leftIndex.set(jk, indexSet);
    }
    indexSet.add(pk);
    
    // Find matching rights
    const rightPks = this.rightIndex.get(jk);
    if (rightPks) {
      for (const rightPk of rightPks) {
        const right = this.rightValues.get(rightPk)!;
        this.results.set(`${pk}::${rightPk}`, { left: row, right });
      }
    }
  }
  
  /**
   * Insert a right row with automatic GC
   */
  insertRight(row: TRight): void {
    const pk = this.getRightKey(row);
    const jk = this.getRightJoinKey(row);
    const ts = this.getRightTimestamp(row);
    
    // Update watermark
    if (ts > this.rightWatermark) {
      this.rightWatermark = ts;
      this.maybeGcLeft(); // Right watermark can GC left side
    }
    
    // Store value and timestamp
    this.rightValues.set(pk, row);
    this.rightTimestamps.set(pk, ts);
    
    // Update join index
    let indexSet = this.rightIndex.get(jk);
    if (!indexSet) {
      indexSet = new Set();
      this.rightIndex.set(jk, indexSet);
    }
    indexSet.add(pk);
    
    // Find matching lefts
    const leftPks = this.leftIndex.get(jk);
    if (leftPks) {
      for (const leftPk of leftPks) {
        const left = this.leftValues.get(leftPk)!;
        this.results.set(`${leftPk}::${pk}`, { left, right: row });
      }
    }
  }
  
  /**
   * Garbage collect old left rows based on right watermark
   */
  private maybeGcLeft(): void {
    if (this.retentionWindow <= 0) return;
    
    const cutoff = this.rightWatermark - this.retentionWindow;
    let pruned = 0;
    
    for (const [pk, ts] of this.leftTimestamps.entries()) {
      if (ts < cutoff) {
        const row = this.leftValues.get(pk);
        if (row) {
          const jk = this.getLeftJoinKey(row);
          
          // Remove from values
          this.leftValues.delete(pk);
          this.leftTimestamps.delete(pk);
          
          // Remove from index
          const indexSet = this.leftIndex.get(jk);
          if (indexSet) {
            indexSet.delete(pk);
            if (indexSet.size === 0) {
              this.leftIndex.delete(jk);
            }
          }
          
          // Remove from results (all results involving this left row)
          for (const resultKey of this.results.keys()) {
            if (resultKey.startsWith(`${pk}::`)) {
              this.results.delete(resultKey);
            }
          }
          
          pruned++;
        }
      }
    }
    
    this.gcStats.leftRowsPruned += pruned;
    if (pruned > 0) {
      this.gcStats.lastGcTime = Date.now();
    }
  }
  
  /**
   * Garbage collect old right rows based on left watermark
   */
  private maybeGcRight(): void {
    if (this.retentionWindow <= 0) return;
    
    const cutoff = this.leftWatermark - this.retentionWindow;
    let pruned = 0;
    
    for (const [pk, ts] of this.rightTimestamps.entries()) {
      if (ts < cutoff) {
        const row = this.rightValues.get(pk);
        if (row) {
          const jk = this.getRightJoinKey(row);
          
          // Remove from values
          this.rightValues.delete(pk);
          this.rightTimestamps.delete(pk);
          
          // Remove from index
          const indexSet = this.rightIndex.get(jk);
          if (indexSet) {
            indexSet.delete(pk);
            if (indexSet.size === 0) {
              this.rightIndex.delete(jk);
            }
          }
          
          // Remove from results
          for (const resultKey of this.results.keys()) {
            if (resultKey.endsWith(`::${pk}`)) {
              this.results.delete(resultKey);
            }
          }
          
          pruned++;
        }
      }
    }
    
    this.gcStats.rightRowsPruned += pruned;
    if (pruned > 0) {
      this.gcStats.lastGcTime = Date.now();
    }
  }
  
  getResults(): [TLeft, TRight][] {
    return Array.from(this.results.values()).map(({ left, right }) => [left, right]);
  }
  
  get count(): number {
    return this.results.size;
  }
  
  get leftCount(): number {
    return this.leftValues.size;
  }
  
  get rightCount(): number {
    return this.rightValues.size;
  }
  
  getGcStats(): typeof this.gcStats {
    return { ...this.gcStats };
  }
  
  getMemoryEstimate(): { leftBytes: number; rightBytes: number; resultBytes: number } {
    // Rough estimate: 100 bytes per object
    const BYTES_PER_ROW = 100;
    return {
      leftBytes: this.leftValues.size * BYTES_PER_ROW,
      rightBytes: this.rightValues.size * BYTES_PER_ROW,
      resultBytes: this.results.size * 2 * BYTES_PER_ROW,
    };
  }
  
  clear(): void {
    this.leftValues.clear();
    this.leftIndex.clear();
    this.leftTimestamps.clear();
    this.rightValues.clear();
    this.rightIndex.clear();
    this.rightTimestamps.clear();
    this.results.clear();
    this.leftWatermark = -Infinity;
    this.rightWatermark = -Infinity;
  }
}

// ============ INCREMENTAL SEMI-JOIN ============

/**
 * Incremental Semi-Join
 * 
 * Returns only the left rows that have at least one matching right row.
 * More efficient than full join when you only need existence check.
 */
export class IncrementalSemiJoinState<
  TLeft extends Record<string, unknown>,
  TRight extends Record<string, unknown>
> {
  // Left table
  private leftValues = new Map<string, TLeft>();
  private leftIndex = new Map<string, Set<string>>();
  
  // Right key existence (we only need to track which keys exist)
  private rightKeyCount = new Map<string, number>();
  
  // Result: left rows that have matching right
  private results = new Map<string, TLeft>();
  
  constructor(
    private getLeftKey: (row: TLeft) => string,
    private getLeftJoinKey: (row: TLeft) => string,
    private getRightJoinKey: (row: TRight) => string
  ) {}
  
  insertLeft(row: TLeft): boolean {
    const pk = this.getLeftKey(row);
    const jk = this.getLeftJoinKey(row);
    
    // Store
    this.leftValues.set(pk, row);
    
    // Update index
    let indexSet = this.leftIndex.get(jk);
    if (!indexSet) {
      indexSet = new Set();
      this.leftIndex.set(jk, indexSet);
    }
    indexSet.add(pk);
    
    // Check if right key exists
    const rightExists = (this.rightKeyCount.get(jk) ?? 0) > 0;
    if (rightExists) {
      this.results.set(pk, row);
    }
    
    return rightExists;
  }
  
  insertRight(row: TRight): number {
    const jk = this.getRightJoinKey(row);
    
    // Increment count
    const prevCount = this.rightKeyCount.get(jk) ?? 0;
    this.rightKeyCount.set(jk, prevCount + 1);
    
    // If this is the first right with this key, add all matching lefts to results
    let added = 0;
    if (prevCount === 0) {
      const leftPks = this.leftIndex.get(jk);
      if (leftPks) {
        for (const leftPk of leftPks) {
          const left = this.leftValues.get(leftPk)!;
          this.results.set(leftPk, left);
          added++;
        }
      }
    }
    
    return added;
  }
  
  removeLeft(pk: string): void {
    const row = this.leftValues.get(pk);
    if (!row) return;
    
    const jk = this.getLeftJoinKey(row);
    
    this.leftValues.delete(pk);
    this.results.delete(pk);
    
    const indexSet = this.leftIndex.get(jk);
    if (indexSet) {
      indexSet.delete(pk);
      if (indexSet.size === 0) {
        this.leftIndex.delete(jk);
      }
    }
  }
  
  removeRight(row: TRight): number {
    const jk = this.getRightJoinKey(row);
    
    const count = this.rightKeyCount.get(jk) ?? 0;
    if (count === 0) return 0;
    
    const newCount = count - 1;
    if (newCount === 0) {
      this.rightKeyCount.delete(jk);
      
      // Remove all matching lefts from results
      let removed = 0;
      const leftPks = this.leftIndex.get(jk);
      if (leftPks) {
        for (const leftPk of leftPks) {
          this.results.delete(leftPk);
          removed++;
        }
      }
      return removed;
    } else {
      this.rightKeyCount.set(jk, newCount);
      return 0;
    }
  }
  
  getResults(): TLeft[] {
    return Array.from(this.results.values());
  }
  
  get count(): number {
    return this.results.size;
  }
  
  clear(): void {
    this.leftValues.clear();
    this.leftIndex.clear();
    this.rightKeyCount.clear();
    this.results.clear();
  }
}

// ============ INCREMENTAL ANTI-JOIN ============

/**
 * Incremental Anti-Join
 * 
 * Returns only the left rows that have NO matching right row.
 * Useful for finding orphaned records or implementing LEFT JOIN with NULL check.
 */
export class IncrementalAntiJoinState<
  TLeft extends Record<string, unknown>,
  TRight extends Record<string, unknown>
> {
  // Left table
  private leftValues = new Map<string, TLeft>();
  private leftIndex = new Map<string, Set<string>>();
  
  // Right key existence
  private rightKeyCount = new Map<string, number>();
  
  // Result: left rows that have NO matching right
  private results = new Map<string, TLeft>();
  
  constructor(
    private getLeftKey: (row: TLeft) => string,
    private getLeftJoinKey: (row: TLeft) => string,
    private getRightJoinKey: (row: TRight) => string
  ) {}
  
  insertLeft(row: TLeft): boolean {
    const pk = this.getLeftKey(row);
    const jk = this.getLeftJoinKey(row);
    
    // Store
    this.leftValues.set(pk, row);
    
    // Update index
    let indexSet = this.leftIndex.get(jk);
    if (!indexSet) {
      indexSet = new Set();
      this.leftIndex.set(jk, indexSet);
    }
    indexSet.add(pk);
    
    // Check if right key exists
    const rightExists = (this.rightKeyCount.get(jk) ?? 0) > 0;
    if (!rightExists) {
      this.results.set(pk, row);
      return true;
    }
    
    return false;
  }
  
  insertRight(row: TRight): number {
    const jk = this.getRightJoinKey(row);
    
    // Increment count
    const prevCount = this.rightKeyCount.get(jk) ?? 0;
    this.rightKeyCount.set(jk, prevCount + 1);
    
    // If this is the first right with this key, remove all matching lefts from results
    let removed = 0;
    if (prevCount === 0) {
      const leftPks = this.leftIndex.get(jk);
      if (leftPks) {
        for (const leftPk of leftPks) {
          this.results.delete(leftPk);
          removed++;
        }
      }
    }
    
    return removed;
  }
  
  removeRight(row: TRight): number {
    const jk = this.getRightJoinKey(row);
    
    const count = this.rightKeyCount.get(jk) ?? 0;
    if (count === 0) return 0;
    
    const newCount = count - 1;
    if (newCount === 0) {
      this.rightKeyCount.delete(jk);
      
      // Add all matching lefts to results (they're now unmatched)
      let added = 0;
      const leftPks = this.leftIndex.get(jk);
      if (leftPks) {
        for (const leftPk of leftPks) {
          const left = this.leftValues.get(leftPk)!;
          this.results.set(leftPk, left);
          added++;
        }
      }
      return added;
    } else {
      this.rightKeyCount.set(jk, newCount);
      return 0;
    }
  }
  
  getResults(): TLeft[] {
    return Array.from(this.results.values());
  }
  
  get count(): number {
    return this.results.size;
  }
  
  clear(): void {
    this.leftValues.clear();
    this.leftIndex.clear();
    this.rightKeyCount.clear();
    this.results.clear();
  }
}
