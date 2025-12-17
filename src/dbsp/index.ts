/**
 * DBSP: Database Stream Processor
 * 
 * A TypeScript implementation of DBSP for incremental view maintenance.
 * Based on the paper "DBSP: Automatic Incremental View Maintenance for Rich Query Languages"
 * 
 * Core concepts:
 * - ZSet: A set with integer weights (elements from an abelian group)
 * - Stream: An infinite sequence of values over time
 * - Operators: Functions that transform streams (lift, delay, integrate, differentiate)
 * - Circuit: A builder for constructing DBSP dataflow graphs
 */

export * from './zset';
export * from './stream';
export * from './operators';
export * from './circuit';
export * from './examples';
export * from './benchmark-data';
export * from './sql/sql-compiler';
export * from './columnar';
export * from './useDBSP';
export * from './async-stream';
export * from './react/useJoinDBSP';

// Re-export optimized join operators from zset
export { 
  IndexedZSet, 
  joinFilter, 
  joinFilterMap, 
  joinWithIndex, 
  antiJoin, 
  semiJoin 
} from './zset';

// Re-export high-performance join implementations
export { 
  OptimizedJoinState, 
  AppendOnlyJoinState,
  benchmarkJoin,
} from './optimized-join';

// Re-export advanced join types
export {
  AsofJoinState,
  StatePrunedJoinState,
  IncrementalSemiJoinState,
  IncrementalAntiJoinState,
} from './advanced-joins';
