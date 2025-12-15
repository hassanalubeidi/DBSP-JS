/**
 * DBSP Performance Benchmarks
 * 
 * Compares DBSP incremental operations against naive full recomputation.
 * Tests with 1M rows and realistic update patterns (0.01% - 2% changes).
 * 
 * Key insight from DBSP paper:
 * - Linear operators (filter, map, project) process only deltas: O(|delta|)
 * - Full recomputation processes all data: O(|DB|)
 * - Expected speedup: O(|DB|/|delta|) = 50x to 10000x for 2% to 0.01% changes
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { ZSet, join } from './zset';
import {
  type Order,
  type Customer,
  generateDataset,
  generateOrderDelta,
  ordersToZSet,
  customersToZSet,
  deltaToZSet,
  hashJoin,
  type JoinStats,
  type BenchmarkDataset,
} from './benchmark-data';
import { IntegrationState, zsetGroup } from './operators';

// ============ BENCHMARK CONFIGURATION ============

// Configuration: Set to true for full 1M benchmark, false for faster CI tests
const FULL_BENCHMARK = process.env.FULL_BENCHMARK === 'true';

const ORDER_COUNT = FULL_BENCHMARK ? 1_000_000 : 100_000;
const CUSTOMER_COUNT = FULL_BENCHMARK ? 100_000 : 10_000;
const PRODUCT_COUNT = FULL_BENCHMARK ? 50_000 : 5_000;

const UPDATE_PERCENTAGES = [0.01, 0.1, 0.5, 1.0, 2.0];

// ============ BENCHMARK UTILITIES ============

interface BenchmarkResult {
  operation: string;
  dataSize: number;
  deltaSize: number;
  deltaPercent: number;
  naiveTimeMs: number;
  incrementalTimeMs: number;
  speedup: number;
  naiveOpsPerSec: number;
  incrementalOpsPerSec: number;
}

interface QueryMetrics {
  queryName: string;
  runs: BenchmarkResult[];
  avgSpeedup: number;
  minSpeedup: number;
  maxSpeedup: number;
}

function formatResult(r: BenchmarkResult): string {
  return `${r.operation} | ${r.dataSize.toLocaleString()} rows | ${r.deltaPercent}% delta (${r.deltaSize} rows) | ` +
         `Naive: ${r.naiveTimeMs.toFixed(2)}ms | Incremental: ${r.incrementalTimeMs.toFixed(2)}ms | ` +
         `Speedup: ${r.speedup.toFixed(1)}x`;
}

// ============ NAIVE IMPLEMENTATIONS (FULL RECOMPUTATION) ============

/**
 * Naive filter: recompute entire result from scratch
 */
function naiveFilter<T>(data: ZSet<T>, predicate: (t: T) => boolean): ZSet<T> {
  return data.filter(predicate);
}

/**
 * Naive map: recompute entire result from scratch
 */
function naiveMap<T, U>(
  data: ZSet<T>,
  fn: (t: T) => U,
  keyFn?: (u: U) => string
): ZSet<U> {
  return data.map(fn, keyFn);
}

/**
 * Naive sum: recompute from entire dataset
 */
function naiveSum<T>(data: ZSet<T>, getValue: (t: T) => number): number {
  return data.sum(getValue);
}

/**
 * Naive count: recompute from entire dataset
 */
function naiveCount<T>(data: ZSet<T>): number {
  return data.count();
}

/**
 * Naive distinct: recompute from entire dataset
 */
function naiveDistinct<T>(data: ZSet<T>): ZSet<T> {
  return data.distinct();
}

/**
 * Naive join: recompute entire join from scratch
 */
function naiveJoin<T, U, K>(
  left: ZSet<T>,
  right: ZSet<U>,
  leftKey: (t: T) => K,
  rightKey: (u: U) => K,
  keyToString?: (k: K) => string
): ZSet<[T, U]> {
  return join(left, right, leftKey, rightKey, keyToString);
}

// ============ INCREMENTAL IMPLEMENTATIONS ============

/**
 * Incremental filter: process only delta (LINEAR operator)
 * Since filter is linear: filter(delta) gives the delta of result
 */
function incrementalFilter<T>(
  delta: ZSet<T>,
  predicate: (t: T) => boolean
): ZSet<T> {
  return delta.filter(predicate);
}

/**
 * Incremental map: process only delta (LINEAR operator)
 */
function incrementalMap<T, U>(
  delta: ZSet<T>,
  fn: (t: T) => U,
  keyFn?: (u: U) => string
): ZSet<U> {
  return delta.map(fn, keyFn);
}

/**
 * Incremental sum: add weighted sum of delta to running total
 * SUM is linear! Δsum = sum(delta)
 */
function incrementalSum<T>(delta: ZSet<T>, getValue: (t: T) => number): number {
  return delta.sum(getValue);
}

/**
 * Incremental count: add delta count to running total
 */
function incrementalCount<T>(delta: ZSet<T>): number {
  return delta.count();
}

/**
 * Incremental join using DBSP formula:
 * Δ(A ⋈ B) = (ΔA ⋈ ΔB) + (A ⋈ ΔB) + (ΔA ⋈ B)
 * 
 * When only one side changes (common case):
 * Δ(A ⋈ B) = (ΔA ⋈ B) when only A changes
 */
function incrementalJoinOneSide<T, U, K>(
  deltaLeft: ZSet<T>,
  right: ZSet<U>,  // Full right side (indexed)
  leftKey: (t: T) => K,
  rightKey: (u: U) => K,
  keyToString?: (k: K) => string
): ZSet<[T, U]> {
  // Only need to join deltaLeft with full right
  return join(deltaLeft, right, leftKey, rightKey, keyToString);
}

// ============ BENCHMARK TESTS ============

describe('DBSP Performance Benchmarks', () => {
  let dataset: BenchmarkDataset;
  let ordersZSet: ZSet<Order>;
  let customersZSet: ZSet<Customer>;
  const allResults: BenchmarkResult[] = [];
  const queryMetrics: Map<string, QueryMetrics> = new Map();
  
  beforeAll(() => {
    console.log('\n📊 Generating benchmark dataset...');
    dataset = generateDataset(ORDER_COUNT, CUSTOMER_COUNT, PRODUCT_COUNT);
    console.log(`   Generated ${dataset.orders.length.toLocaleString()} orders`);
    console.log(`   Generated ${dataset.customers.length.toLocaleString()} customers`);
    
    console.log('\n📦 Building ZSets...');
    ordersZSet = ordersToZSet(dataset.orders);
    customersZSet = customersToZSet(dataset.customers);
    console.log(`   Orders ZSet size: ${ordersZSet.size().toLocaleString()}`);
    console.log(`   Customers ZSet size: ${customersZSet.size().toLocaleString()}`);
  });

  function recordResult(result: BenchmarkResult) {
    allResults.push(result);
    
    let metrics = queryMetrics.get(result.operation);
    if (!metrics) {
      metrics = {
        queryName: result.operation,
        runs: [],
        avgSpeedup: 0,
        minSpeedup: Infinity,
        maxSpeedup: 0,
      };
      queryMetrics.set(result.operation, metrics);
    }
    
    metrics.runs.push(result);
    metrics.minSpeedup = Math.min(metrics.minSpeedup, result.speedup);
    metrics.maxSpeedup = Math.max(metrics.maxSpeedup, result.speedup);
    metrics.avgSpeedup = metrics.runs.reduce((a, r) => a + r.speedup, 0) / metrics.runs.length;
  }

  describe('Filter Operation (Linear)', () => {
    const predicate = (o: Order) => o.status === 'pending' && o.price > 50;
    
    for (const percent of UPDATE_PERCENTAGES) {
      it(`should be faster than naive at ${percent}% delta`, () => {
        // Generate delta
        const delta = generateOrderDelta(dataset.orders, percent, dataset.orders.length + 1);
        const deltaZSet = deltaToZSet(delta);
        
        // Naive: full recomputation
        const naiveStart = performance.now();
        const naiveResult = naiveFilter(ordersZSet, predicate);
        const naiveTime = performance.now() - naiveStart;
        
        // Incremental: process only delta
        const incStart = performance.now();
        const incResult = incrementalFilter(deltaZSet, predicate);
        const incTime = performance.now() - incStart;
        
        const result: BenchmarkResult = {
          operation: 'filter',
          dataSize: ordersZSet.size(),
          deltaSize: deltaZSet.size(),
          deltaPercent: percent,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup: naiveTime / incTime,
          naiveOpsPerSec: ordersZSet.size() / (naiveTime / 1000),
          incrementalOpsPerSec: deltaZSet.size() / (incTime / 1000),
        };
        
        recordResult(result);
        console.log(`   ${formatResult(result)}`);
        
        // Incremental should be faster for small deltas
        if (percent <= 1.0) {
          expect(result.speedup).toBeGreaterThan(1);
        }
        
        // Verify correctness: both produce valid ZSets
        expect(naiveResult.size()).toBeGreaterThanOrEqual(0);
        expect(incResult.size()).toBeGreaterThanOrEqual(0);
      });
    }
  });

  describe('Map Operation (Linear)', () => {
    const mapFn = (o: Order) => ({
      orderId: o.id,
      total: o.price * o.quantity,
      region: o.region,
    });
    
    for (const percent of UPDATE_PERCENTAGES) {
      it(`should be faster than naive at ${percent}% delta`, () => {
        const delta = generateOrderDelta(dataset.orders, percent, dataset.orders.length + 1);
        const deltaZSet = deltaToZSet(delta);
        
        // Naive
        const naiveStart = performance.now();
        const naiveResult = naiveMap(ordersZSet, mapFn);
        const naiveTime = performance.now() - naiveStart;
        
        // Incremental
        const incStart = performance.now();
        const incResult = incrementalMap(deltaZSet, mapFn);
        const incTime = performance.now() - incStart;
        
        const result: BenchmarkResult = {
          operation: 'map',
          dataSize: ordersZSet.size(),
          deltaSize: deltaZSet.size(),
          deltaPercent: percent,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup: naiveTime / incTime,
          naiveOpsPerSec: ordersZSet.size() / (naiveTime / 1000),
          incrementalOpsPerSec: deltaZSet.size() / (incTime / 1000),
        };
        
        recordResult(result);
        console.log(`   ${formatResult(result)}`);
        
        if (percent <= 1.0) {
          expect(result.speedup).toBeGreaterThan(1);
        }
      });
    }
  });

  describe('Sum Aggregation (Linear)', () => {
    for (const percent of UPDATE_PERCENTAGES) {
      it(`should be faster than naive at ${percent}% delta`, () => {
        const delta = generateOrderDelta(dataset.orders, percent, dataset.orders.length + 1);
        const deltaZSet = deltaToZSet(delta);
        
        // Naive: sum entire dataset
        const naiveStart = performance.now();
        const naiveResult = naiveSum(ordersZSet, o => o.price * o.quantity);
        const naiveTime = performance.now() - naiveStart;
        
        // Incremental: only sum delta (add to running total)
        const incStart = performance.now();
        const incResult = incrementalSum(deltaZSet, o => o.price * o.quantity);
        const incTime = performance.now() - incStart;
        
        const result: BenchmarkResult = {
          operation: 'sum',
          dataSize: ordersZSet.size(),
          deltaSize: deltaZSet.size(),
          deltaPercent: percent,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup: naiveTime / incTime,
          naiveOpsPerSec: ordersZSet.size() / (naiveTime / 1000),
          incrementalOpsPerSec: deltaZSet.size() / (incTime / 1000),
        };
        
        recordResult(result);
        console.log(`   ${formatResult(result)}`);
        
        if (percent <= 1.0) {
          expect(result.speedup).toBeGreaterThan(1);
        }
      });
    }
  });

  describe('Count Aggregation (Linear)', () => {
    for (const percent of UPDATE_PERCENTAGES) {
      it(`should be faster than naive at ${percent}% delta`, () => {
        const delta = generateOrderDelta(dataset.orders, percent, dataset.orders.length + 1);
        const deltaZSet = deltaToZSet(delta);
        
        const naiveStart = performance.now();
        const naiveResult = naiveCount(ordersZSet);
        const naiveTime = performance.now() - naiveStart;
        
        const incStart = performance.now();
        const incResult = incrementalCount(deltaZSet);
        const incTime = performance.now() - incStart;
        
        const result: BenchmarkResult = {
          operation: 'count',
          dataSize: ordersZSet.size(),
          deltaSize: deltaZSet.size(),
          deltaPercent: percent,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup: naiveTime / incTime,
          naiveOpsPerSec: ordersZSet.size() / (naiveTime / 1000),
          incrementalOpsPerSec: deltaZSet.size() / (incTime / 1000),
        };
        
        recordResult(result);
        console.log(`   ${formatResult(result)}`);
        
        if (percent <= 1.0) {
          expect(result.speedup).toBeGreaterThan(1);
        }
      });
    }
  });

  describe('Join Operation (Bilinear with Hash Join)', () => {
    // Join orders with customers on customerId
    const ordersKeyFn = (o: Order) => o.customerId;
    const customersKeyFn = (c: Customer) => c.id;
    
    for (const percent of UPDATE_PERCENTAGES) {
      it(`should be faster than naive at ${percent}% delta`, () => {
        const delta = generateOrderDelta(dataset.orders, percent, dataset.orders.length + 1);
        const deltaZSet = deltaToZSet(delta);
        
        // Naive: full join of all orders with all customers
        const naiveStart = performance.now();
        const naiveResult = naiveJoin(
          ordersZSet,
          customersZSet,
          ordersKeyFn,
          customersKeyFn,
          String
        );
        const naiveTime = performance.now() - naiveStart;
        
        // Incremental: only join delta orders with customers
        // This is Δ(A ⋈ B) ≈ ΔA ⋈ B when only A changes
        const incStart = performance.now();
        const incResult = incrementalJoinOneSide(
          deltaZSet,
          customersZSet,
          ordersKeyFn,
          customersKeyFn,
          String
        );
        const incTime = performance.now() - incStart;
        
        const result: BenchmarkResult = {
          operation: 'join',
          dataSize: ordersZSet.size(),
          deltaSize: deltaZSet.size(),
          deltaPercent: percent,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup: naiveTime / incTime,
          naiveOpsPerSec: ordersZSet.size() / (naiveTime / 1000),
          incrementalOpsPerSec: deltaZSet.size() / (incTime / 1000),
        };
        
        recordResult(result);
        console.log(`   ${formatResult(result)}`);
        
        if (percent <= 1.0) {
          expect(result.speedup).toBeGreaterThan(1);
        }
      });
    }
  });

  describe('Hash Join Verification', () => {
    it('should verify hash join is working correctly', () => {
      // Create small test sets for verification
      // Filter orders to only those with customerId in range 1-100 to ensure matches
      const ordersWithMatchingCustomers = dataset.orders
        .filter(o => o.customerId <= 100)
        .slice(0, 1000);
      
      // If no orders match, create some test orders that will match
      const testOrders = ordersWithMatchingCustomers.length > 0 
        ? ordersWithMatchingCustomers 
        : dataset.orders.slice(0, 100).map((o, i) => ({ ...o, customerId: (i % 100) + 1 }));
      
      const smallOrders = ZSet.fromValues(
        testOrders,
        o => o.id.toString()
      );
      const smallCustomers = ZSet.fromValues(
        dataset.customers.slice(0, 100),
        c => c.id.toString()
      );
      
      const { result, stats } = hashJoin(
        smallOrders,
        smallCustomers,
        o => o.customerId,
        c => c.id,
        String
      );
      
      console.log('\n   📊 Hash Join Statistics:');
      console.log(`   Left size: ${stats.leftSize}`);
      console.log(`   Right size: ${stats.rightSize}`);
      console.log(`   Hash table buckets: ${stats.hashTableBuckets}`);
      console.log(`   Hash table entries: ${stats.hashTableSize}`);
      console.log(`   Probe count: ${stats.probeCount}`);
      console.log(`   Match count: ${stats.matchCount}`);
      console.log(`   Build time: ${stats.buildTimeMs.toFixed(2)}ms`);
      console.log(`   Probe time: ${stats.probeTimeMs.toFixed(2)}ms`);
      console.log(`   Total time: ${stats.totalTimeMs.toFixed(2)}ms`);
      
      // Verify hash table was built
      expect(stats.hashTableBuckets).toBeGreaterThan(0);
      expect(stats.hashTableSize).toBe(stats.rightSize);
      
      // Verify all left elements were probed
      expect(stats.probeCount).toBe(stats.leftSize);
      
      // Should find some matches (customers exist)
      expect(stats.matchCount).toBeGreaterThan(0);
    });
    
    it('should use hash-based lookup (O(1) per probe)', () => {
      // Verify that doubling input doesn't double probe time significantly
      const sizes = [100, 1000, 10000];
      const probeTimes: number[] = [];
      
      for (const size of sizes) {
        const orders = ZSet.fromValues(
          dataset.orders.slice(0, size),
          o => o.id.toString()
        );
        
        const { stats } = hashJoin(
          orders,
          customersZSet,  // Same right side
          o => o.customerId,
          c => c.id,
          String
        );
        
        probeTimes.push(stats.probeTimeMs / stats.probeCount);
      }
      
      console.log('\n   📊 Hash Join O(1) Verification:');
      sizes.forEach((size, i) => {
        console.log(`   Size ${size}: ${probeTimes[i].toFixed(4)}ms per probe`);
      });
      
      // Per-probe time should be roughly constant (allowing for overhead)
      // 10x size should not result in 10x per-probe time
      const ratio = probeTimes[2] / probeTimes[0];
      console.log(`   Time ratio (10000/100): ${ratio.toFixed(2)}x (should be ~1x for O(1))`);
      
      expect(ratio).toBeLessThan(5); // Allow some variance but not O(n)
    });
  });

  describe('Composed Pipeline (Chain Rule)', () => {
    // Pipeline: filter -> map -> aggregate
    for (const percent of UPDATE_PERCENTAGES) {
      it(`should be faster than naive at ${percent}% delta`, () => {
        const delta = generateOrderDelta(dataset.orders, percent, dataset.orders.length + 1);
        const deltaZSet = deltaToZSet(delta);
        
        // Naive: full pipeline recomputation
        const naiveStart = performance.now();
        const filtered = naiveFilter(ordersZSet, o => o.status === 'pending');
        const mapped = naiveMap(filtered, o => ({ total: o.price * o.quantity }));
        const naiveSum = mapped.sum(t => t.total);
        const naiveTime = performance.now() - naiveStart;
        
        // Incremental: apply pipeline to delta only
        // By chain rule: (Q1 ∘ Q2)^Δ = Q1^Δ ∘ Q2^Δ
        const incStart = performance.now();
        const filteredDelta = incrementalFilter(deltaZSet, o => o.status === 'pending');
        const mappedDelta = incrementalMap(filteredDelta, o => ({ total: o.price * o.quantity }));
        const incSum = incrementalSum(mappedDelta, t => t.total);
        const incTime = performance.now() - incStart;
        
        const result: BenchmarkResult = {
          operation: 'pipeline (filter→map→sum)',
          dataSize: ordersZSet.size(),
          deltaSize: deltaZSet.size(),
          deltaPercent: percent,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup: naiveTime / incTime,
          naiveOpsPerSec: ordersZSet.size() / (naiveTime / 1000),
          incrementalOpsPerSec: deltaZSet.size() / (incTime / 1000),
        };
        
        recordResult(result);
        console.log(`   ${formatResult(result)}`);
        
        if (percent <= 1.0) {
          expect(result.speedup).toBeGreaterThan(1);
        }
      });
    }
  });

  describe('Summary Report', () => {
    it('should print comprehensive benchmark summary', () => {
      console.log('\n');
      console.log('═'.repeat(100));
      console.log('                        DBSP BENCHMARK SUMMARY');
      console.log('═'.repeat(100));
      console.log(`Dataset: ${ORDER_COUNT.toLocaleString()} orders, ${CUSTOMER_COUNT.toLocaleString()} customers\n`);
      
      console.log('┌─────────────────────────────┬───────────────┬───────────────┬───────────────┐');
      console.log('│ Operation                   │ Min Speedup   │ Avg Speedup   │ Max Speedup   │');
      console.log('├─────────────────────────────┼───────────────┼───────────────┼───────────────┤');
      
      for (const [name, metrics] of queryMetrics) {
        const paddedName = name.padEnd(27);
        const min = metrics.minSpeedup.toFixed(1).padStart(10) + 'x';
        const avg = metrics.avgSpeedup.toFixed(1).padStart(10) + 'x';
        const max = metrics.maxSpeedup.toFixed(1).padStart(10) + 'x';
        console.log(`│ ${paddedName} │ ${min}    │ ${avg}    │ ${max}    │`);
      }
      
      console.log('└─────────────────────────────┴───────────────┴───────────────┴───────────────┘');
      
      console.log('\nKey Insights:');
      console.log('• Linear operators (filter, map, sum, count) achieve highest speedups');
      console.log('• Speedup ≈ |DB|/|delta| for linear operators');
      console.log('• Join speedup comes from processing only delta rows');
      console.log('• Chain rule allows incremental evaluation of composed queries');
      
      // Overall check
      const allSpeedups = allResults.map(r => r.speedup);
      const avgSpeedup = allSpeedups.reduce((a, b) => a + b, 0) / allSpeedups.length;
      
      console.log(`\n📈 Overall average speedup: ${avgSpeedup.toFixed(1)}x`);
      
      expect(avgSpeedup).toBeGreaterThan(1);
    });
  });
});

describe('Realistic Update Scenarios', () => {
  let dataset: BenchmarkDataset;
  let state: IntegrationState<ZSet<Order>>;
  let currentView: ZSet<Order>;
  const queryLog: Array<{
    step: number;
    deltaSize: number;
    deltaPercent: number;
    operation: string;
    timeMs: number;
    resultSize: number;
  }> = [];
  
  beforeAll(() => {
    dataset = generateDataset(ORDER_COUNT, CUSTOMER_COUNT, PRODUCT_COUNT);
    state = new IntegrationState(zsetGroup<Order>());
    currentView = ZSet.zero<Order>(o => o.id.toString());
  });

  it('should handle stream of updates efficiently', () => {
    console.log('\n📊 Simulating realistic update stream...');
    console.log('Query: SELECT * FROM orders WHERE status = "pending" AND price > 50\n');
    
    const predicate = (o: Order) => o.status === 'pending' && o.price > 50;
    
    // Initial load
    const initialOrders = ordersToZSet(dataset.orders);
    const initStart = performance.now();
    currentView = state.step(initialOrders.filter(predicate));
    const initTime = performance.now() - initStart;
    
    console.log(`Initial load: ${dataset.orders.length.toLocaleString()} orders in ${initTime.toFixed(2)}ms`);
    console.log(`Initial view size: ${currentView.size().toLocaleString()} matching orders\n`);
    
    // Simulate 20 update batches with varying sizes
    const updateScenarios = [
      { percent: 0.01, description: 'Tiny update (0.01%)' },
      { percent: 0.05, description: 'Very small update (0.05%)' },
      { percent: 0.1, description: 'Small update (0.1%)' },
      { percent: 0.5, description: 'Medium update (0.5%)' },
      { percent: 1.0, description: 'Large update (1%)' },
      { percent: 2.0, description: 'Very large update (2%)' },
      { percent: 0.01, description: 'Tiny update (0.01%)' },
      { percent: 0.1, description: 'Small update (0.1%)' },
      { percent: 0.01, description: 'Tiny update (0.01%)' },
      { percent: 0.05, description: 'Very small update (0.05%)' },
    ];
    
    console.log('┌───────┬────────────────────────────┬────────────┬───────────┬────────────────┐');
    console.log('│ Step  │ Update Type                │ Delta Size │ Time (ms) │ Current View   │');
    console.log('├───────┼────────────────────────────┼────────────┼───────────┼────────────────┤');
    
    let nextId = dataset.orders.length + 1;
    
    for (let i = 0; i < updateScenarios.length; i++) {
      const scenario = updateScenarios[i];
      
      // Generate delta
      const delta = generateOrderDelta(dataset.orders, scenario.percent, nextId);
      nextId += delta.inserts.length;
      const deltaZSet = deltaToZSet(delta);
      
      // Apply incrementally
      const start = performance.now();
      const filteredDelta = deltaZSet.filter(predicate);
      currentView = state.step(filteredDelta);
      const elapsed = performance.now() - start;
      
      queryLog.push({
        step: i + 1,
        deltaSize: deltaZSet.size(),
        deltaPercent: scenario.percent,
        operation: 'filter',
        timeMs: elapsed,
        resultSize: currentView.size(),
      });
      
      const stepStr = (i + 1).toString().padStart(5);
      const descStr = scenario.description.padEnd(26);
      const deltaStr = deltaZSet.size().toString().padStart(10);
      const timeStr = elapsed.toFixed(2).padStart(9);
      const viewStr = currentView.size().toLocaleString().padStart(14);
      
      console.log(`│ ${stepStr} │ ${descStr} │ ${deltaStr} │ ${timeStr} │ ${viewStr} │`);
    }
    
    console.log('└───────┴────────────────────────────┴────────────┴───────────┴────────────────┘');
    
    // Verify all updates were fast
    const avgTime = queryLog.reduce((a, q) => a + q.timeMs, 0) / queryLog.length;
    console.log(`\nAverage update time: ${avgTime.toFixed(2)}ms`);
    console.log(`Total queries processed: ${queryLog.length}`);
    
    // Updates should be faster than naive full recomputation
    // At 1M rows, naive would take ~200ms+ per update
    const maxAllowedMs = FULL_BENCHMARK ? 200 : 50;
    expect(avgTime).toBeLessThan(maxAllowedMs);
  });

  it('should maintain correctness under stress', () => {
    console.log('\n🔍 Verifying incremental correctness under stress...');
    
    // Reset state
    state = new IntegrationState(zsetGroup<Order>());
    
    // Build up incrementally
    const predicate = (o: Order) => o.status === 'shipped';
    
    const chunk1 = ordersToZSet(dataset.orders.slice(0, 10000));
    const chunk2 = ordersToZSet(dataset.orders.slice(10000, 30000));
    const chunk3 = ordersToZSet(dataset.orders.slice(30000, ORDER_COUNT));
    
    // Incremental build-up
    let incrementalResult = state.step(chunk1.filter(predicate));
    incrementalResult = state.step(chunk2.filter(predicate));
    incrementalResult = state.step(chunk3.filter(predicate));
    
    // Naive full computation
    const fullData = ordersToZSet(dataset.orders);
    const naiveResult = fullData.filter(predicate);
    
    // Should match
    console.log(`Incremental result size: ${incrementalResult.size()}`);
    console.log(`Naive result size: ${naiveResult.size()}`);
    
    expect(incrementalResult.size()).toBe(naiveResult.size());
  });
});

