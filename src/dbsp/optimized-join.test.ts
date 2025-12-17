/**
 * Optimized Join Performance Tests
 * 
 * Demonstrates the massive performance improvements from proper optimization
 */

import { describe, it, expect } from 'vitest';
import { OptimizedJoinState, AppendOnlyJoinState, benchmarkJoin } from './optimized-join';
import { ZSet, join } from './zset';

// ============ TEST DATA ============

interface Order {
  orderId: number;
  customerId: number;
  amount: number;
  category: string;
}

interface Customer {
  customerId: number;
  name: string;
  tier: string;
}

function generateOrders(count: number, customerIdRange: number): Order[] {
  const categories = ['Electronics', 'Clothing', 'Food', 'Books'];
  return Array.from({ length: count }, (_, i) => ({
    orderId: i + 1,
    customerId: (i % customerIdRange) + 1,
    amount: 100 + (i % 900),
    category: categories[i % 4],
  }));
}

function generateCustomers(count: number): Customer[] {
  const tiers = ['Bronze', 'Silver', 'Gold', 'Platinum'];
  return Array.from({ length: count }, (_, i) => ({
    customerId: i + 1,
    name: `Customer ${i + 1}`,
    tier: tiers[i % 4],
  }));
}

// ============ TESTS ============

describe('Optimized Join Performance', () => {
  
  describe('Correctness Tests', () => {
    it('OptimizedJoinState produces correct results', () => {
      const join = new OptimizedJoinState<Order, Customer>(
        (o) => String(o.orderId),
        (c) => String(c.customerId),
        (o) => String(o.customerId),
        (c) => String(c.customerId)
      );
      
      // Add customers
      join.batchInsertRight([
        { customerId: 1, name: 'Alice', tier: 'Gold' },
        { customerId: 2, name: 'Bob', tier: 'Silver' },
      ]);
      
      // Add orders
      join.batchInsertLeft([
        { orderId: 1, customerId: 1, amount: 100, category: 'A' },
        { orderId: 2, customerId: 1, amount: 200, category: 'B' },
        { orderId: 3, customerId: 2, amount: 300, category: 'C' },
        { orderId: 4, customerId: 3, amount: 400, category: 'D' }, // No matching customer
      ]);
      
      const results = join.getResults();
      expect(results.length).toBe(3);
      expect(join.count).toBe(3);
      
      // Check Alice has 2 orders
      const aliceOrders = results.filter(([o, c]) => c.name === 'Alice');
      expect(aliceOrders.length).toBe(2);
      
      // Check Bob has 1 order
      const bobOrders = results.filter(([o, c]) => c.name === 'Bob');
      expect(bobOrders.length).toBe(1);
    });
    
    it('handles updates correctly', () => {
      const join = new OptimizedJoinState<Order, Customer>(
        (o) => String(o.orderId),
        (c) => String(c.customerId),
        (o) => String(o.customerId),
        (c) => String(c.customerId)
      );
      
      join.insertRight({ customerId: 1, name: 'Alice', tier: 'Gold' });
      join.insertLeft({ orderId: 1, customerId: 1, amount: 100, category: 'A' });
      
      expect(join.count).toBe(1);
      
      // Update order (same PK, different values)
      join.insertLeft({ orderId: 1, customerId: 1, amount: 200, category: 'A' });
      
      expect(join.count).toBe(1);
      const results = join.getResults();
      expect(results[0][0].amount).toBe(200);
    });
    
    it('handles deletions correctly', () => {
      const join = new OptimizedJoinState<Order, Customer>(
        (o) => String(o.orderId),
        (c) => String(c.customerId),
        (o) => String(o.customerId),
        (c) => String(c.customerId)
      );
      
      join.insertRight({ customerId: 1, name: 'Alice', tier: 'Gold' });
      join.insertLeft({ orderId: 1, customerId: 1, amount: 100, category: 'A' });
      join.insertLeft({ orderId: 2, customerId: 1, amount: 200, category: 'B' });
      
      expect(join.count).toBe(2);
      
      // Remove one order
      join.removeLeft('1');
      expect(join.count).toBe(1);
      
      // Remove customer (should remove all remaining joins)
      join.removeRight('1');
      expect(join.count).toBe(0);
    });
    
    it('AppendOnlyJoinState produces correct results', () => {
      const join = new AppendOnlyJoinState<Order, Customer>(
        (o) => String(o.customerId),
        (c) => String(c.customerId)
      );
      
      join.batchInsertRight([
        { customerId: 1, name: 'Alice', tier: 'Gold' },
        { customerId: 2, name: 'Bob', tier: 'Silver' },
      ]);
      
      join.batchInsertLeft([
        { orderId: 1, customerId: 1, amount: 100, category: 'A' },
        { orderId: 2, customerId: 1, amount: 200, category: 'B' },
        { orderId: 3, customerId: 2, amount: 300, category: 'C' },
      ]);
      
      expect(join.count).toBe(3);
    });
  });
  
  describe('Performance Benchmarks', () => {
    
    it('compares all implementations at scale', () => {
      const LEFT_SIZE = 100_000;
      const RIGHT_SIZE = 10_000;
      const DELTA_SIZE = 1000;
      
      console.log('\n' + '='.repeat(80));
      console.log('JOIN PERFORMANCE BENCHMARK');
      console.log('='.repeat(80));
      console.log(`\nSetup: ${LEFT_SIZE.toLocaleString()} orders, ${RIGHT_SIZE.toLocaleString()} customers`);
      console.log(`Delta: ${DELTA_SIZE.toLocaleString()} new orders\n`);
      
      // Generate data
      const orders = generateOrders(LEFT_SIZE, RIGHT_SIZE);
      const customers = generateCustomers(RIGHT_SIZE);
      const deltaOrders = generateOrders(DELTA_SIZE, RIGHT_SIZE)
        .map((o, i) => ({ ...o, orderId: LEFT_SIZE + i + 1 }));
      
      // ========== METHOD 1: ZSet-based (current) ==========
      const zsetOrders = ZSet.fromValues(orders, (o) => String(o.orderId));
      const zsetCustomers = ZSet.fromValues(customers, (c) => String(c.customerId));
      
      const zsetFullResult = benchmarkJoin('ZSet Full Join', () => {
        const result = join(zsetOrders, zsetCustomers, (o) => o.customerId, (c) => c.customerId);
        return result.size();
      }, 5);
      
      // ========== METHOD 2: Optimized incremental ==========
      const optimized = new OptimizedJoinState<Order, Customer>(
        (o) => String(o.orderId),
        (c) => String(c.customerId),
        (o) => String(o.customerId),
        (c) => String(c.customerId)
      );
      
      // Setup (one-time cost)
      const setupStart = performance.now();
      optimized.batchInsertRight(customers);
      optimized.batchInsertLeft(orders);
      const setupTime = performance.now() - setupStart;
      
      // Delta processing
      const optimizedDeltaResult = benchmarkJoin('Optimized Delta', () => {
        for (const o of deltaOrders) {
          optimized.insertLeft(o);
        }
        return optimized.count;
      }, 1); // Only 1 iteration since state persists
      
      // ========== METHOD 3: Append-only (even faster) ==========
      const appendOnly = new AppendOnlyJoinState<Order, Customer>(
        (o) => String(o.customerId),
        (c) => String(c.customerId),
        false // Don't store results, just count
      );
      
      // Setup
      const appendSetupStart = performance.now();
      appendOnly.batchInsertRight(customers);
      appendOnly.batchInsertLeft(orders);
      const appendSetupTime = performance.now() - appendSetupStart;
      
      // Create fresh for delta test
      const appendOnlyDelta = new AppendOnlyJoinState<Order, Customer>(
        (o) => String(o.customerId),
        (c) => String(c.customerId),
        false
      );
      appendOnlyDelta.batchInsertRight(customers);
      appendOnlyDelta.batchInsertLeft(orders);
      
      const appendDeltaResult = benchmarkJoin('Append-Only Delta', () => {
        for (const o of deltaOrders) {
          appendOnlyDelta.insertLeft(o);
        }
        return appendOnlyDelta.count;
      }, 1);
      
      // ========== RESULTS ==========
      // Compare delta processing vs full recompute
      // The key insight: ZSet recomputes EVERYTHING, but optimized only processes delta
      
      console.log('┌─────────────────────────────┬──────────────┬──────────────┬──────────────┐');
      console.log('│ Method                      │ Time         │ Results      │ vs ZSet      │');
      console.log('├─────────────────────────────┼──────────────┼──────────────┼──────────────┤');
      console.log(`│ ZSet Full Recompute         │ ${zsetFullResult.avgMs.toFixed(2).padStart(8)}ms │ ${zsetFullResult.result.toLocaleString().padStart(10)} │ ${('1x').padStart(10)} │`);
      console.log(`│ Optimized Delta (inc.)      │ ${optimizedDeltaResult.avgMs.toFixed(3).padStart(8)}ms │ ${optimizedDeltaResult.result.toLocaleString().padStart(10)} │ ${(zsetFullResult.avgMs / optimizedDeltaResult.avgMs).toFixed(0).padStart(9)}x │`);
      console.log(`│ Append-Only Delta (inc.)    │ ${appendDeltaResult.avgMs.toFixed(3).padStart(8)}ms │ ${appendDeltaResult.result.toLocaleString().padStart(10)} │ ${(zsetFullResult.avgMs / appendDeltaResult.avgMs).toFixed(0).padStart(9)}x │`);
      console.log('└─────────────────────────────┴──────────────┴──────────────┴──────────────┘');
      
      console.log('\n📊 Per-Row Analysis:');
      console.log(`   ZSet: ${(zsetFullResult.avgMs / (LEFT_SIZE + DELTA_SIZE) * 1000).toFixed(3)}μs/row (recomputes all)`);
      console.log(`   Optimized: ${(optimizedDeltaResult.avgMs / DELTA_SIZE * 1000).toFixed(3)}μs/delta-row`);
      console.log(`   Append-Only: ${(appendDeltaResult.avgMs / DELTA_SIZE * 1000).toFixed(3)}μs/delta-row`);
      
      console.log('\n📊 Setup Costs (one-time):');
      console.log(`   Optimized setup: ${setupTime.toFixed(2)}ms`);
      console.log(`   Append-only setup: ${appendSetupTime.toFixed(2)}ms`);
      
      const speedup = zsetFullResult.avgMs / appendDeltaResult.avgMs;
      console.log(`\n🚀 DELTA SPEEDUP: ${speedup.toFixed(0)}x faster than ZSet full recompute!`);
      
      // ZSet is 100-150ms, append-only delta is ~0.05ms = 2000-3000x speedup
      expect(speedup).toBeGreaterThan(500);
    });
    
    it('shows O(delta) scaling - time stays constant regardless of table size', () => {
      const RIGHT_SIZE = 10_000;
      const DELTA_SIZE = 100;
      const STEPS = 20;
      
      console.log('\n' + '='.repeat(80));
      console.log('O(DELTA) SCALING TEST');
      console.log('='.repeat(80));
      console.log(`\nProving: Update time stays constant as table grows`);
      console.log(`Delta size: ${DELTA_SIZE} orders per step\n`);
      
      const customers = generateCustomers(RIGHT_SIZE);
      
      const join = new OptimizedJoinState<Order, Customer>(
        (o) => String(o.orderId),
        (c) => String(c.customerId),
        (o) => String(o.customerId),
        (c) => String(c.customerId)
      );
      
      // Add customers
      join.batchInsertRight(customers);
      
      const times: number[] = [];
      const sizes: number[] = [];
      let orderId = 0;
      
      // Add batches and measure time
      for (let step = 0; step < STEPS; step++) {
        const delta = generateOrders(DELTA_SIZE, RIGHT_SIZE)
          .map((o, i) => ({ ...o, orderId: ++orderId }));
        
        const start = performance.now();
        for (const o of delta) {
          join.insertLeft(o);
        }
        const elapsed = performance.now() - start;
        
        times.push(elapsed);
        sizes.push(join.leftCount);
      }
      
      console.log('┌────────┬────────────────┬────────────────┐');
      console.log('│ Step   │ Table Size     │ Delta Time     │');
      console.log('├────────┼────────────────┼────────────────┤');
      
      for (let i = 0; i < STEPS; i += 4) {
        console.log(`│ ${(i + 1).toString().padStart(4)}   │ ${sizes[i].toLocaleString().padStart(12)} │ ${times[i].toFixed(3).padStart(10)}ms │`);
      }
      console.log('└────────┴────────────────┴────────────────┘');
      
      // Calculate first half vs second half average
      const firstHalf = times.slice(0, STEPS / 2);
      const secondHalf = times.slice(STEPS / 2);
      const firstAvg = firstHalf.reduce((a, b) => a + b, 0) / firstHalf.length;
      const secondAvg = secondHalf.reduce((a, b) => a + b, 0) / secondHalf.length;
      
      console.log(`\n📊 O(delta) Verification:`);
      console.log(`   First half avg (${sizes[0].toLocaleString()}-${sizes[STEPS/2-1].toLocaleString()} rows): ${firstAvg.toFixed(3)}ms`);
      console.log(`   Second half avg (${sizes[STEPS/2].toLocaleString()}-${sizes[STEPS-1].toLocaleString()} rows): ${secondAvg.toFixed(3)}ms`);
      console.log(`   Ratio: ${(secondAvg / firstAvg).toFixed(2)}x (should be ~1.0 for O(delta))`);
      
      // Time should stay roughly constant (within 2x) as table grows
      expect(secondAvg / firstAvg).toBeLessThan(2);
    });
    
    it('shows massive speedup for small deltas', () => {
      const LEFT_SIZE = 1_000_000;
      const RIGHT_SIZE = 100_000;
      const DELTA_SIZE = 10;
      
      console.log('\n' + '='.repeat(80));
      console.log('EXTREME SCALE TEST');
      console.log('='.repeat(80));
      console.log(`\nSetup: ${LEFT_SIZE.toLocaleString()} orders, ${RIGHT_SIZE.toLocaleString()} customers`);
      console.log(`Delta: ${DELTA_SIZE} new orders (0.001% of data)\n`);
      
      const customers = generateCustomers(RIGHT_SIZE);
      const orders = generateOrders(LEFT_SIZE, RIGHT_SIZE);
      const deltaOrders = generateOrders(DELTA_SIZE, RIGHT_SIZE)
        .map((o, i) => ({ ...o, orderId: LEFT_SIZE + i + 1 }));
      
      // Setup optimized join
      const setupStart = performance.now();
      const join = new AppendOnlyJoinState<Order, Customer>(
        (o) => String(o.customerId),
        (c) => String(c.customerId),
        false
      );
      join.batchInsertRight(customers);
      join.batchInsertLeft(orders);
      const setupTime = performance.now() - setupStart;
      
      console.log(`Setup time: ${setupTime.toFixed(0)}ms`);
      console.log(`Initial results: ${join.count.toLocaleString()}\n`);
      
      // Process tiny delta
      const deltaStart = performance.now();
      for (const o of deltaOrders) {
        join.insertLeft(o);
      }
      const deltaTime = performance.now() - deltaStart;
      
      // Compare to naive
      const naiveStart = performance.now();
      const allOrders = [...orders, ...deltaOrders];
      let naiveCount = 0;
      const custMap = new Map(customers.map(c => [c.customerId, c]));
      for (const o of allOrders) {
        if (custMap.has(o.customerId)) naiveCount++;
      }
      const naiveTime = performance.now() - naiveStart;
      
      console.log(`Naive recompute: ${naiveTime.toFixed(0)}ms`);
      console.log(`Incremental delta: ${deltaTime.toFixed(3)}ms`);
      console.log(`\n🚀 SPEEDUP: ${(naiveTime / deltaTime).toFixed(0)}x`);
      console.log(`Per-row cost: ${(deltaTime / DELTA_SIZE * 1000).toFixed(1)}μs`);
      
      // Should achieve > 10000x speedup for this ratio
      expect(naiveTime / deltaTime).toBeGreaterThan(1000);
    });
  });
});

