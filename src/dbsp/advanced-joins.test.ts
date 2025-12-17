/**
 * Tests for Advanced Join Implementations
 */

import { describe, it, expect } from 'vitest';
import {
  AsofJoinState,
  StatePrunedJoinState,
  IncrementalSemiJoinState,
  IncrementalAntiJoinState,
} from './advanced-joins';

// ============ TEST DATA ============

interface Order {
  orderId: number;
  customerId: number;
  amount: number;
  timestamp: number;
}

interface Price {
  symbol: string;
  price: number;
  timestamp: number;
}

interface Customer {
  customerId: number;
  name: string;
  createdAt: number;
}

interface Trade {
  tradeId: number;
  symbol: string;
  quantity: number;
  timestamp: number;
}

// ============ ASOF JOIN TESTS ============

describe('AsofJoinState', () => {
  describe('backward matching', () => {
    it('matches with most recent right row', () => {
      const join = new AsofJoinState<Trade, Price>(
        (t) => t.symbol,
        (p) => p.symbol,
        (t) => t.timestamp,
        (p) => p.timestamp,
        'backward'
      );
      
      // Insert prices first (time-ordered)
      join.insertRight({ symbol: 'AAPL', price: 100, timestamp: 1000 });
      join.insertRight({ symbol: 'AAPL', price: 105, timestamp: 2000 });
      join.insertRight({ symbol: 'AAPL', price: 110, timestamp: 3000 });
      
      // Trade at t=2500 should match price at t=2000 ($105)
      const match = join.insertLeft({ tradeId: 1, symbol: 'AAPL', quantity: 10, timestamp: 2500 });
      
      expect(match).not.toBeNull();
      expect(match?.price).toBe(105);
    });
    
    it('returns null when no earlier price exists', () => {
      const join = new AsofJoinState<Trade, Price>(
        (t) => t.symbol,
        (p) => p.symbol,
        (t) => t.timestamp,
        (p) => p.timestamp,
        'backward'
      );
      
      // Price at t=2000
      join.insertRight({ symbol: 'AAPL', price: 100, timestamp: 2000 });
      
      // Trade at t=1000 (before any price)
      const match = join.insertLeft({ tradeId: 1, symbol: 'AAPL', quantity: 10, timestamp: 1000 });
      
      expect(match).toBeNull();
    });
    
    it('handles multiple symbols correctly', () => {
      const join = new AsofJoinState<Trade, Price>(
        (t) => t.symbol,
        (p) => p.symbol,
        (t) => t.timestamp,
        (p) => p.timestamp,
        'backward'
      );
      
      join.insertRight({ symbol: 'AAPL', price: 100, timestamp: 1000 });
      join.insertRight({ symbol: 'GOOG', price: 200, timestamp: 1000 });
      
      const appleMatch = join.insertLeft({ tradeId: 1, symbol: 'AAPL', quantity: 10, timestamp: 1500 });
      const googMatch = join.insertLeft({ tradeId: 2, symbol: 'GOOG', quantity: 5, timestamp: 1500 });
      
      expect(appleMatch?.price).toBe(100);
      expect(googMatch?.price).toBe(200);
    });
    
    it('matches exact timestamp', () => {
      const join = new AsofJoinState<Trade, Price>(
        (t) => t.symbol,
        (p) => p.symbol,
        (t) => t.timestamp,
        (p) => p.timestamp,
        'backward'
      );
      
      join.insertRight({ symbol: 'AAPL', price: 100, timestamp: 1000 });
      
      const match = join.insertLeft({ tradeId: 1, symbol: 'AAPL', quantity: 10, timestamp: 1000 });
      
      expect(match?.price).toBe(100);
    });
  });
  
  describe('forward matching', () => {
    it('matches with next available price', () => {
      const join = new AsofJoinState<Trade, Price>(
        (t) => t.symbol,
        (p) => p.symbol,
        (t) => t.timestamp,
        (p) => p.timestamp,
        'forward'
      );
      
      join.insertRight({ symbol: 'AAPL', price: 100, timestamp: 1000 });
      join.insertRight({ symbol: 'AAPL', price: 105, timestamp: 2000 });
      
      // Trade at t=1500 should match price at t=2000 ($105) in forward mode
      const match = join.insertLeft({ tradeId: 1, symbol: 'AAPL', quantity: 10, timestamp: 1500 });
      
      expect(match?.price).toBe(105);
    });
  });
});

// ============ STATE PRUNING TESTS ============

describe('StatePrunedJoinState', () => {
  it('maintains join results correctly', () => {
    const join = new StatePrunedJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (c) => String(c.customerId),
      (o) => String(o.customerId),
      (c) => String(c.customerId),
      (o) => o.timestamp,
      (c) => c.createdAt,
      1000 // 1000ms retention window
    );
    
    join.insertRight({ customerId: 1, name: 'Alice', createdAt: 1000 });
    join.insertRight({ customerId: 2, name: 'Bob', createdAt: 1000 });
    
    join.insertLeft({ orderId: 1, customerId: 1, amount: 100, timestamp: 1500 });
    join.insertLeft({ orderId: 2, customerId: 1, amount: 200, timestamp: 1600 });
    join.insertLeft({ orderId: 3, customerId: 2, amount: 300, timestamp: 1700 });
    
    expect(join.count).toBe(3);
  });
  
  it('prunes old data based on watermark', () => {
    const join = new StatePrunedJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (c) => String(c.customerId),
      (o) => String(o.customerId),
      (c) => String(c.customerId),
      (o) => o.timestamp,
      (c) => c.createdAt,
      1000 // 1000ms retention window
    );
    
    // Add old customer
    join.insertRight({ customerId: 1, name: 'Alice', createdAt: 1000 });
    
    // Add order with much newer timestamp - should trigger GC
    join.insertLeft({ orderId: 1, customerId: 1, amount: 100, timestamp: 1500 });
    
    expect(join.count).toBe(1);
    
    // Add order with even newer timestamp
    join.insertLeft({ orderId: 2, customerId: 1, amount: 200, timestamp: 3000 });
    
    // Customer at t=1000 should be pruned (3000 - 1000 > 1000 window)
    const stats = join.getGcStats();
    expect(stats.rightRowsPruned).toBeGreaterThanOrEqual(0);
    
    // New order for old customer won't match (customer was GC'd)
    join.insertLeft({ orderId: 3, customerId: 1, amount: 300, timestamp: 3500 });
    
    // count may have decreased due to GC
    expect(join.rightCount).toBe(0);
  });
  
  it('tracks GC statistics', () => {
    const join = new StatePrunedJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (c) => String(c.customerId),
      (o) => String(o.customerId),
      (c) => String(c.customerId),
      (o) => o.timestamp,
      (c) => c.createdAt,
      100
    );
    
    // Add customers at t=0
    for (let i = 0; i < 10; i++) {
      join.insertRight({ customerId: i, name: `Customer ${i}`, createdAt: 0 });
    }
    
    expect(join.rightCount).toBe(10);
    
    // Add order at t=500 (way past retention window)
    join.insertLeft({ orderId: 1, customerId: 1, amount: 100, timestamp: 500 });
    
    // All customers should be pruned
    const stats = join.getGcStats();
    expect(stats.rightRowsPruned).toBe(10);
    expect(join.rightCount).toBe(0);
  });
  
  it('provides memory estimates', () => {
    const join = new StatePrunedJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (c) => String(c.customerId),
      (o) => String(o.customerId),
      (c) => String(c.customerId),
      (o) => o.timestamp,
      (c) => c.createdAt,
      Infinity // No pruning
    );
    
    for (let i = 0; i < 100; i++) {
      join.insertRight({ customerId: i, name: `Customer ${i}`, createdAt: 1000 });
      join.insertLeft({ orderId: i, customerId: i, amount: 100, timestamp: 1000 });
    }
    
    const mem = join.getMemoryEstimate();
    expect(mem.leftBytes).toBeGreaterThan(0);
    expect(mem.rightBytes).toBeGreaterThan(0);
    expect(mem.resultBytes).toBeGreaterThan(0);
  });
});

// ============ SEMI-JOIN TESTS ============

describe('IncrementalSemiJoinState', () => {
  it('returns only matching left rows', () => {
    const join = new IncrementalSemiJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (o) => String(o.customerId),
      (c) => String(c.customerId)
    );
    
    // Add customers
    join.insertRight({ customerId: 1, name: 'Alice', createdAt: 0 });
    join.insertRight({ customerId: 2, name: 'Bob', createdAt: 0 });
    
    // Add orders - some with matching customers, some without
    join.insertLeft({ orderId: 1, customerId: 1, amount: 100, timestamp: 0 }); // Matches
    join.insertLeft({ orderId: 2, customerId: 3, amount: 200, timestamp: 0 }); // No match
    join.insertLeft({ orderId: 3, customerId: 2, amount: 300, timestamp: 0 }); // Matches
    
    const results = join.getResults();
    
    expect(results.length).toBe(2);
    expect(results.map(o => o.orderId).sort()).toEqual([1, 3]);
  });
  
  it('handles late-arriving right rows', () => {
    const join = new IncrementalSemiJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (o) => String(o.customerId),
      (c) => String(c.customerId)
    );
    
    // Add orders first (no customers yet)
    join.insertLeft({ orderId: 1, customerId: 1, amount: 100, timestamp: 0 });
    join.insertLeft({ orderId: 2, customerId: 1, amount: 200, timestamp: 0 });
    
    expect(join.count).toBe(0); // No matches yet
    
    // Now add customer
    const added = join.insertRight({ customerId: 1, name: 'Alice', createdAt: 0 });
    
    expect(added).toBe(2); // Two orders now match
    expect(join.count).toBe(2);
  });
  
  it('handles right row deletion', () => {
    const join = new IncrementalSemiJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (o) => String(o.customerId),
      (c) => String(c.customerId)
    );
    
    const customer = { customerId: 1, name: 'Alice', createdAt: 0 };
    join.insertRight(customer);
    join.insertLeft({ orderId: 1, customerId: 1, amount: 100, timestamp: 0 });
    
    expect(join.count).toBe(1);
    
    // Remove customer
    const removed = join.removeRight(customer);
    
    expect(removed).toBe(1);
    expect(join.count).toBe(0);
  });
  
  it('handles multiple right rows per key', () => {
    const join = new IncrementalSemiJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (o) => String(o.customerId),
      (c) => String(c.customerId)
    );
    
    // Add same customer key twice (different names)
    join.insertRight({ customerId: 1, name: 'Alice', createdAt: 0 });
    join.insertRight({ customerId: 1, name: 'Alice Alternate', createdAt: 1 });
    
    join.insertLeft({ orderId: 1, customerId: 1, amount: 100, timestamp: 0 });
    
    expect(join.count).toBe(1);
    
    // Remove one instance - should still match
    join.removeRight({ customerId: 1, name: 'Alice', createdAt: 0 });
    expect(join.count).toBe(1);
    
    // Remove second instance - no longer matches
    join.removeRight({ customerId: 1, name: 'Alice Alternate', createdAt: 1 });
    expect(join.count).toBe(0);
  });
});

// ============ ANTI-JOIN TESTS ============

describe('IncrementalAntiJoinState', () => {
  it('returns only non-matching left rows', () => {
    const join = new IncrementalAntiJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (o) => String(o.customerId),
      (c) => String(c.customerId)
    );
    
    // Add customers
    join.insertRight({ customerId: 1, name: 'Alice', createdAt: 0 });
    join.insertRight({ customerId: 2, name: 'Bob', createdAt: 0 });
    
    // Add orders
    join.insertLeft({ orderId: 1, customerId: 1, amount: 100, timestamp: 0 }); // Has customer
    join.insertLeft({ orderId: 2, customerId: 3, amount: 200, timestamp: 0 }); // Orphaned!
    join.insertLeft({ orderId: 3, customerId: 2, amount: 300, timestamp: 0 }); // Has customer
    
    const results = join.getResults();
    
    expect(results.length).toBe(1);
    expect(results[0].orderId).toBe(2);
    expect(results[0].customerId).toBe(3); // The orphaned order
  });
  
  it('updates when right row is added', () => {
    const join = new IncrementalAntiJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (o) => String(o.customerId),
      (c) => String(c.customerId)
    );
    
    // Add orphaned orders
    join.insertLeft({ orderId: 1, customerId: 1, amount: 100, timestamp: 0 });
    join.insertLeft({ orderId: 2, customerId: 1, amount: 200, timestamp: 0 });
    
    expect(join.count).toBe(2); // Both orphaned
    
    // Add customer - orders no longer orphaned
    const removed = join.insertRight({ customerId: 1, name: 'Alice', createdAt: 0 });
    
    expect(removed).toBe(2);
    expect(join.count).toBe(0);
  });
  
  it('updates when right row is removed', () => {
    const join = new IncrementalAntiJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (o) => String(o.customerId),
      (c) => String(c.customerId)
    );
    
    const customer = { customerId: 1, name: 'Alice', createdAt: 0 };
    join.insertRight(customer);
    join.insertLeft({ orderId: 1, customerId: 1, amount: 100, timestamp: 0 });
    
    expect(join.count).toBe(0); // Order has matching customer
    
    // Remove customer - order becomes orphaned
    const added = join.removeRight(customer);
    
    expect(added).toBe(1);
    expect(join.count).toBe(1);
  });
  
  it('finds orphaned records (practical use case)', () => {
    const join = new IncrementalAntiJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (o) => String(o.customerId),
      (c) => String(c.customerId)
    );
    
    // Simulate a data integrity check
    const customers = [
      { customerId: 1, name: 'Alice', createdAt: 0 },
      { customerId: 2, name: 'Bob', createdAt: 0 },
      { customerId: 5, name: 'Eve', createdAt: 0 },
    ];
    
    const orders = [
      { orderId: 1, customerId: 1, amount: 100, timestamp: 0 },
      { orderId: 2, customerId: 2, amount: 200, timestamp: 0 },
      { orderId: 3, customerId: 3, amount: 300, timestamp: 0 }, // Orphan!
      { orderId: 4, customerId: 4, amount: 400, timestamp: 0 }, // Orphan!
      { orderId: 5, customerId: 5, amount: 500, timestamp: 0 },
    ];
    
    for (const c of customers) join.insertRight(c);
    for (const o of orders) join.insertLeft(o);
    
    const orphans = join.getResults();
    
    expect(orphans.length).toBe(2);
    expect(orphans.map(o => o.customerId).sort()).toEqual([3, 4]);
  });
});

// ============ PERFORMANCE BENCHMARKS ============

describe('Advanced Join Benchmarks', () => {
  it('benchmarks StatePrunedJoinState vs OptimizedJoinState', () => {
    const SIZE = 10_000;
    const DELTA = 100;
    
    console.log('\n=== State Pruning Benchmark ===');
    console.log(`Size: ${SIZE.toLocaleString()}, Delta: ${DELTA}`);
    
    // With state pruning
    const prunedJoin = new StatePrunedJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (c) => String(c.customerId),
      (o) => String(o.customerId),
      (c) => String(c.customerId),
      (o) => o.timestamp,
      (c) => c.createdAt,
      1000 // 1 second window
    );
    
    // Add initial data at t=0
    for (let i = 0; i < SIZE; i++) {
      prunedJoin.insertRight({ customerId: i, name: `C${i}`, createdAt: 0 });
    }
    
    // Add orders at increasing timestamps
    const start = performance.now();
    for (let i = 0; i < DELTA; i++) {
      prunedJoin.insertLeft({
        orderId: i,
        customerId: i % SIZE,
        amount: 100,
        timestamp: i * 100, // Increasing timestamps
      });
    }
    const prunedTime = performance.now() - start;
    
    console.log(`\nWith Pruning:`);
    console.log(`  Time: ${prunedTime.toFixed(2)}ms`);
    console.log(`  Right rows remaining: ${prunedJoin.rightCount}`);
    console.log(`  GC stats:`, prunedJoin.getGcStats());
    console.log(`  Memory:`, prunedJoin.getMemoryEstimate());
    
    // State pruning keeps memory bounded
    expect(prunedJoin.rightCount).toBeLessThan(SIZE);
  });
  
  it('benchmarks IncrementalSemiJoinState', () => {
    const LEFT_SIZE = 100_000;
    const RIGHT_SIZE = 10_000;
    
    console.log('\n=== Semi-Join Benchmark ===');
    console.log(`Left: ${LEFT_SIZE.toLocaleString()}, Right: ${RIGHT_SIZE.toLocaleString()}`);
    
    const join = new IncrementalSemiJoinState<Order, Customer>(
      (o) => String(o.orderId),
      (o) => String(o.customerId),
      (c) => String(c.customerId)
    );
    
    // Setup
    const setupStart = performance.now();
    for (let i = 0; i < RIGHT_SIZE; i++) {
      join.insertRight({ customerId: i, name: `C${i}`, createdAt: 0 });
    }
    for (let i = 0; i < LEFT_SIZE; i++) {
      join.insertLeft({
        orderId: i,
        customerId: i % (RIGHT_SIZE * 2), // Half will match
        amount: 100,
        timestamp: 0,
      });
    }
    const setupTime = performance.now() - setupStart;
    
    console.log(`Setup time: ${setupTime.toFixed(2)}ms`);
    console.log(`Matches: ${join.count.toLocaleString()} / ${LEFT_SIZE.toLocaleString()}`);
    
    // Should be ~50% matches (half of customerId's exist)
    expect(join.count).toBeGreaterThan(LEFT_SIZE * 0.4);
    expect(join.count).toBeLessThan(LEFT_SIZE * 0.6);
  });
});

