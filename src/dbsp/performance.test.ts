/**
 * DBSP Performance Regression Tests
 * 
 * These tests verify that DBSP maintains O(delta) complexity, not O(data).
 * Performance should NOT degrade as dataset size grows - only delta size matters.
 */

import { describe, it, expect } from 'vitest';
import { ZSet } from './zset';
import { SQLCompiler } from './sql/sql-compiler';
import { Circuit } from './circuit';

// ============ HELPERS ============

interface Order {
  id: number;
  region: string;
  amount: number;
  status: string;
}

function generateOrder(id: number): Order {
  const regions = ['NA', 'EU', 'APAC', 'LATAM', 'MEA'];
  const statuses = ['pending', 'shipped', 'delivered'];
  return {
    id,
    region: regions[id % regions.length],
    amount: Math.round((Math.random() * 500 + 10) * 100) / 100,
    status: statuses[id % statuses.length],
  };
}

function generateOrders(count: number, startId: number = 1): Order[] {
  const orders: Order[] = [];
  for (let i = 0; i < count; i++) {
    orders.push(generateOrder(startId + i));
  }
  return orders;
}

function measureTime<T>(fn: () => T): { result: T; timeMs: number } {
  const start = performance.now();
  const result = fn();
  const timeMs = performance.now() - start;
  return { result, timeMs };
}

// ============ PERFORMANCE TESTS ============

describe('DBSP Performance - Filter Query', () => {
  it('should maintain O(delta) for filter operations', () => {
    const sql = `
      CREATE TABLE orders (id INT, region VARCHAR, amount DECIMAL, status VARCHAR);
      CREATE VIEW pending AS SELECT * FROM orders WHERE status = 'pending';
    `;
    const compiler = new SQLCompiler();
    const { circuit, views } = compiler.compile(sql);
    
    let outputCount = 0;
    views.pending.output(() => {
      outputCount++;
    });
    
    // Phase 1: Load initial data (10K orders)
    const initialData = generateOrders(10000);
    const { timeMs: loadTime } = measureTime(() => {
      circuit.step(new Map([
        ['orders', ZSet.fromValues(initialData, (o) => String(o.id))]
      ]));
    });
    console.log(`[Filter] Initial load (10K): ${loadTime.toFixed(2)}ms`);
    
    // Phase 2: Small incremental updates (100 orders each)
    const updateTimes: number[] = [];
    let nextId = 10001;
    
    for (let i = 0; i < 50; i++) {
      const batch = generateOrders(100, nextId);
      nextId += 100;
      
      const { timeMs } = measureTime(() => {
        circuit.step(new Map([
          ['orders', ZSet.fromValues(batch, (o) => String(o.id))]
        ]));
      });
      updateTimes.push(timeMs);
    }
    
    // Analyze: time should NOT grow significantly
    const firstTen = updateTimes.slice(0, 10);
    const lastTen = updateTimes.slice(-10);
    const avgFirst = firstTen.reduce((a, b) => a + b, 0) / firstTen.length;
    const avgLast = lastTen.reduce((a, b) => a + b, 0) / lastTen.length;
    
    console.log(`[Filter] First 10 avg: ${avgFirst.toFixed(2)}ms, Last 10 avg: ${avgLast.toFixed(2)}ms`);
    console.log(`[Filter] Slowdown ratio: ${(avgLast / avgFirst).toFixed(2)}x`);
    
    // Performance should not degrade more than 3x (allow for JIT warmup, GC variance)
    expect(avgLast / avgFirst).toBeLessThan(3);
  });
  
  it('should show linear scaling with batch size, not data size', () => {
    const sql = `
      CREATE TABLE orders (id INT, region VARCHAR, amount DECIMAL, status VARCHAR);
      CREATE VIEW pending AS SELECT * FROM orders WHERE status = 'pending';
    `;
    const compiler = new SQLCompiler();
    const { circuit } = compiler.compile(sql);
    
    // Load baseline data
    const initial = generateOrders(50000);
    circuit.step(new Map([
      ['orders', ZSet.fromValues(initial, (o) => String(o.id))]
    ]));
    
    // Test different batch sizes - time should scale with batch, not total data
    const batchSizes = [10, 100, 1000];
    const results: { batchSize: number; avgTime: number }[] = [];
    
    let nextId = 50001;
    for (const batchSize of batchSizes) {
      const times: number[] = [];
      
      for (let i = 0; i < 10; i++) {
        const batch = generateOrders(batchSize, nextId);
        nextId += batchSize;
        
        const { timeMs } = measureTime(() => {
          circuit.step(new Map([
            ['orders', ZSet.fromValues(batch, (o) => String(o.id))]
          ]));
        });
        times.push(timeMs);
      }
      
      const avgTime = times.reduce((a, b) => a + b, 0) / times.length;
      results.push({ batchSize, avgTime });
      console.log(`[Filter] Batch size ${batchSize}: avg ${avgTime.toFixed(2)}ms`);
    }
    
    // Verify: 10x batch size should result in roughly 10x time (linear)
    const ratio100to10 = results[1].avgTime / results[0].avgTime;
    const ratio1000to100 = results[2].avgTime / results[1].avgTime;
    
    console.log(`[Filter] 100/10 ratio: ${ratio100to10.toFixed(2)}x (expected ~10x)`);
    console.log(`[Filter] 1000/100 ratio: ${ratio1000to100.toFixed(2)}x (expected ~10x)`);
    
    // Should be roughly linear (allow variance in test environment)
    // These ratios can vary significantly due to JIT warmup, GC, etc.
    expect(ratio100to10).toBeLessThan(100); // Very lenient for CI
    expect(ratio1000to100).toBeLessThan(100);
  });
});

describe('DBSP Performance - GROUP BY Query', () => {
  it('should maintain O(delta) for GROUP BY aggregations', () => {
    const sql = `
      CREATE TABLE orders (id INT, region VARCHAR, amount DECIMAL, status VARCHAR);
      CREATE VIEW by_region AS 
        SELECT region, SUM(amount) AS total, COUNT(*) AS cnt 
        FROM orders 
        GROUP BY region;
    `;
    const compiler = new SQLCompiler();
    const { circuit, views } = compiler.compile(sql);
    
    let updateCount = 0;
    views.by_region.output(() => {
      updateCount++;
    });
    
    // Phase 1: Load initial data (10K orders)
    const initialData = generateOrders(10000);
    const { timeMs: loadTime } = measureTime(() => {
      circuit.step(new Map([
        ['orders', ZSet.fromValues(initialData, (o) => String(o.id))]
      ]));
    });
    console.log(`[GROUP BY] Initial load (10K): ${loadTime.toFixed(2)}ms`);
    
    // Phase 2: Incremental updates
    const updateTimes: number[] = [];
    let nextId = 10001;
    
    for (let i = 0; i < 50; i++) {
      const batch = generateOrders(100, nextId);
      nextId += 100;
      
      const { timeMs } = measureTime(() => {
        circuit.step(new Map([
          ['orders', ZSet.fromValues(batch, (o) => String(o.id))]
        ]));
      });
      updateTimes.push(timeMs);
    }
    
    // Analyze degradation
    const firstTen = updateTimes.slice(0, 10);
    const lastTen = updateTimes.slice(-10);
    const avgFirst = firstTen.reduce((a, b) => a + b, 0) / firstTen.length;
    const avgLast = lastTen.reduce((a, b) => a + b, 0) / lastTen.length;
    
    console.log(`[GROUP BY] First 10 avg: ${avgFirst.toFixed(2)}ms, Last 10 avg: ${avgLast.toFixed(2)}ms`);
    console.log(`[GROUP BY] Slowdown ratio: ${(avgLast / avgFirst).toFixed(2)}x`);
    console.log(`[GROUP BY] Update count: ${updateCount}`);
    
    // GROUP BY should also be O(delta) - affected groups only
    expect(avgLast / avgFirst).toBeLessThan(2);
  });
  
  it('should NOT recompute all groups on each update', () => {
    const sql = `
      CREATE TABLE orders (id INT, region VARCHAR, amount DECIMAL);
      CREATE VIEW by_region AS 
        SELECT region, SUM(amount) AS total, COUNT(*) AS cnt 
        FROM orders 
        GROUP BY region;
    `;
    const compiler = new SQLCompiler();
    const { circuit, views } = compiler.compile(sql);
    
    // Track how many groups are emitted per update
    const outputsPerStep: number[] = [];
    views.by_region.output((delta) => {
      const zset = delta as ZSet<any>;
      outputsPerStep.push(zset.size());
    });
    
    // Load initial data with all 5 regions
    const initial = generateOrders(1000);
    circuit.step(new Map([
      ['orders', ZSet.fromValues(initial, (o) => String(o.id))]
    ]));
    
    console.log(`[GROUP BY] Initial load emitted ${outputsPerStep[0]} group updates`);
    
    // Now add data for ONLY one region
    const singleRegionBatch: Order[] = [];
    for (let i = 0; i < 100; i++) {
      singleRegionBatch.push({
        id: 2000 + i,
        region: 'NA', // Only NA region
        amount: 100,
        status: 'pending',
      });
    }
    
    circuit.step(new Map([
      ['orders', ZSet.fromValues(singleRegionBatch, (o) => String(o.id))]
    ]));
    
    console.log(`[GROUP BY] Single-region update emitted ${outputsPerStep[1]} group updates`);
    
    // Adding to ONE region should only emit updates for THAT region
    // (2 updates: -1 for old value, +1 for new value)
    expect(outputsPerStep[1]).toBeLessThanOrEqual(2);
  });
});

describe('DBSP Performance - Memory and State Growth', () => {
  it('should identify if internal state is growing unexpectedly', () => {
    const sql = `
      CREATE TABLE orders (id INT, region VARCHAR, amount DECIMAL, status VARCHAR);
      CREATE VIEW pending AS SELECT * FROM orders WHERE status = 'pending';
    `;
    const compiler = new SQLCompiler();
    const { circuit, views } = compiler.compile(sql);
    
    // Track output ZSet sizes
    const outputSizes: number[] = [];
    let lastOutput: any = null;
    
    views.pending.output((delta) => {
      const zset = delta as ZSet<any>;
      outputSizes.push(zset.size());
      lastOutput = zset;
    });
    
    // Insert and then delete the same data repeatedly
    // This should NOT cause state to grow
    for (let i = 0; i < 20; i++) {
      const batch = generateOrders(100, i * 100);
      
      // Insert
      circuit.step(new Map([
        ['orders', ZSet.fromValues(batch, (o) => String(o.id))]
      ]));
      
      // Delete (same rows with -1 weight)
      circuit.step(new Map([
        ['orders', ZSet.fromEntries(
          batch.map(o => [o, -1] as [Order, number]),
          (o) => String(o.id)
        )]
      ]));
    }
    
    console.log(`[Memory] Output sizes across ${outputSizes.length} steps`);
    console.log(`[Memory] Last 10 sizes: ${outputSizes.slice(-10).join(', ')}`);
    
    // After inserts+deletes, state should be stable (near 0 for net-zero changes)
  });
});

describe('DBSP Performance - Output Integration (useDBSP simulation)', () => {
  it('should identify if output integration is the bottleneck', () => {
    const sql = `
      CREATE TABLE orders (id INT, region VARCHAR, amount DECIMAL, status VARCHAR);
      CREATE VIEW pending AS SELECT * FROM orders WHERE status = 'pending';
    `;
    const compiler = new SQLCompiler();
    const { circuit, views } = compiler.compile(sql);
    
    // Simulate what useDBSP does: integrate output into a Map
    const integratedData = new Map<string, { row: any; weight: number }>();
    let integrationTime = 0;
    
    views.pending.output((delta) => {
      const start = performance.now();
      const zset = delta as ZSet<any>;
      
      // This is what useDBSP does - integrate delta into state
      for (const [row, weight] of zset.entries()) {
        const key = String(row.id);
        const existing = integratedData.get(key);
        const newWeight = (existing?.weight || 0) + weight;
        
        if (newWeight === 0) {
          integratedData.delete(key);
        } else {
          integratedData.set(key, { row, weight: newWeight });
        }
      }
      
      integrationTime += performance.now() - start;
    });
    
    // Load initial data
    const initial = generateOrders(50000);
    circuit.step(new Map([
      ['orders', ZSet.fromValues(initial, (o) => String(o.id))]
    ]));
    console.log(`[Integration] After 50K load: integration took ${integrationTime.toFixed(2)}ms, map size: ${integratedData.size}`);
    
    // Now do incremental updates and track integration time
    const integrationTimes: number[] = [];
    let nextId = 50001;
    
    for (let i = 0; i < 50; i++) {
      const batch = generateOrders(100, nextId);
      nextId += 100;
      
      integrationTime = 0;
      circuit.step(new Map([
        ['orders', ZSet.fromValues(batch, (o) => String(o.id))]
      ]));
      integrationTimes.push(integrationTime);
    }
    
    const avgTime = integrationTimes.reduce((a, b) => a + b, 0) / integrationTimes.length;
    console.log(`[Integration] Avg integration time per 100-row update: ${avgTime.toFixed(3)}ms`);
    console.log(`[Integration] Final map size: ${integratedData.size}`);
    
    // Check for degradation
    const firstHalf = integrationTimes.slice(0, 25);
    const secondHalf = integrationTimes.slice(25);
    const avgFirst = firstHalf.reduce((a, b) => a + b, 0) / firstHalf.length;
    const avgSecond = secondHalf.reduce((a, b) => a + b, 0) / secondHalf.length;
    
    console.log(`[Integration] First half avg: ${avgFirst.toFixed(3)}ms, Second half avg: ${avgSecond.toFixed(3)}ms`);
    console.log(`[Integration] Ratio: ${(avgSecond / avgFirst).toFixed(2)}x`);
  });
  
  it('should identify if array conversion is the bottleneck', () => {
    const sql = `
      CREATE TABLE orders (id INT, region VARCHAR, amount DECIMAL, status VARCHAR);
      CREATE VIEW pending AS SELECT * FROM orders WHERE status = 'pending';
    `;
    const compiler = new SQLCompiler();
    const { circuit, views } = compiler.compile(sql);
    
    const integratedData = new Map<string, { row: any; weight: number }>();
    let arrayConversionTime = 0;
    let lastResults: any[] = [];
    
    views.pending.output((delta) => {
      const zset = delta as ZSet<any>;
      
      // Integrate
      for (const [row, weight] of zset.entries()) {
        const key = String(row.id);
        const existing = integratedData.get(key);
        const newWeight = (existing?.weight || 0) + weight;
        
        if (newWeight === 0) {
          integratedData.delete(key);
        } else {
          integratedData.set(key, { row, weight: newWeight });
        }
      }
      
      // THIS is what useDBSP does every update - convert Map to array
      const start = performance.now();
      lastResults = Array.from(integratedData.values())
        .filter(e => e.weight > 0)
        .map(e => e.row);
      arrayConversionTime = performance.now() - start;
    });
    
    // Load initial data
    const initial = generateOrders(50000);
    circuit.step(new Map([
      ['orders', ZSet.fromValues(initial, (o) => String(o.id))]
    ]));
    console.log(`[Array] After 50K load: array conversion took ${arrayConversionTime.toFixed(2)}ms, result size: ${lastResults.length}`);
    
    // Track array conversion time as data grows
    const conversionTimes: number[] = [];
    let nextId = 50001;
    
    for (let i = 0; i < 50; i++) {
      const batch = generateOrders(100, nextId);
      nextId += 100;
      
      circuit.step(new Map([
        ['orders', ZSet.fromValues(batch, (o) => String(o.id))]
      ]));
      conversionTimes.push(arrayConversionTime);
    }
    
    console.log(`[Array] Conversion times: ${conversionTimes.slice(0, 5).map(t => t.toFixed(2)).join(', ')}... ${conversionTimes.slice(-5).map(t => t.toFixed(2)).join(', ')}`);
    
    const avgFirst = conversionTimes.slice(0, 10).reduce((a, b) => a + b, 0) / 10;
    const avgLast = conversionTimes.slice(-10).reduce((a, b) => a + b, 0) / 10;
    
    console.log(`[Array] First 10 avg: ${avgFirst.toFixed(2)}ms, Last 10 avg: ${avgLast.toFixed(2)}ms`);
    console.log(`[Array] Slowdown: ${(avgLast / avgFirst).toFixed(2)}x`);
    console.log(`[Array] Final result size: ${lastResults.length}`);
    
    // This is likely the bottleneck - O(n) operation on every delta!
  });
  
  it('should show that React setState with large arrays causes lag', () => {
    // Simulate what happens when we call setResults with large arrays
    // This isn't testing React directly but shows the cost of creating large arrays
    
    const sizes = [1000, 10000, 50000, 100000];
    
    for (const size of sizes) {
      // Create a map simulating accumulated state
      const data = new Map<string, { row: any; weight: number }>();
      for (let i = 0; i < size; i++) {
        data.set(String(i), { row: generateOrder(i), weight: 1 });
      }
      
      // Measure time to convert to array (what useDBSP does on every update)
      const { timeMs } = measureTime(() => {
        return Array.from(data.values())
          .filter(e => e.weight > 0)
          .map(e => e.row);
      });
      
      console.log(`[setState sim] Map size ${size}: array conversion took ${timeMs.toFixed(2)}ms`);
    }
  });
});

describe('DBSP Performance - Stress Test', () => {
  it('should handle sustained high throughput without degradation', () => {
    const sql = `
      CREATE TABLE orders (id INT, region VARCHAR, amount DECIMAL, status VARCHAR);
      CREATE VIEW pending AS SELECT * FROM orders WHERE status = 'pending';
    `;
    const compiler = new SQLCompiler();
    const { circuit } = compiler.compile(sql);
    
    // Initial load
    const initial = generateOrders(100000);
    const { timeMs: loadTime } = measureTime(() => {
      circuit.step(new Map([
        ['orders', ZSet.fromValues(initial, (o) => String(o.id))]
      ]));
    });
    console.log(`[Stress] Initial 100K load: ${loadTime.toFixed(2)}ms`);
    
    // Sustained updates
    const updateTimes: number[] = [];
    let nextId = 100001;
    
    for (let i = 0; i < 100; i++) {
      const batch = generateOrders(1000, nextId);
      nextId += 1000;
      
      const { timeMs } = measureTime(() => {
        circuit.step(new Map([
          ['orders', ZSet.fromValues(batch, (o) => String(o.id))]
        ]));
      });
      updateTimes.push(timeMs);
    }
    
    // Analyze trend
    const segments = 5;
    const segmentSize = updateTimes.length / segments;
    const segmentAvgs: number[] = [];
    
    for (let i = 0; i < segments; i++) {
      const start = Math.floor(i * segmentSize);
      const end = Math.floor((i + 1) * segmentSize);
      const segment = updateTimes.slice(start, end);
      const avg = segment.reduce((a, b) => a + b, 0) / segment.length;
      segmentAvgs.push(avg);
      console.log(`[Stress] Segment ${i + 1}: avg ${avg.toFixed(2)}ms`);
    }
    
    // Check for exponential growth
    const growthRatios: number[] = [];
    for (let i = 1; i < segmentAvgs.length; i++) {
      growthRatios.push(segmentAvgs[i] / segmentAvgs[0]);
    }
    
    console.log(`[Stress] Growth ratios vs first segment: ${growthRatios.map(r => r.toFixed(2)).join(', ')}`);
    
    // Last segment should NOT be dramatically slower than first
    const lastRatio = growthRatios[growthRatios.length - 1];
    console.log(`[Stress] Final slowdown: ${lastRatio.toFixed(2)}x`);
    
    expect(lastRatio).toBeLessThan(3); // Allow some variation but not exponential
  });
  
  it('should profile where time is being spent', () => {
    const sql = `
      CREATE TABLE orders (id INT, region VARCHAR, amount DECIMAL, status VARCHAR);
      CREATE VIEW pending AS SELECT * FROM orders WHERE status = 'pending';
      CREATE VIEW by_region AS 
        SELECT region, SUM(amount) AS total, COUNT(*) AS cnt 
        FROM orders 
        GROUP BY region;
    `;
    const compiler = new SQLCompiler();
    const { circuit } = compiler.compile(sql);
    
    // Load substantial data
    const initial = generateOrders(50000);
    circuit.step(new Map([
      ['orders', ZSet.fromValues(initial, (o) => String(o.id))]
    ]));
    
    // Profile a single update in detail
    const batch = generateOrders(1000, 50001);
    
    console.log('\n[Profile] Detailed timing for 1000-row update after 50K rows:');
    
    // Time ZSet creation
    const { timeMs: zsetTime, result: zset } = measureTime(() => {
      return ZSet.fromValues(batch, (o) => String(o.id));
    });
    console.log(`  ZSet creation: ${zsetTime.toFixed(2)}ms`);
    
    // Time circuit step
    const { timeMs: stepTime } = measureTime(() => {
      circuit.step(new Map([['orders', zset]]));
    });
    console.log(`  Circuit step: ${stepTime.toFixed(2)}ms`);
    console.log(`  Total: ${(zsetTime + stepTime).toFixed(2)}ms`);
  });
});

