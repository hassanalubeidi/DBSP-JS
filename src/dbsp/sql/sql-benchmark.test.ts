/**
 * SQL to DBSP Compiler Benchmarks
 * 
 * Benchmarks the SQL compiler against naive SQL execution with 1M rows.
 * Tests various SQL queries and measures incremental vs naive performance.
 * 
 * Key insight: Incremental computation wins when:
 * - Data size is large (1M+ rows)
 * - Delta size is small (< 1% of total)
 * - Query is complex (joins, aggregations)
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { SQLCompiler } from './sql-compiler';
import { ZSet } from '../zset';
import { 
  generateDataset, 
  generateOrderDelta,
  ordersToZSet,
  customersToZSet,
  deltaToZSet,
  type Order,
  type Customer,
  type BenchmarkDataset,
  type DeltaBatch
} from '../benchmark-data';

// ============ BENCHMARK CONFIGURATION ============

// Set to true for full 1M benchmark via env var
const FULL_BENCHMARK = process.env.FULL_BENCHMARK === 'true';

const ORDER_COUNT = FULL_BENCHMARK ? 1_000_000 : 100_000;
const CUSTOMER_COUNT = FULL_BENCHMARK ? 100_000 : 10_000;
const PRODUCT_COUNT = FULL_BENCHMARK ? 50_000 : 5_000;

const UPDATE_PERCENTAGES = FULL_BENCHMARK 
  ? [0.001, 0.01, 0.1, 0.5, 1.0, 2.0] 
  : [0.1, 1.0, 2.0];

// ============ BENCHMARK UTILITIES ============

interface SQLBenchmarkResult {
  query: string;
  dataSize: number;
  deltaSize: number;
  deltaPercent: number;
  naiveTimeMs: number;
  incrementalTimeMs: number;
  speedup: number;
  winner: 'DBSP' | 'Naive' | 'Tie';
}

const results: SQLBenchmarkResult[] = [];

function formatResult(r: SQLBenchmarkResult): string {
  const emoji = r.winner === 'DBSP' ? '🚀' : r.winner === 'Tie' ? '🔄' : '⚡';
  return `${emoji} ${r.query.padEnd(45)} | ${r.deltaPercent}% (${r.deltaSize.toLocaleString().padStart(7)} rows) | ` +
         `Naive: ${r.naiveTimeMs.toFixed(2).padStart(8)}ms | Inc: ${r.incrementalTimeMs.toFixed(2).padStart(8)}ms | ` +
         `${r.speedup.toFixed(2).padStart(6)}x ${r.winner}`;
}

// ============ BENCHMARK DATA ============

let dataset: BenchmarkDataset;
let ordersZSet: ZSet<Order>;
let customersZSet: ZSet<Customer>;

// ============ TESTS ============

describe('SQL Compiler Benchmarks', { timeout: 120000 }, () => {
  beforeAll(() => {
    console.log('\n');
    console.log('═'.repeat(120));
    console.log('                              SQL-TO-DBSP COMPILER BENCHMARK');
    console.log('═'.repeat(120));
    console.log(`Mode: ${FULL_BENCHMARK ? 'FULL (1M rows)' : 'QUICK (100K rows)'}`);
    console.log(`To run full benchmark: FULL_BENCHMARK=true npm run test:run -- src/dbsp/sql/sql-benchmark.test.ts`);
    console.log('═'.repeat(120));
    
    console.log('\n📊 Generating benchmark dataset...');
    dataset = generateDataset(ORDER_COUNT, CUSTOMER_COUNT, PRODUCT_COUNT);
    console.log(`   ✓ Generated ${dataset.orders.length.toLocaleString()} orders`);
    console.log(`   ✓ Generated ${dataset.customers.length.toLocaleString()} customers`);
    
    console.log('\n📦 Building ZSets...');
    ordersZSet = ordersToZSet(dataset.orders);
    customersZSet = customersToZSet(dataset.customers);
    console.log(`   ✓ Orders ZSet: ${ordersZSet.size().toLocaleString()} entries`);
    console.log(`   ✓ Customers ZSet: ${customersZSet.size().toLocaleString()} entries`);
    console.log('\n' + '─'.repeat(120));
  });

  describe('Filter Queries (Linear Operators)', () => {
    it('benchmarks WHERE clause with string equality', () => {
      console.log('\n📈 Filter Query: WHERE status = "pending"');
      
      const sql = `
        CREATE TABLE orders (id INT, customerId INT, productId INT, quantity INT, price DECIMAL, status VARCHAR, region VARCHAR, timestamp BIGINT);
        CREATE VIEW pending_orders AS SELECT * FROM orders WHERE status = 'pending';
      `;
      
      for (const pct of UPDATE_PERCENTAGES) {
        const compiler = new SQLCompiler();
        const { circuit, views } = compiler.compile(sql);
        
        // Setup incremental output - just track the delta, don't integrate!
        let deltaResult: ZSet<any> | null = null;
        views.pending_orders.output((zset) => {
          deltaResult = zset as ZSet<any>;
        });
        
        // Initial load (warm-up, not timed)
        circuit.step(new Map([['orders', ordersZSet]]));
        
        // Generate delta
        const delta: DeltaBatch<Order> = generateOrderDelta(dataset.orders, pct, ORDER_COUNT + 1);
        const deltaZSet = deltaToZSet(delta);
        
        // Benchmark NAIVE: Must iterate ALL data to filter
        const naiveStart = performance.now();
        const allOrders = Array.from(ordersZSet.entries()).concat(
          Array.from(deltaZSet.entries())
        );
        let naiveCount = 0;
        for (const [order] of allOrders) {
          if ((order as Order).status === 'pending') naiveCount++;
        }
        const naiveTime = performance.now() - naiveStart;
        
        // Benchmark INCREMENTAL: Only filter the delta - O(delta)!
        const incStart = performance.now();
        circuit.step(new Map([['orders', deltaZSet]]));
        const incTime = performance.now() - incStart;
        
        const speedup = naiveTime / incTime;
        const result: SQLBenchmarkResult = {
          query: 'WHERE status = "pending"',
          dataSize: ORDER_COUNT,
          deltaSize: delta.totalChanges,
          deltaPercent: pct,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup,
          winner: speedup > 1.2 ? 'DBSP' : speedup < 0.8 ? 'Naive' : 'Tie',
        };
        results.push(result);
        console.log('   ' + formatResult(result));
      }
    });

    it('benchmarks WHERE clause with numeric comparison', () => {
      console.log('\n📈 Filter Query: WHERE price > 50');
      
      const sql = `
        CREATE TABLE orders (id INT, customerId INT, productId INT, quantity INT, price DECIMAL, status VARCHAR, region VARCHAR, timestamp BIGINT);
        CREATE VIEW high_value AS SELECT * FROM orders WHERE price > 50;
      `;
      
      for (const pct of UPDATE_PERCENTAGES) {
        const compiler = new SQLCompiler();
        const { circuit, views } = compiler.compile(sql);
        
        let deltaResult: ZSet<any> | null = null;
        views.high_value.output((zset) => {
          deltaResult = zset as ZSet<any>;
        });
        
        circuit.step(new Map([['orders', ordersZSet]]));
        
        const delta: DeltaBatch<Order> = generateOrderDelta(dataset.orders, pct, ORDER_COUNT + 1);
        const deltaZSet = deltaToZSet(delta);
        
        // Naive: iterate ALL data
        const naiveStart = performance.now();
        const allOrders = Array.from(ordersZSet.entries()).concat(Array.from(deltaZSet.entries()));
        let naiveCount = 0;
        for (const [order] of allOrders) {
          if ((order as Order).price > 50) naiveCount++;
        }
        const naiveTime = performance.now() - naiveStart;
        
        // Incremental: only filter delta
        const incStart = performance.now();
        circuit.step(new Map([['orders', deltaZSet]]));
        const incTime = performance.now() - incStart;
        
        const speedup = naiveTime / incTime;
        const result: SQLBenchmarkResult = {
          query: 'WHERE price > 50',
          dataSize: ORDER_COUNT,
          deltaSize: delta.totalChanges,
          deltaPercent: pct,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup,
          winner: speedup > 1.2 ? 'DBSP' : speedup < 0.8 ? 'Naive' : 'Tie',
        };
        results.push(result);
        console.log('   ' + formatResult(result));
      }
    });

    it('benchmarks compound AND conditions', () => {
      console.log('\n📈 Filter Query: WHERE status = "pending" AND price > 50');
      
      const sql = `
        CREATE TABLE orders (id INT, customerId INT, productId INT, quantity INT, price DECIMAL, status VARCHAR, region VARCHAR, timestamp BIGINT);
        CREATE VIEW filtered AS SELECT * FROM orders WHERE status = 'pending' AND price > 50;
      `;
      
      for (const pct of UPDATE_PERCENTAGES) {
        const compiler = new SQLCompiler();
        const { circuit, views } = compiler.compile(sql);
        
        let deltaResult: ZSet<any> | null = null;
        views.filtered.output((zset) => {
          deltaResult = zset as ZSet<any>;
        });
        
        circuit.step(new Map([['orders', ordersZSet]]));
        
        const delta: DeltaBatch<Order> = generateOrderDelta(dataset.orders, pct, ORDER_COUNT + 1);
        const deltaZSet = deltaToZSet(delta);
        
        // Naive
        const naiveStart = performance.now();
        const allOrders = Array.from(ordersZSet.entries()).concat(Array.from(deltaZSet.entries()));
        let naiveCount = 0;
        for (const [order] of allOrders) {
          if ((order as Order).status === 'pending' && (order as Order).price > 50) naiveCount++;
        }
        const naiveTime = performance.now() - naiveStart;
        
        // Incremental
        const incStart = performance.now();
        circuit.step(new Map([['orders', deltaZSet]]));
        const incTime = performance.now() - incStart;
        
        const speedup = naiveTime / incTime;
        const result: SQLBenchmarkResult = {
          query: 'WHERE status="pending" AND price>50',
          dataSize: ORDER_COUNT,
          deltaSize: delta.totalChanges,
          deltaPercent: pct,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup,
          winner: speedup > 1.2 ? 'DBSP' : speedup < 0.8 ? 'Naive' : 'Tie',
        };
        results.push(result);
        console.log('   ' + formatResult(result));
      }
    });
  });

  describe('Aggregation Queries (Linear in DBSP)', () => {
    it('benchmarks COUNT(*) aggregation', () => {
      console.log('\n📈 Aggregation: COUNT(*) - LINEAR: Δ(COUNT(R)) = COUNT(ΔR)');
      
      const sql = `
        CREATE TABLE orders (id INT, customerId INT, productId INT, quantity INT, price DECIMAL, status VARCHAR, region VARCHAR, timestamp BIGINT);
        CREATE VIEW order_count AS SELECT COUNT(*) as cnt FROM orders;
      `;
      
      for (const pct of UPDATE_PERCENTAGES) {
        const compiler = new SQLCompiler();
        const { circuit, views } = compiler.compile(sql);
        
        // Track delta count directly (linear operator!)
        let deltaCount = 0;
        views.order_count.output((delta: number) => {
          deltaCount = delta;
        });
        
        circuit.step(new Map([['orders', ordersZSet]]));
        
        const delta: DeltaBatch<Order> = generateOrderDelta(dataset.orders, pct, ORDER_COUNT + 1);
        const deltaZSet = deltaToZSet(delta);
        
        // Naive - must iterate ALL data to count
        const naiveStart = performance.now();
        let naiveCount = 0;
        for (const [_, weight] of ordersZSet.entries()) {
          naiveCount += weight;
        }
        for (const [_, weight] of deltaZSet.entries()) {
          naiveCount += weight;
        }
        const naiveTime = performance.now() - naiveStart;
        
        // Incremental - just count the delta weights - O(delta)!
        const incStart = performance.now();
        circuit.step(new Map([['orders', deltaZSet]]));
        const incTime = performance.now() - incStart;
        
        const speedup = naiveTime / incTime;
        const result: SQLBenchmarkResult = {
          query: 'COUNT(*)',
          dataSize: ORDER_COUNT,
          deltaSize: delta.totalChanges,
          deltaPercent: pct,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup,
          winner: speedup > 1.2 ? 'DBSP' : speedup < 0.8 ? 'Naive' : 'Tie',
        };
        results.push(result);
        console.log('   ' + formatResult(result));
      }
    });

    it('benchmarks SUM(column) aggregation', () => {
      console.log('\n📈 Aggregation: SUM(price) - LINEAR: Δ(SUM(R)) = SUM(ΔR)');
      
      const sql = `
        CREATE TABLE orders (id INT, customerId INT, productId INT, quantity INT, price DECIMAL, status VARCHAR, region VARCHAR, timestamp BIGINT);
        CREATE VIEW total_value AS SELECT SUM(price) as total FROM orders;
      `;
      
      for (const pct of UPDATE_PERCENTAGES) {
        const compiler = new SQLCompiler();
        const { circuit, views } = compiler.compile(sql);
        
        let deltaSum = 0;
        views.total_value.output((delta: number) => {
          deltaSum = delta;
        });
        
        circuit.step(new Map([['orders', ordersZSet]]));
        
        const delta: DeltaBatch<Order> = generateOrderDelta(dataset.orders, pct, ORDER_COUNT + 1);
        const deltaZSet = deltaToZSet(delta);
        
        // Naive - must iterate ALL data to sum
        const naiveStart = performance.now();
        let naiveSum = 0;
        for (const [order, weight] of ordersZSet.entries()) {
          naiveSum += (order as Order).price * weight;
        }
        for (const [order, weight] of deltaZSet.entries()) {
          naiveSum += (order as Order).price * weight;
        }
        const naiveTime = performance.now() - naiveStart;
        
        // Incremental - just sum the delta - O(delta)!
        const incStart = performance.now();
        circuit.step(new Map([['orders', deltaZSet]]));
        const incTime = performance.now() - incStart;
        
        const speedup = naiveTime / incTime;
        const result: SQLBenchmarkResult = {
          query: 'SUM(price)',
          dataSize: ORDER_COUNT,
          deltaSize: delta.totalChanges,
          deltaPercent: pct,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup,
          winner: speedup > 1.2 ? 'DBSP' : speedup < 0.8 ? 'Naive' : 'Tie',
        };
        results.push(result);
        console.log('   ' + formatResult(result));
      }
    });
  });

  describe('Pattern Matching (Linear Operators)', () => {
    it('benchmarks LIKE pattern matching', () => {
      console.log('\n📈 Pattern: WHERE region LIKE "NA%"');
      
      const sql = `
        CREATE TABLE orders (id INT, customerId INT, productId INT, quantity INT, price DECIMAL, status VARCHAR, region VARCHAR, timestamp BIGINT);
        CREATE VIEW na_orders AS SELECT * FROM orders WHERE region LIKE 'NA%';
      `;
      
      for (const pct of UPDATE_PERCENTAGES) {
        const compiler = new SQLCompiler();
        const { circuit, views } = compiler.compile(sql);
        
        let deltaResult: ZSet<any> | null = null;
        views.na_orders.output((zset) => {
          deltaResult = zset as ZSet<any>;
        });
        
        circuit.step(new Map([['orders', ordersZSet]]));
        
        const delta: DeltaBatch<Order> = generateOrderDelta(dataset.orders, pct, ORDER_COUNT + 1);
        const deltaZSet = deltaToZSet(delta);
        
        // Naive
        const naiveStart = performance.now();
        const allOrders = Array.from(ordersZSet.entries()).concat(Array.from(deltaZSet.entries()));
        let naiveCount = 0;
        for (const [order] of allOrders) {
          if ((order as Order).region.startsWith('NA')) naiveCount++;
        }
        const naiveTime = performance.now() - naiveStart;
        
        // Incremental
        const incStart = performance.now();
        circuit.step(new Map([['orders', deltaZSet]]));
        const incTime = performance.now() - incStart;
        
        const speedup = naiveTime / incTime;
        const result: SQLBenchmarkResult = {
          query: 'WHERE region LIKE "NA%"',
          dataSize: ORDER_COUNT,
          deltaSize: delta.totalChanges,
          deltaPercent: pct,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup,
          winner: speedup > 1.2 ? 'DBSP' : speedup < 0.8 ? 'Naive' : 'Tie',
        };
        results.push(result);
        console.log('   ' + formatResult(result));
      }
    });

    it('benchmarks IN list query', () => {
      console.log('\n📈 Pattern: WHERE status IN ("pending", "processing")');
      
      const sql = `
        CREATE TABLE orders (id INT, customerId INT, productId INT, quantity INT, price DECIMAL, status VARCHAR, region VARCHAR, timestamp BIGINT);
        CREATE VIEW active AS SELECT * FROM orders WHERE status IN ('pending', 'processing');
      `;
      
      for (const pct of UPDATE_PERCENTAGES) {
        const compiler = new SQLCompiler();
        const { circuit, views } = compiler.compile(sql);
        
        let deltaResult: ZSet<any> | null = null;
        views.active.output((zset) => {
          deltaResult = zset as ZSet<any>;
        });
        
        circuit.step(new Map([['orders', ordersZSet]]));
        
        const delta: DeltaBatch<Order> = generateOrderDelta(dataset.orders, pct, ORDER_COUNT + 1);
        const deltaZSet = deltaToZSet(delta);
        
        const activeStatuses = new Set(['pending', 'processing']);
        
        // Naive
        const naiveStart = performance.now();
        const allOrders = Array.from(ordersZSet.entries()).concat(Array.from(deltaZSet.entries()));
        let naiveCount = 0;
        for (const [order] of allOrders) {
          if (activeStatuses.has((order as Order).status)) naiveCount++;
        }
        const naiveTime = performance.now() - naiveStart;
        
        // Incremental
        const incStart = performance.now();
        circuit.step(new Map([['orders', deltaZSet]]));
        const incTime = performance.now() - incStart;
        
        const speedup = naiveTime / incTime;
        const result: SQLBenchmarkResult = {
          query: 'WHERE status IN ("pending","processing")',
          dataSize: ORDER_COUNT,
          deltaSize: delta.totalChanges,
          deltaPercent: pct,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup,
          winner: speedup > 1.2 ? 'DBSP' : speedup < 0.8 ? 'Naive' : 'Tie',
        };
        results.push(result);
        console.log('   ' + formatResult(result));
      }
    });

    it('benchmarks BETWEEN range query', () => {
      console.log('\n📈 Pattern: WHERE price BETWEEN 25 AND 75');
      
      const sql = `
        CREATE TABLE orders (id INT, customerId INT, productId INT, quantity INT, price DECIMAL, status VARCHAR, region VARCHAR, timestamp BIGINT);
        CREATE VIEW mid_price AS SELECT * FROM orders WHERE price BETWEEN 25 AND 75;
      `;
      
      for (const pct of UPDATE_PERCENTAGES) {
        const compiler = new SQLCompiler();
        const { circuit, views } = compiler.compile(sql);
        
        let deltaResult: ZSet<any> | null = null;
        views.mid_price.output((zset) => {
          deltaResult = zset as ZSet<any>;
        });
        
        circuit.step(new Map([['orders', ordersZSet]]));
        
        const delta: DeltaBatch<Order> = generateOrderDelta(dataset.orders, pct, ORDER_COUNT + 1);
        const deltaZSet = deltaToZSet(delta);
        
        // Naive
        const naiveStart = performance.now();
        const allOrders = Array.from(ordersZSet.entries()).concat(Array.from(deltaZSet.entries()));
        let naiveCount = 0;
        for (const [order] of allOrders) {
          const p = (order as Order).price;
          if (p >= 25 && p <= 75) naiveCount++;
        }
        const naiveTime = performance.now() - naiveStart;
        
        // Incremental
        const incStart = performance.now();
        circuit.step(new Map([['orders', deltaZSet]]));
        const incTime = performance.now() - incStart;
        
        const speedup = naiveTime / incTime;
        const result: SQLBenchmarkResult = {
          query: 'WHERE price BETWEEN 25 AND 75',
          dataSize: ORDER_COUNT,
          deltaSize: delta.totalChanges,
          deltaPercent: pct,
          naiveTimeMs: naiveTime,
          incrementalTimeMs: incTime,
          speedup,
          winner: speedup > 1.2 ? 'DBSP' : speedup < 0.8 ? 'Naive' : 'Tie',
        };
        results.push(result);
        console.log('   ' + formatResult(result));
      }
    });
  });

  describe('Summary', () => {
    it('prints benchmark summary', () => {
      console.log('\n');
      console.log('═'.repeat(120));
      console.log('                                      BENCHMARK SUMMARY');
      console.log('═'.repeat(120));
      
      const dbspWins = results.filter(r => r.winner === 'DBSP').length;
      const naiveWins = results.filter(r => r.winner === 'Naive').length;
      const ties = results.filter(r => r.winner === 'Tie').length;
      
      console.log(`\nTotal tests: ${results.length}`);
      console.log(`  🚀 DBSP wins: ${dbspWins} (${(100 * dbspWins / results.length).toFixed(1)}%)`);
      console.log(`  ⚡ Naive wins: ${naiveWins} (${(100 * naiveWins / results.length).toFixed(1)}%)`);
      console.log(`  🔄 Ties: ${ties} (${(100 * ties / results.length).toFixed(1)}%)`);
      
      // Group by query
      const byQuery = new Map<string, SQLBenchmarkResult[]>();
      for (const r of results) {
        const existing = byQuery.get(r.query) || [];
        existing.push(r);
        byQuery.set(r.query, existing);
      }
      
      console.log('\nPer-query breakdown:');
      for (const [query, queryResults] of byQuery) {
        const avgSpeedup = queryResults.reduce((s, r) => s + r.speedup, 0) / queryResults.length;
        const maxSpeedup = Math.max(...queryResults.map(r => r.speedup));
        const minSpeedup = Math.min(...queryResults.map(r => r.speedup));
        console.log(`  ${query}`);
        console.log(`    Avg speedup: ${avgSpeedup.toFixed(2)}x | Min: ${minSpeedup.toFixed(2)}x | Max: ${maxSpeedup.toFixed(2)}x`);
      }
      
      console.log('\n💡 Key Insights (DBSP Theory):');
      console.log('• Linear operators (filter, map, count, sum): Δ(Q(R)) = Q(ΔR) → O(|delta|)');
      console.log('• Speedup ratio ≈ |Database| / |Delta| for linear operators');
      console.log('• With 1M rows and 0.01% delta → theoretical speedup = 10,000x');
      console.log('• Circuit overhead reduces this in practice');
      
      console.log('\n' + '═'.repeat(120));
      
      expect(true).toBe(true);
    });
  });
});
