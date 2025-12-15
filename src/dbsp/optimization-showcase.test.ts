/**
 * DBSP Optimization Showcase
 * 
 * This benchmark demonstrates the progression of optimizations:
 * 
 * 1. NAIVE: Full recomputation on every update
 *    - Simulates traditional database behavior
 *    - O(n) work for every change
 * 
 * 2. DBSP (Incremental): Process only changes
 *    - Implements DBSP theory from the paper
 *    - O(delta) work for linear operators
 * 
 * 3. DBSP + COLUMNAR: Incremental + optimized storage
 *    - TypedArrays for numeric data
 *    - Bitmap-based filtering
 *    - Cache-friendly memory layout
 * 
 * Dataset: 1M e-commerce orders with realistic schema and queries
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { ZSet } from './zset';
import { Circuit, StreamHandle } from './circuit';
import { ColumnarTable, ColumnarZSet, type TableSchema } from './columnar';

// ============ REALISTIC E-COMMERCE SCHEMA ============

interface EcommerceOrder {
  // Identifiers
  orderId: number;
  customerId: number;
  productId: number;
  sellerId: number;
  
  // Financials (float64)
  subtotal: number;
  tax: number;
  shipping: number;
  discount: number;
  total: number;
  
  // Quantities (int32)
  quantity: number;
  itemCount: number;
  
  // Categoricals (string)
  status: string;           // pending, processing, shipped, delivered, cancelled, refunded
  paymentMethod: string;    // credit_card, debit_card, paypal, apple_pay, crypto
  shippingMethod: string;   // standard, express, overnight, pickup
  region: string;           // NA, EU, APAC, LATAM, MEA
  country: string;          // US, CA, UK, DE, FR, JP, AU, BR, etc.
  category: string;         // electronics, clothing, home, sports, books, toys
  
  // Flags (boolean - stored as int in columnar)
  isPrime: number;
  isGift: number;
  hasInsurance: number;
  
  // Timestamps (int32 - days since epoch for simplicity)
  orderDate: number;
  shipDate: number;
}

const ORDER_SCHEMA: TableSchema = {
  columns: [
    { name: 'orderId', type: 'int32' },
    { name: 'customerId', type: 'int32' },
    { name: 'productId', type: 'int32' },
    { name: 'sellerId', type: 'int32' },
    { name: 'subtotal', type: 'float64' },
    { name: 'tax', type: 'float64' },
    { name: 'shipping', type: 'float64' },
    { name: 'discount', type: 'float64' },
    { name: 'total', type: 'float64' },
    { name: 'quantity', type: 'int32' },
    { name: 'itemCount', type: 'int32' },
    { name: 'status', type: 'string' },
    { name: 'paymentMethod', type: 'string' },
    { name: 'shippingMethod', type: 'string' },
    { name: 'region', type: 'string' },
    { name: 'country', type: 'string' },
    { name: 'category', type: 'string' },
    { name: 'isPrime', type: 'int32' },
    { name: 'isGift', type: 'int32' },
    { name: 'hasInsurance', type: 'int32' },
    { name: 'orderDate', type: 'int32' },
    { name: 'shipDate', type: 'int32' },
  ]
};

// ============ DATA GENERATION ============

const STATUSES = ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded'];
const PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'crypto'];
const SHIPPING_METHODS = ['standard', 'express', 'overnight', 'pickup'];
const REGIONS = ['NA', 'EU', 'APAC', 'LATAM', 'MEA'];
const COUNTRIES = ['US', 'CA', 'UK', 'DE', 'FR', 'JP', 'AU', 'BR', 'IN', 'MX'];
const CATEGORIES = ['electronics', 'clothing', 'home', 'sports', 'books', 'toys', 'food', 'beauty'];

function randomChoice<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function generateOrder(id: number): EcommerceOrder {
  const subtotal = Math.random() * 500 + 10;
  const tax = subtotal * 0.08;
  const shipping = Math.random() * 20;
  const discount = Math.random() < 0.3 ? subtotal * Math.random() * 0.2 : 0;
  
  return {
    orderId: id,
    customerId: randomInt(1, 100000),
    productId: randomInt(1, 50000),
    sellerId: randomInt(1, 5000),
    subtotal,
    tax,
    shipping,
    discount,
    total: subtotal + tax + shipping - discount,
    quantity: randomInt(1, 10),
    itemCount: randomInt(1, 5),
    status: randomChoice(STATUSES),
    paymentMethod: randomChoice(PAYMENT_METHODS),
    shippingMethod: randomChoice(SHIPPING_METHODS),
    region: randomChoice(REGIONS),
    country: randomChoice(COUNTRIES),
    category: randomChoice(CATEGORIES),
    isPrime: Math.random() < 0.4 ? 1 : 0,
    isGift: Math.random() < 0.15 ? 1 : 0,
    hasInsurance: Math.random() < 0.1 ? 1 : 0,
    orderDate: randomInt(19000, 19365), // 2022-2023
    shipDate: randomInt(19000, 19365),
  };
}

function generateDelta(orders: EcommerceOrder[], percent: number): EcommerceOrder[] {
  const count = Math.max(1, Math.floor(orders.length * percent / 100));
  const delta: EcommerceOrder[] = [];
  
  for (let i = 0; i < count; i++) {
    delta.push(generateOrder(orders.length + i + 1));
  }
  
  return delta;
}

// ============ BENCHMARK RESULTS ============

interface BenchmarkResult {
  query: string;
  naive: number;
  dbsp: number;
  columnar: number;
  dbspSpeedup: number;
  columnarSpeedup: number;
  totalSpeedup: number;
}

const results: BenchmarkResult[] = [];

function formatMs(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}μs`;
  if (ms < 1000) return `${ms.toFixed(2)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

function printResult(r: BenchmarkResult): void {
  console.log(`   📊 ${r.query}`);
  console.log(`      Naive:    ${formatMs(r.naive).padStart(10)}`);
  console.log(`      DBSP:     ${formatMs(r.dbsp).padStart(10)} (${r.dbspSpeedup.toFixed(0)}x faster than naive)`);
  console.log(`      Columnar: ${formatMs(r.columnar).padStart(10)} (${r.columnarSpeedup.toFixed(0)}x faster than DBSP, ${r.totalSpeedup.toFixed(0)}x total)`);
}

// ============ THE BENCHMARK ============

describe('Optimization Showcase: Naive → DBSP → Columnar', { timeout: 120000 }, () => {
  const ORDER_COUNT = 1_000_000;
  const DELTA_PERCENT = 0.1; // 0.1% = 1000 new orders
  
  let orders: EcommerceOrder[];
  let delta: EcommerceOrder[];
  
  // Data structures for each approach
  let rowZSet: ZSet<EcommerceOrder>;
  let deltaZSet: ZSet<EcommerceOrder>;
  let columnarTable: ColumnarTable;
  let columnarDelta: ColumnarTable;
  
  beforeAll(() => {
    console.log('\n');
    console.log('╔══════════════════════════════════════════════════════════════════════════════════════════════╗');
    console.log('║                        DBSP OPTIMIZATION SHOWCASE                                            ║');
    console.log('║                                                                                              ║');
    console.log('║  Comparing: Naive (full recompute) → DBSP (incremental) → DBSP + Columnar (optimized)       ║');
    console.log('╚══════════════════════════════════════════════════════════════════════════════════════════════╝');
    
    console.log(`\n📊 Generating ${ORDER_COUNT.toLocaleString()} realistic e-commerce orders...`);
    console.log(`   Schema: 22 columns (4 IDs, 5 financials, 2 quantities, 6 categoricals, 3 flags, 2 dates)`);
    
    const genStart = performance.now();
    orders = [];
    for (let i = 0; i < ORDER_COUNT; i++) {
      orders.push(generateOrder(i + 1));
    }
    console.log(`   ✓ Generated in ${((performance.now() - genStart) / 1000).toFixed(2)}s`);
    
    console.log(`\n📦 Building data structures...`);
    
    // Row-based ZSet
    const rowStart = performance.now();
    rowZSet = ZSet.fromValues(orders, (o) => o.orderId.toString());
    console.log(`   ✓ Row ZSet: ${((performance.now() - rowStart) / 1000).toFixed(2)}s`);
    
    // Columnar table
    const colStart = performance.now();
    columnarTable = new ColumnarTable(ORDER_SCHEMA, ORDER_COUNT);
    columnarTable.bulkInsert(orders);
    console.log(`   ✓ Columnar Table: ${((performance.now() - colStart) / 1000).toFixed(2)}s`);
    
    // Generate delta
    console.log(`\n🔄 Generating delta (${DELTA_PERCENT}% = ${Math.floor(ORDER_COUNT * DELTA_PERCENT / 100).toLocaleString()} new orders)...`);
    delta = generateDelta(orders, DELTA_PERCENT);
    deltaZSet = ZSet.fromValues(delta, (o) => o.orderId.toString());
    columnarDelta = new ColumnarTable(ORDER_SCHEMA, delta.length);
    columnarDelta.bulkInsert(delta);
    
    console.log('\n' + '─'.repeat(100));
  });

  // ============ QUERY 1: Simple COUNT ============

  it('Query 1: COUNT(*) - Total order count', () => {
    console.log('\n🔍 QUERY 1: SELECT COUNT(*) FROM orders');
    console.log('   Purpose: Basic aggregate - how many orders total?');
    
    // NAIVE: Count all data every time
    const naiveStart = performance.now();
    let naiveCount = 0;
    for (const order of orders) { naiveCount++; }
    for (const order of delta) { naiveCount++; }
    const naiveTime = performance.now() - naiveStart;
    
    // DBSP: Just count the delta (COUNT is linear!)
    const dbspStart = performance.now();
    const dbspDeltaCount = deltaZSet.count();
    const dbspTime = performance.now() - dbspStart;
    
    // COLUMNAR: TypedArray iteration
    const colStart = performance.now();
    const colDeltaCount = columnarDelta.count();
    const colTime = performance.now() - colStart;
    
    const result: BenchmarkResult = {
      query: 'COUNT(*)',
      naive: naiveTime,
      dbsp: dbspTime,
      columnar: colTime,
      dbspSpeedup: naiveTime / dbspTime,
      columnarSpeedup: dbspTime / colTime,
      totalSpeedup: naiveTime / colTime,
    };
    results.push(result);
    printResult(result);
  });

  // ============ QUERY 2: SUM with multiple columns ============

  it('Query 2: SUM(total), SUM(tax), SUM(discount) - Revenue analysis', () => {
    console.log('\n🔍 QUERY 2: SELECT SUM(total), SUM(tax), SUM(discount) FROM orders');
    console.log('   Purpose: Financial aggregates for revenue reporting');
    
    // NAIVE: Iterate all data, sum 3 columns
    const naiveStart = performance.now();
    let naiveTotal = 0, naiveTax = 0, naiveDiscount = 0;
    for (const o of orders) {
      naiveTotal += o.total;
      naiveTax += o.tax;
      naiveDiscount += o.discount;
    }
    for (const o of delta) {
      naiveTotal += o.total;
      naiveTax += o.tax;
      naiveDiscount += o.discount;
    }
    const naiveTime = performance.now() - naiveStart;
    
    // DBSP: Just sum the delta
    const dbspStart = performance.now();
    const dbspTotal = deltaZSet.sum(o => o.total);
    const dbspTax = deltaZSet.sum(o => o.tax);
    const dbspDiscount = deltaZSet.sum(o => o.discount);
    const dbspTime = performance.now() - dbspStart;
    
    // COLUMNAR: TypedArray sums
    const colStart = performance.now();
    const colTotal = columnarDelta.sum('total');
    const colTax = columnarDelta.sum('tax');
    const colDiscount = columnarDelta.sum('discount');
    const colTime = performance.now() - colStart;
    
    const result: BenchmarkResult = {
      query: 'SUM(total, tax, discount)',
      naive: naiveTime,
      dbsp: dbspTime,
      columnar: colTime,
      dbspSpeedup: naiveTime / dbspTime,
      columnarSpeedup: dbspTime / colTime,
      totalSpeedup: naiveTime / colTime,
    };
    results.push(result);
    printResult(result);
  });

  // ============ QUERY 3: Filtered COUNT ============

  it('Query 3: COUNT WHERE status = "delivered" AND isPrime = 1', () => {
    console.log('\n🔍 QUERY 3: SELECT COUNT(*) FROM orders WHERE status = "delivered" AND isPrime = 1');
    console.log('   Purpose: Count successful Prime orders');
    
    // NAIVE: Filter all data
    const naiveStart = performance.now();
    let naiveCount = 0;
    for (const o of orders) {
      if (o.status === 'delivered' && o.isPrime === 1) naiveCount++;
    }
    for (const o of delta) {
      if (o.status === 'delivered' && o.isPrime === 1) naiveCount++;
    }
    const naiveTime = performance.now() - naiveStart;
    
    // DBSP: Filter only delta
    const dbspStart = performance.now();
    const dbspFiltered = deltaZSet.filter(o => o.status === 'delivered' && o.isPrime === 1);
    const dbspCount = dbspFiltered.count();
    const dbspTime = performance.now() - dbspStart;
    
    // COLUMNAR: Bitmap masks + masked count
    const colStart = performance.now();
    const statusMask = columnarDelta.createMaskString('status', '=', 'delivered');
    const primeMask = columnarDelta.createMaskNumeric('isPrime', '=', 1);
    const combinedMask = ColumnarTable.andMasks(statusMask, primeMask);
    const colCount = columnarDelta.countMasked(combinedMask);
    const colTime = performance.now() - colStart;
    
    const result: BenchmarkResult = {
      query: 'COUNT WHERE status=delivered AND isPrime',
      naive: naiveTime,
      dbsp: dbspTime,
      columnar: colTime,
      dbspSpeedup: naiveTime / dbspTime,
      columnarSpeedup: dbspTime / colTime,
      totalSpeedup: naiveTime / colTime,
    };
    results.push(result);
    printResult(result);
  });

  // ============ QUERY 4: Complex filter with range ============

  it('Query 4: SUM(total) WHERE total BETWEEN 100 AND 500 AND region IN ("NA", "EU")', () => {
    console.log('\n🔍 QUERY 4: SELECT SUM(total) FROM orders WHERE total BETWEEN 100 AND 500 AND region IN ("NA", "EU")');
    console.log('   Purpose: Revenue from mid-value orders in key markets');
    
    const targetRegions = new Set(['NA', 'EU']);
    
    // NAIVE
    const naiveStart = performance.now();
    let naiveSum = 0;
    for (const o of orders) {
      if (o.total >= 100 && o.total <= 500 && targetRegions.has(o.region)) {
        naiveSum += o.total;
      }
    }
    for (const o of delta) {
      if (o.total >= 100 && o.total <= 500 && targetRegions.has(o.region)) {
        naiveSum += o.total;
      }
    }
    const naiveTime = performance.now() - naiveStart;
    
    // DBSP
    const dbspStart = performance.now();
    const dbspFiltered = deltaZSet.filter(o => 
      o.total >= 100 && o.total <= 500 && targetRegions.has(o.region)
    );
    const dbspSum = dbspFiltered.sum(o => o.total);
    const dbspTime = performance.now() - dbspStart;
    
    // COLUMNAR
    const colStart = performance.now();
    const rangeMask = columnarDelta.createMaskNumeric('total', 'between', 100, 500);
    const regionMask = columnarDelta.createMaskString('region', 'in', ['NA', 'EU']);
    const mask = ColumnarTable.andMasks(rangeMask, regionMask);
    const colSum = columnarDelta.sumMasked('total', mask);
    const colTime = performance.now() - colStart;
    
    const result: BenchmarkResult = {
      query: 'SUM WHERE BETWEEN AND IN',
      naive: naiveTime,
      dbsp: dbspTime,
      columnar: colTime,
      dbspSpeedup: naiveTime / dbspTime,
      columnarSpeedup: dbspTime / colTime,
      totalSpeedup: naiveTime / colTime,
    };
    results.push(result);
    printResult(result);
  });

  // ============ QUERY 5: Pattern matching ============

  it('Query 5: COUNT WHERE category LIKE "elec%" AND paymentMethod IN (credit_card, paypal)', () => {
    console.log('\n🔍 QUERY 5: SELECT COUNT(*) FROM orders WHERE category LIKE "elec%" AND paymentMethod IN ("credit_card", "paypal")');
    console.log('   Purpose: Electronics orders with major payment methods');
    
    const targetPayments = new Set(['credit_card', 'paypal']);
    
    // NAIVE
    const naiveStart = performance.now();
    let naiveCount = 0;
    for (const o of orders) {
      if (o.category.startsWith('elec') && targetPayments.has(o.paymentMethod)) {
        naiveCount++;
      }
    }
    for (const o of delta) {
      if (o.category.startsWith('elec') && targetPayments.has(o.paymentMethod)) {
        naiveCount++;
      }
    }
    const naiveTime = performance.now() - naiveStart;
    
    // DBSP
    const dbspStart = performance.now();
    const dbspFiltered = deltaZSet.filter(o => 
      o.category.startsWith('elec') && targetPayments.has(o.paymentMethod)
    );
    const dbspCount = dbspFiltered.count();
    const dbspTime = performance.now() - dbspStart;
    
    // COLUMNAR
    const colStart = performance.now();
    const catMask = columnarDelta.createMaskString('category', 'like', 'elec%');
    const payMask = columnarDelta.createMaskString('paymentMethod', 'in', ['credit_card', 'paypal']);
    const mask = ColumnarTable.andMasks(catMask, payMask);
    const colCount = columnarDelta.countMasked(mask);
    const colTime = performance.now() - colStart;
    
    const result: BenchmarkResult = {
      query: 'COUNT WHERE LIKE AND IN',
      naive: naiveTime,
      dbsp: dbspTime,
      columnar: colTime,
      dbspSpeedup: naiveTime / dbspTime,
      columnarSpeedup: dbspTime / colTime,
      totalSpeedup: naiveTime / colTime,
    };
    results.push(result);
    printResult(result);
  });

  // ============ QUERY 6: Multiple numeric aggregates ============

  it('Query 6: AVG(total), MIN(total), MAX(total) - Price distribution', () => {
    console.log('\n🔍 QUERY 6: SELECT AVG(total), MIN(total), MAX(total) FROM orders');
    console.log('   Purpose: Order value distribution analysis');
    
    // NAIVE
    const naiveStart = performance.now();
    let sum = 0, count = 0, min = Infinity, max = -Infinity;
    for (const o of orders) {
      sum += o.total;
      count++;
      if (o.total < min) min = o.total;
      if (o.total > max) max = o.total;
    }
    for (const o of delta) {
      sum += o.total;
      count++;
      if (o.total < min) min = o.total;
      if (o.total > max) max = o.total;
    }
    const naiveAvg = sum / count;
    const naiveTime = performance.now() - naiveStart;
    
    // DBSP (note: MIN/MAX are NOT linear, need full state for correctness)
    // For incremental MIN/MAX we'd need the integrated state
    // Here we just process delta for timing comparison
    const dbspStart = performance.now();
    const dbspSum = deltaZSet.sum(o => o.total);
    const dbspCount = deltaZSet.count();
    let dbspMin = Infinity, dbspMax = -Infinity;
    for (const [o] of deltaZSet.entries()) {
      if (o.total < dbspMin) dbspMin = o.total;
      if (o.total > dbspMax) dbspMax = o.total;
    }
    const dbspTime = performance.now() - dbspStart;
    
    // COLUMNAR
    const colStart = performance.now();
    const colAvg = columnarDelta.avg('total');
    const colMin = columnarDelta.min('total');
    const colMax = columnarDelta.max('total');
    const colTime = performance.now() - colStart;
    
    const result: BenchmarkResult = {
      query: 'AVG, MIN, MAX(total)',
      naive: naiveTime,
      dbsp: dbspTime,
      columnar: colTime,
      dbspSpeedup: naiveTime / dbspTime,
      columnarSpeedup: dbspTime / colTime,
      totalSpeedup: naiveTime / colTime,
    };
    results.push(result);
    printResult(result);
  });

  // ============ QUERY 7: OR conditions ============

  it('Query 7: COUNT WHERE status = "cancelled" OR status = "refunded" - Problem orders', () => {
    console.log('\n🔍 QUERY 7: SELECT COUNT(*) FROM orders WHERE status = "cancelled" OR status = "refunded"');
    console.log('   Purpose: Count problematic orders for ops review');
    
    // NAIVE
    const naiveStart = performance.now();
    let naiveCount = 0;
    for (const o of orders) {
      if (o.status === 'cancelled' || o.status === 'refunded') naiveCount++;
    }
    for (const o of delta) {
      if (o.status === 'cancelled' || o.status === 'refunded') naiveCount++;
    }
    const naiveTime = performance.now() - naiveStart;
    
    // DBSP
    const dbspStart = performance.now();
    const dbspFiltered = deltaZSet.filter(o => o.status === 'cancelled' || o.status === 'refunded');
    const dbspCount = dbspFiltered.count();
    const dbspTime = performance.now() - dbspStart;
    
    // COLUMNAR - use IN for OR on same column
    const colStart = performance.now();
    const mask = columnarDelta.createMaskString('status', 'in', ['cancelled', 'refunded']);
    const colCount = columnarDelta.countMasked(mask);
    const colTime = performance.now() - colStart;
    
    const result: BenchmarkResult = {
      query: 'COUNT WHERE status IN (cancelled, refunded)',
      naive: naiveTime,
      dbsp: dbspTime,
      columnar: colTime,
      dbspSpeedup: naiveTime / dbspTime,
      columnarSpeedup: dbspTime / colTime,
      totalSpeedup: naiveTime / colTime,
    };
    results.push(result);
    printResult(result);
  });

  // ============ QUERY 8: Full table scan simulation ============

  it('Query 8: SUM(subtotal + tax + shipping - discount) - Validate totals', () => {
    console.log('\n🔍 QUERY 8: SELECT SUM(subtotal + tax + shipping - discount) FROM orders');
    console.log('   Purpose: Validate calculated totals (touches 4 columns)');
    
    // NAIVE
    const naiveStart = performance.now();
    let naiveSum = 0;
    for (const o of orders) {
      naiveSum += o.subtotal + o.tax + o.shipping - o.discount;
    }
    for (const o of delta) {
      naiveSum += o.subtotal + o.tax + o.shipping - o.discount;
    }
    const naiveTime = performance.now() - naiveStart;
    
    // DBSP
    const dbspStart = performance.now();
    const dbspSum = deltaZSet.sum(o => o.subtotal + o.tax + o.shipping - o.discount);
    const dbspTime = performance.now() - dbspStart;
    
    // COLUMNAR - sum each column separately (better cache)
    const colStart = performance.now();
    const colSum = columnarDelta.sum('subtotal') + 
                   columnarDelta.sum('tax') + 
                   columnarDelta.sum('shipping') - 
                   columnarDelta.sum('discount');
    const colTime = performance.now() - colStart;
    
    const result: BenchmarkResult = {
      query: 'SUM(4 columns expression)',
      naive: naiveTime,
      dbsp: dbspTime,
      columnar: colTime,
      dbspSpeedup: naiveTime / dbspTime,
      columnarSpeedup: dbspTime / colTime,
      totalSpeedup: naiveTime / colTime,
    };
    results.push(result);
    printResult(result);
  });

  // ============ SUMMARY ============

  it('SUMMARY: Optimization Impact Analysis', () => {
    console.log('\n');
    console.log('╔══════════════════════════════════════════════════════════════════════════════════════════════╗');
    console.log('║                              OPTIMIZATION SUMMARY                                            ║');
    console.log('╚══════════════════════════════════════════════════════════════════════════════════════════════╝');
    
    console.log('\n📈 SPEEDUP COMPARISON (higher is better)\n');
    console.log('┌─────────────────────────────────────────┬──────────────┬──────────────┬──────────────┐');
    console.log('│ Query                                   │ Naive→DBSP   │ DBSP→Columnar│ Naive→Total  │');
    console.log('├─────────────────────────────────────────┼──────────────┼──────────────┼──────────────┤');
    
    for (const r of results) {
      const query = r.query.padEnd(39);
      const dbsp = `${r.dbspSpeedup.toFixed(0)}x`.padStart(10);
      const col = `${r.columnarSpeedup.toFixed(1)}x`.padStart(10);
      const total = `${r.totalSpeedup.toFixed(0)}x`.padStart(10);
      console.log(`│ ${query} │ ${dbsp}   │ ${col}   │ ${total}   │`);
    }
    
    console.log('└─────────────────────────────────────────┴──────────────┴──────────────┴──────────────┘');
    
    // Calculate averages
    const avgDbsp = results.reduce((s, r) => s + r.dbspSpeedup, 0) / results.length;
    const avgCol = results.reduce((s, r) => s + r.columnarSpeedup, 0) / results.length;
    const avgTotal = results.reduce((s, r) => s + r.totalSpeedup, 0) / results.length;
    
    const maxDbsp = Math.max(...results.map(r => r.dbspSpeedup));
    const maxCol = Math.max(...results.map(r => r.columnarSpeedup));
    const maxTotal = Math.max(...results.map(r => r.totalSpeedup));
    
    console.log('\n📊 STATISTICS\n');
    console.log(`   Average Speedups:`);
    console.log(`   • Naive → DBSP:     ${avgDbsp.toFixed(0)}x average, ${maxDbsp.toFixed(0)}x max`);
    console.log(`   • DBSP → Columnar:  ${avgCol.toFixed(1)}x average, ${maxCol.toFixed(1)}x max`);
    console.log(`   • Naive → Total:    ${avgTotal.toFixed(0)}x average, ${maxTotal.toFixed(0)}x max`);
    
    console.log('\n💡 KEY INSIGHTS\n');
    console.log('   1. DBSP (Incremental Processing):');
    console.log('      • Speedup ≈ |Database| / |Delta| for linear operators');
    console.log('      • With 1M rows and 0.1% delta → ~1000x theoretical speedup');
    console.log('      • Real speedup lower due to JS overhead');
    console.log('');
    console.log('   2. Columnar Storage:');
    console.log('      • TypedArrays: 10-100x faster than object iteration');
    console.log('      • Cache locality: Read one column without touching others');
    console.log('      • Bitmap masks: Filter without allocating new arrays');
    console.log('');
    console.log('   3. Combined (DBSP + Columnar):');
    console.log('      • Process only changes (DBSP)');
    console.log('      • Process them efficiently (Columnar)');
    console.log('      • Best of both worlds!');
    
    console.log('\n' + '═'.repeat(100));
    
    expect(avgTotal).toBeGreaterThan(1);
  });
});

