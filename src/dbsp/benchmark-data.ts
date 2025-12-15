/**
 * Benchmark Data Generator
 * 
 * Generates 1M rows of fake data for benchmarking DBSP operations.
 */

import { ZSet, type Weight } from './zset';

// ============ DATA TYPES ============

export interface Order {
  id: number;
  customerId: number;
  productId: number;
  quantity: number;
  price: number;
  status: 'pending' | 'processing' | 'shipped' | 'delivered' | 'cancelled';
  region: string;
  timestamp: number;
}

export interface Customer {
  id: number;
  name: string;
  email: string;
  tier: 'bronze' | 'silver' | 'gold' | 'platinum';
  region: string;
}

export interface Product {
  id: number;
  name: string;
  category: string;
  price: number;
  inStock: boolean;
}

// ============ DATA GENERATION ============

const REGIONS = ['NA', 'EU', 'APAC', 'LATAM', 'MEA'];
const STATUSES: Order['status'][] = ['pending', 'processing', 'shipped', 'delivered', 'cancelled'];
const TIERS: Customer['tier'][] = ['bronze', 'silver', 'gold', 'platinum'];
const CATEGORIES = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Food', 'Toys', 'Auto'];

function randomChoice<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

export function generateOrder(id: number): Order {
  return {
    id,
    customerId: randomInt(1, 100000), // 100k customers
    productId: randomInt(1, 50000),   // 50k products
    quantity: randomInt(1, 10),
    price: randomInt(10, 10000) / 100, // $0.10 - $100.00
    status: randomChoice(STATUSES),
    region: randomChoice(REGIONS),
    timestamp: Date.now() - randomInt(0, 365 * 24 * 60 * 60 * 1000),
  };
}

export function generateCustomer(id: number): Customer {
  return {
    id,
    name: `Customer_${id}`,
    email: `customer${id}@example.com`,
    tier: randomChoice(TIERS),
    region: randomChoice(REGIONS),
  };
}

export function generateProduct(id: number): Product {
  return {
    id,
    name: `Product_${id}`,
    category: randomChoice(CATEGORIES),
    price: randomInt(100, 100000) / 100,
    inStock: Math.random() > 0.1, // 90% in stock
  };
}

// ============ DATASET GENERATION ============

export interface BenchmarkDataset {
  orders: Order[];
  customers: Customer[];
  products: Product[];
  metadata: {
    orderCount: number;
    customerCount: number;
    productCount: number;
    generatedAt: string;
  };
}

export function generateDataset(
  orderCount: number = 1_000_000,
  customerCount: number = 100_000,
  productCount: number = 50_000
): BenchmarkDataset {
  console.log(`Generating dataset: ${orderCount.toLocaleString()} orders...`);
  
  const orders: Order[] = [];
  for (let i = 1; i <= orderCount; i++) {
    orders.push(generateOrder(i));
  }
  
  const customers: Customer[] = [];
  for (let i = 1; i <= customerCount; i++) {
    customers.push(generateCustomer(i));
  }
  
  const products: Product[] = [];
  for (let i = 1; i <= productCount; i++) {
    products.push(generateProduct(i));
  }
  
  return {
    orders,
    customers,
    products,
    metadata: {
      orderCount,
      customerCount,
      productCount,
      generatedAt: new Date().toISOString(),
    },
  };
}

// ============ DELTA GENERATION ============

export interface DeltaBatch<T> {
  inserts: T[];
  updates: { old: T; new: T }[];
  deletes: T[];
  totalChanges: number;
  percentOfTotal: number;
}

/**
 * Generate a delta batch representing changes to the dataset.
 * @param data Current dataset
 * @param percentChange Percentage of data to change (0.01 to 2.0)
 */
export function generateOrderDelta(
  orders: Order[],
  percentChange: number,
  nextId: number
): DeltaBatch<Order> {
  const totalChanges = Math.max(1, Math.floor(orders.length * (percentChange / 100)));
  
  // Split changes: 40% inserts, 40% updates, 20% deletes
  const insertCount = Math.floor(totalChanges * 0.4);
  const updateCount = Math.floor(totalChanges * 0.4);
  const deleteCount = totalChanges - insertCount - updateCount;
  
  const inserts: Order[] = [];
  for (let i = 0; i < insertCount; i++) {
    inserts.push(generateOrder(nextId + i));
  }
  
  const updates: { old: Order; new: Order }[] = [];
  const usedIndices = new Set<number>();
  for (let i = 0; i < updateCount && usedIndices.size < orders.length; i++) {
    let idx: number;
    do {
      idx = randomInt(0, orders.length - 1);
    } while (usedIndices.has(idx));
    usedIndices.add(idx);
    
    const oldOrder = orders[idx];
    const newOrder = {
      ...oldOrder,
      status: randomChoice(STATUSES),
      quantity: randomInt(1, 10),
    };
    updates.push({ old: oldOrder, new: newOrder });
  }
  
  const deletes: Order[] = [];
  for (let i = 0; i < deleteCount && usedIndices.size < orders.length; i++) {
    let idx: number;
    do {
      idx = randomInt(0, orders.length - 1);
    } while (usedIndices.has(idx));
    usedIndices.add(idx);
    deletes.push(orders[idx]);
  }
  
  return {
    inserts,
    updates,
    deletes,
    totalChanges,
    percentOfTotal: percentChange,
  };
}

// ============ ZSET CONVERSION ============

export function ordersToZSet(orders: Order[]): ZSet<Order> {
  return ZSet.fromValues(orders, o => o.id.toString());
}

export function customersToZSet(customers: Customer[]): ZSet<Customer> {
  return ZSet.fromValues(customers, c => c.id.toString());
}

export function productsToZSet(products: Product[]): ZSet<Product> {
  return ZSet.fromValues(products, p => p.id.toString());
}

export function deltaToZSet(delta: DeltaBatch<Order>): ZSet<Order> {
  const entries: [Order, Weight][] = [];
  
  for (const order of delta.inserts) {
    entries.push([order, 1]);
  }
  
  for (const { old, new: newOrder } of delta.updates) {
    entries.push([old, -1]);
    entries.push([newOrder, 1]);
  }
  
  for (const order of delta.deletes) {
    entries.push([order, -1]);
  }
  
  return ZSet.fromEntries(entries, o => o.id.toString());
}

// ============ HASH JOIN VERIFICATION ============

/**
 * Hash-based join that explicitly builds and uses a hash table.
 * For benchmarking and verification purposes.
 */
export function hashJoin<T, U, K>(
  left: ZSet<T>,
  right: ZSet<U>,
  leftKey: (t: T) => K,
  rightKey: (u: U) => K,
  keyToString: (k: K) => string = JSON.stringify
): { result: ZSet<[T, U]>; stats: JoinStats } {
  const stats: JoinStats = {
    leftSize: left.size(),
    rightSize: right.size(),
    hashTableSize: 0,
    hashTableBuckets: 0,
    probeCount: 0,
    matchCount: 0,
    buildTimeMs: 0,
    probeTimeMs: 0,
    totalTimeMs: 0,
  };
  
  const startTotal = performance.now();
  
  // BUILD PHASE: Create hash table on right side
  const buildStart = performance.now();
  const hashTable = new Map<string, { value: U; weight: Weight }[]>();
  
  for (const [value, weight] of right.entries()) {
    const key = keyToString(rightKey(value));
    const bucket = hashTable.get(key);
    if (bucket) {
      bucket.push({ value, weight });
    } else {
      hashTable.set(key, [{ value, weight }]);
    }
  }
  
  stats.buildTimeMs = performance.now() - buildStart;
  stats.hashTableBuckets = hashTable.size;
  for (const bucket of hashTable.values()) {
    stats.hashTableSize += bucket.length;
  }
  
  // PROBE PHASE: Probe hash table with left side
  const probeStart = performance.now();
  const result = new ZSet<[T, U]>(([a, b]) => JSON.stringify([a, b]));
  
  for (const [leftValue, leftWeight] of left.entries()) {
    stats.probeCount++;
    const key = keyToString(leftKey(leftValue));
    const matches = hashTable.get(key);
    
    if (matches) {
      for (const { value: rightValue, weight: rightWeight } of matches) {
        stats.matchCount++;
        result.insert([leftValue, rightValue], leftWeight * rightWeight);
      }
    }
  }
  
  stats.probeTimeMs = performance.now() - probeStart;
  stats.totalTimeMs = performance.now() - startTotal;
  
  return { result, stats };
}

export interface JoinStats {
  leftSize: number;
  rightSize: number;
  hashTableSize: number;
  hashTableBuckets: number;
  probeCount: number;
  matchCount: number;
  buildTimeMs: number;
  probeTimeMs: number;
  totalTimeMs: number;
}

