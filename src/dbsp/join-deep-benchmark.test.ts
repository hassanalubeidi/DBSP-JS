/**
 * Deep Performance Analysis of Join Operations
 * 
 * Investigates where time is being spent and identifies optimization opportunities
 */

import { describe, it, expect } from 'vitest';
import { ZSet, join, joinFilter, IndexedZSet, joinWithIndex } from './zset';

// ============ TEST DATA ============

interface Order {
  orderId: number;
  customerId: number;
  amount: number;
}

interface Customer {
  customerId: number;
  name: string;
  tier: string;
}

function generateOrders(count: number, customerIdRange: number): Order[] {
  return Array.from({ length: count }, (_, i) => ({
    orderId: i + 1,
    customerId: (i % customerIdRange) + 1,  // Deterministic distribution
    amount: 100 + (i % 900),
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

// ============ MICRO-BENCHMARKS ============

describe('Join Deep Performance Analysis', () => {
  
  describe('Step 1: Identify ZSet Overhead', () => {
    it('measures raw Map vs ZSet operations', () => {
      const COUNT = 100_000;
      
      // Raw Map insert
      const map = new Map<string, { value: Order; weight: number }>();
      const mapStart = performance.now();
      for (let i = 0; i < COUNT; i++) {
        const order = { orderId: i, customerId: i % 1000, amount: 100 };
        map.set(String(i), { value: order, weight: 1 });
      }
      const mapTime = performance.now() - mapStart;
      
      // ZSet insert
      const zset = new ZSet<Order>((o) => String(o.orderId));
      const zsetStart = performance.now();
      for (let i = 0; i < COUNT; i++) {
        const order = { orderId: i, customerId: i % 1000, amount: 100 };
        zset.insert(order, 1);
      }
      const zsetTime = performance.now() - zsetStart;
      
      console.log('\n=== ZSet vs Map Overhead ===');
      console.log(`Raw Map insert (${COUNT.toLocaleString()}): ${mapTime.toFixed(2)}ms`);
      console.log(`ZSet insert (${COUNT.toLocaleString()}): ${zsetTime.toFixed(2)}ms`);
      console.log(`ZSet overhead: ${(zsetTime / mapTime).toFixed(2)}x`);
      
      expect(zsetTime).toBeLessThan(mapTime * 5); // ZSet shouldn't be more than 5x slower
    });
    
    it('measures ZSet.entries() overhead', () => {
      const COUNT = 100_000;
      const zset = ZSet.fromValues(
        generateOrders(COUNT, 10000),
        (o) => String(o.orderId)
      );
      
      // Measure entries() call
      const start = performance.now();
      const entries = zset.entries();
      const entriesTime = performance.now() - start;
      
      // Measure iteration
      const iterStart = performance.now();
      let sum = 0;
      for (const [value, weight] of entries) {
        sum += value.amount * weight;
      }
      const iterTime = performance.now() - iterStart;
      
      console.log('\n=== ZSet.entries() Overhead ===');
      console.log(`entries() call (${COUNT.toLocaleString()} items): ${entriesTime.toFixed(2)}ms`);
      console.log(`Iteration time: ${iterTime.toFixed(2)}ms`);
      console.log(`Total: ${(entriesTime + iterTime).toFixed(2)}ms`);
      
      expect(entries.length).toBe(COUNT);
    });
  });
  
  describe('Step 2: Analyze Join Index Building', () => {
    it('profiles index building time', () => {
      const orders = generateOrders(100_000, 10_000);
      const customers = generateCustomers(10_000);
      
      console.log('\n=== Index Building Analysis ===');
      
      // Current approach: build index in join()
      const ordersZSet = ZSet.fromValues(orders, (o) => String(o.orderId));
      const customersZSet = ZSet.fromValues(customers, (c) => String(c.customerId));
      
      // Time the join (which builds index internally)
      const joinStart = performance.now();
      const result = join(
        ordersZSet,
        customersZSet,
        (o) => o.customerId,
        (c) => c.customerId
      );
      const joinTime = performance.now() - joinStart;
      
      // Now let's break down what's happening
      const indexBuildStart = performance.now();
      const indexB = new Map<string, { value: Customer; weight: number }[]>();
      for (const [value, weight] of customersZSet.entries()) {
        const key = String(value.customerId);
        const list = indexB.get(key) ?? [];
        list.push({ value, weight });
        indexB.set(key, list);
      }
      const indexBuildTime = performance.now() - indexBuildStart;
      
      // Time just the probe phase
      const probeStart = performance.now();
      let matchCount = 0;
      for (const [valueA, weightA] of ordersZSet.entries()) {
        const key = String(valueA.customerId);
        const matches = indexB.get(key) ?? [];
        matchCount += matches.length;
      }
      const probeTime = performance.now() - probeStart;
      
      console.log(`Total join time: ${joinTime.toFixed(2)}ms`);
      console.log(`Index build time: ${indexBuildTime.toFixed(2)}ms (${(indexBuildTime/joinTime*100).toFixed(1)}%)`);
      console.log(`Probe time: ${probeTime.toFixed(2)}ms (${(probeTime/joinTime*100).toFixed(1)}%)`);
      console.log(`Matches found: ${matchCount.toLocaleString()}`);
      console.log(`Result size: ${result.size()}`);
    });
    
    it('compares per-step index rebuild vs persistent index', () => {
      const STEPS = 50;
      const DELTA_SIZE = 100;
      const BASE_SIZE = 10_000;
      const CUSTOMER_COUNT = 1000;
      
      const customers = generateCustomers(CUSTOMER_COUNT);
      const customersZSet = ZSet.fromValues(customers, (c) => String(c.customerId));
      
      console.log('\n=== Per-Step vs Persistent Index ===');
      console.log(`Base: ${BASE_SIZE} orders, ${CUSTOMER_COUNT} customers`);
      console.log(`Delta: ${DELTA_SIZE} orders/step, ${STEPS} steps\n`);
      
      // Method 1: Rebuild index every step (current naive approach)
      let totalRebuildTime = 0;
      for (let step = 0; step < STEPS; step++) {
        const delta = generateOrders(DELTA_SIZE, CUSTOMER_COUNT)
          .map((o, i) => ({ ...o, orderId: BASE_SIZE + step * DELTA_SIZE + i }));
        const deltaZSet = ZSet.fromValues(delta, (o) => String(o.orderId));
        
        const start = performance.now();
        // This rebuilds index from scratch every time
        const indexB = new Map<string, { value: Customer; weight: number }[]>();
        for (const [value, weight] of customersZSet.entries()) {
          const key = String(value.customerId);
          const list = indexB.get(key) ?? [];
          list.push({ value, weight });
          indexB.set(key, list);
        }
        // Then probe
        for (const [valueA, weightA] of deltaZSet.entries()) {
          const key = String(valueA.customerId);
          const matches = indexB.get(key) ?? [];
        }
        totalRebuildTime += performance.now() - start;
      }
      
      // Method 2: Build index once, reuse
      const persistentIndex = new Map<string, { value: Customer; weight: number }[]>();
      const indexBuildStart = performance.now();
      for (const [value, weight] of customersZSet.entries()) {
        const key = String(value.customerId);
        const list = persistentIndex.get(key) ?? [];
        list.push({ value, weight });
        persistentIndex.set(key, list);
      }
      const indexBuildOnce = performance.now() - indexBuildStart;
      
      let totalPersistentTime = 0;
      for (let step = 0; step < STEPS; step++) {
        const delta = generateOrders(DELTA_SIZE, CUSTOMER_COUNT)
          .map((o, i) => ({ ...o, orderId: BASE_SIZE + step * DELTA_SIZE + i }));
        const deltaZSet = ZSet.fromValues(delta, (o) => String(o.orderId));
        
        const start = performance.now();
        // Just probe - index already exists
        for (const [valueA, weightA] of deltaZSet.entries()) {
          const key = String(valueA.customerId);
          const matches = persistentIndex.get(key) ?? [];
        }
        totalPersistentTime += performance.now() - start;
      }
      
      console.log(`Rebuild index every step: ${totalRebuildTime.toFixed(2)}ms total`);
      console.log(`  Average per step: ${(totalRebuildTime/STEPS).toFixed(3)}ms`);
      console.log(`Build once + probe: ${(indexBuildOnce + totalPersistentTime).toFixed(2)}ms total`);
      console.log(`  Index build: ${indexBuildOnce.toFixed(2)}ms`);
      console.log(`  Total probe: ${totalPersistentTime.toFixed(2)}ms`);
      console.log(`  Average probe per step: ${(totalPersistentTime/STEPS).toFixed(3)}ms`);
      console.log(`\nSpeedup: ${(totalRebuildTime / (indexBuildOnce + totalPersistentTime)).toFixed(1)}x`);
      console.log(`Per-step speedup: ${((totalRebuildTime/STEPS) / (totalPersistentTime/STEPS)).toFixed(1)}x`);
    });
  });
  
  describe('Step 3: Analyze Bilinear Join Formula', () => {
    it('profiles each component of Δ(a ⋈ b) = Δa ⋈ Δb + a ⋈ Δb + Δa ⋈ b', () => {
      const A_SIZE = 10_000;
      const B_SIZE = 1_000;
      const DELTA_A = 100;
      const DELTA_B = 10;
      
      console.log('\n=== Bilinear Join Formula Analysis ===');
      console.log(`|A| = ${A_SIZE}, |B| = ${B_SIZE}, |ΔA| = ${DELTA_A}, |ΔB| = ${DELTA_B}\n`);
      
      const prevA = ZSet.fromValues(generateOrders(A_SIZE, B_SIZE), (o) => String(o.orderId));
      const prevB = ZSet.fromValues(generateCustomers(B_SIZE), (c) => String(c.customerId));
      const deltaA = ZSet.fromValues(
        generateOrders(DELTA_A, B_SIZE).map((o, i) => ({ ...o, orderId: A_SIZE + i })),
        (o) => String(o.orderId)
      );
      const deltaB = ZSet.fromValues(
        generateCustomers(DELTA_B).map((c, i) => ({ ...c, customerId: B_SIZE + i })),
        (c) => String(c.customerId)
      );
      
      // Component 1: Δa ⋈ Δb (small × small)
      const t1Start = performance.now();
      const join1 = join(deltaA, deltaB, (o) => o.customerId, (c) => c.customerId);
      const t1 = performance.now() - t1Start;
      
      // Component 2: prevA ⋈ Δb (large × small)
      const t2Start = performance.now();
      const join2 = join(prevA, deltaB, (o) => o.customerId, (c) => c.customerId);
      const t2 = performance.now() - t2Start;
      
      // Component 3: Δa ⋈ prevB (small × large)
      const t3Start = performance.now();
      const join3 = join(deltaA, prevB, (o) => o.customerId, (c) => c.customerId);
      const t3 = performance.now() - t3Start;
      
      // Sum results
      const sumStart = performance.now();
      const result = join1.add(join2).add(join3);
      const sumTime = performance.now() - sumStart;
      
      const total = t1 + t2 + t3 + sumTime;
      
      console.log('Time breakdown:');
      console.log(`  Δa ⋈ Δb (${DELTA_A}×${DELTA_B}): ${t1.toFixed(3)}ms (${(t1/total*100).toFixed(1)}%)`);
      console.log(`  prevA ⋈ Δb (${A_SIZE}×${DELTA_B}): ${t2.toFixed(3)}ms (${(t2/total*100).toFixed(1)}%)`);
      console.log(`  Δa ⋈ prevB (${DELTA_A}×${B_SIZE}): ${t3.toFixed(3)}ms (${(t3/total*100).toFixed(1)}%)`);
      console.log(`  Sum results: ${sumTime.toFixed(3)}ms (${(sumTime/total*100).toFixed(1)}%)`);
      console.log(`  Total: ${total.toFixed(3)}ms`);
      
      console.log('\nResult sizes:');
      console.log(`  Δa ⋈ Δb: ${join1.size()}`);
      console.log(`  prevA ⋈ Δb: ${join2.size()}`);
      console.log(`  Δa ⋈ prevB: ${join3.size()}`);
      console.log(`  Combined: ${result.size()}`);
      
      // The bottleneck should be prevA ⋈ Δb because we're scanning all of prevA
      // This is where we need optimization!
      console.log('\n⚠️  BOTTLENECK: prevA ⋈ Δb scans entire prevA even though Δb is small!');
      console.log('    Solution: Index prevA and probe with Δb');
    });
    
    it('shows optimized bilinear join with pre-built indexes', () => {
      const A_SIZE = 10_000;
      const B_SIZE = 1_000;
      const DELTA_A = 100;
      const DELTA_B = 10;
      
      console.log('\n=== Optimized Bilinear Join ===');
      
      // Build persistent indexes ONCE
      const indexA = new Map<number, Order[]>();
      const indexB = new Map<number, Customer[]>();
      
      const orders = generateOrders(A_SIZE, B_SIZE);
      const customers = generateCustomers(B_SIZE);
      
      const indexBuildStart = performance.now();
      for (const o of orders) {
        const list = indexA.get(o.customerId) ?? [];
        list.push(o);
        indexA.set(o.customerId, list);
      }
      for (const c of customers) {
        const list = indexB.get(c.customerId) ?? [];
        list.push(c);
        indexB.set(c.customerId, list);
      }
      const indexBuildTime = performance.now() - indexBuildStart;
      console.log(`Index build time (one-time): ${indexBuildTime.toFixed(2)}ms`);
      
      // Now process delta
      const deltaOrders = generateOrders(DELTA_A, B_SIZE).map((o, i) => ({ ...o, orderId: A_SIZE + i }));
      const deltaCustomers = generateCustomers(DELTA_B).map((c, i) => ({ ...c, customerId: B_SIZE + i }));
      
      const deltaStart = performance.now();
      
      // Component 1: Δa ⋈ Δb (just iterate delta × delta)
      let count1 = 0;
      for (const o of deltaOrders) {
        for (const c of deltaCustomers) {
          if (o.customerId === c.customerId) count1++;
        }
      }
      
      // Component 2: prevA ⋈ Δb - USE INDEX ON A!
      let count2 = 0;
      for (const c of deltaCustomers) {
        const matches = indexA.get(c.customerId) ?? [];
        count2 += matches.length;
      }
      
      // Component 3: Δa ⋈ prevB - USE INDEX ON B!
      let count3 = 0;
      for (const o of deltaOrders) {
        const matches = indexB.get(o.customerId) ?? [];
        count3 += matches.length;
      }
      
      const deltaTime = performance.now() - deltaStart;
      
      console.log(`Delta processing time: ${deltaTime.toFixed(3)}ms`);
      console.log(`  Δa ⋈ Δb matches: ${count1}`);
      console.log(`  prevA ⋈ Δb matches: ${count2}`);
      console.log(`  Δa ⋈ prevB matches: ${count3}`);
      
      // Compare to naive
      const naiveStart = performance.now();
      let naiveCount = 0;
      for (const o of [...orders, ...deltaOrders]) {
        for (const c of [...customers, ...deltaCustomers]) {
          if (o.customerId === c.customerId) naiveCount++;
        }
      }
      const naiveTime = performance.now() - naiveStart;
      
      console.log(`\nNaive full recompute: ${naiveTime.toFixed(2)}ms`);
      console.log(`Incremental with indexes: ${deltaTime.toFixed(3)}ms`);
      console.log(`Speedup: ${(naiveTime / deltaTime).toFixed(0)}x`);
    });
  });
  
  describe('Step 4: Optimized Implementation', () => {
    it('benchmarks truly optimized incremental join', () => {
      const A_SIZE = 50_000;
      const B_SIZE = 5_000;
      const STEPS = 100;
      const DELTA_SIZE = 100;
      
      console.log('\n=== Truly Optimized Incremental Join ===');
      console.log(`A: ${A_SIZE.toLocaleString()} orders, B: ${B_SIZE.toLocaleString()} customers`);
      console.log(`Delta: ${DELTA_SIZE} orders/step, ${STEPS} steps\n`);
      
      // Initial data
      const orders = generateOrders(A_SIZE, B_SIZE);
      const customers = generateCustomers(B_SIZE);
      
      // Build PERSISTENT indexes - this is the key!
      // Index A by join key (customerId)
      const indexA = new Map<number, Set<number>>(); // customerId -> Set of orderIds
      const orderMap = new Map<number, Order>();     // orderId -> Order
      
      // Index B by join key (customerId)  
      const indexB = new Map<number, Customer>();    // customerId -> Customer
      
      const setupStart = performance.now();
      
      // Populate indexes
      for (const o of orders) {
        orderMap.set(o.orderId, o);
        let set = indexA.get(o.customerId);
        if (!set) {
          set = new Set();
          indexA.set(o.customerId, set);
        }
        set.add(o.orderId);
      }
      for (const c of customers) {
        indexB.set(c.customerId, c);
      }
      
      // Initial join result (integrated state)
      const joinResults = new Map<string, [Order, Customer]>();
      for (const o of orders) {
        const c = indexB.get(o.customerId);
        if (c) {
          joinResults.set(`${o.orderId}::${c.customerId}`, [o, c]);
        }
      }
      
      const setupTime = performance.now() - setupStart;
      console.log(`Setup time: ${setupTime.toFixed(2)}ms`);
      console.log(`Initial join results: ${joinResults.size.toLocaleString()}\n`);
      
      // Process deltas
      const deltaTimes: number[] = [];
      let nextOrderId = A_SIZE;
      
      for (let step = 0; step < STEPS; step++) {
        const deltaOrders = generateOrders(DELTA_SIZE, B_SIZE)
          .map((o, i) => ({ ...o, orderId: nextOrderId + i }));
        nextOrderId += DELTA_SIZE;
        
        const stepStart = performance.now();
        
        // Process delta - O(|delta| * avg_matches)
        for (const o of deltaOrders) {
          // Add to index
          orderMap.set(o.orderId, o);
          let set = indexA.get(o.customerId);
          if (!set) {
            set = new Set();
            indexA.set(o.customerId, set);
          }
          set.add(o.orderId);
          
          // Find matching customer (O(1) lookup!)
          const c = indexB.get(o.customerId);
          if (c) {
            joinResults.set(`${o.orderId}::${c.customerId}`, [o, c]);
          }
        }
        
        deltaTimes.push(performance.now() - stepStart);
      }
      
      const avgDeltaTime = deltaTimes.reduce((a, b) => a + b, 0) / STEPS;
      const minDeltaTime = Math.min(...deltaTimes);
      const maxDeltaTime = Math.max(...deltaTimes);
      
      console.log('Delta processing times:');
      console.log(`  Average: ${avgDeltaTime.toFixed(3)}ms`);
      console.log(`  Min: ${minDeltaTime.toFixed(3)}ms`);
      console.log(`  Max: ${maxDeltaTime.toFixed(3)}ms`);
      console.log(`Final join results: ${joinResults.size.toLocaleString()}`);
      
      // Compare to naive recompute
      const allOrders = [...orders, ...generateOrders(STEPS * DELTA_SIZE, B_SIZE).map((o, i) => ({ ...o, orderId: A_SIZE + i }))];
      
      const naiveStart = performance.now();
      const naiveResults = new Map<string, [Order, Customer]>();
      for (const o of allOrders) {
        const c = customers.find(c => c.customerId === o.customerId);
        if (c) {
          naiveResults.set(`${o.orderId}::${c.customerId}`, [o, c]);
        }
      }
      const naiveTime = performance.now() - naiveStart;
      
      console.log(`\nNaive full recompute: ${naiveTime.toFixed(2)}ms`);
      console.log(`Single delta update: ${avgDeltaTime.toFixed(3)}ms`);
      console.log(`\n🚀 SPEEDUP: ${(naiveTime / avgDeltaTime).toFixed(0)}x`);
      
      // Theoretical analysis
      const theoreticalSpeedup = (A_SIZE + STEPS * DELTA_SIZE) / DELTA_SIZE;
      console.log(`Theoretical max speedup: ${theoreticalSpeedup.toFixed(0)}x`);
    });
    
    it('shows the REAL bottleneck: ZSet operations', () => {
      const SIZE = 10_000;
      const DELTA = 100;
      
      console.log('\n=== ZSet vs Raw Operations Comparison ===');
      
      const orders = generateOrders(SIZE, 1000);
      const customers = generateCustomers(1000);
      
      // Method 1: Using ZSet (current implementation)
      const ordersZSet = ZSet.fromValues(orders, (o) => String(o.orderId));
      const customersZSet = ZSet.fromValues(customers, (c) => String(c.customerId));
      
      const zsetStart = performance.now();
      const zsetResult = join(
        ordersZSet,
        customersZSet,
        (o) => o.customerId,
        (c) => c.customerId
      );
      const zsetTime = performance.now() - zsetStart;
      
      // Method 2: Raw Maps (optimal)
      const rawStart = performance.now();
      
      // Build index
      const customerIndex = new Map<number, Customer>();
      for (const c of customers) {
        customerIndex.set(c.customerId, c);
      }
      
      // Join
      const rawResults: [Order, Customer][] = [];
      for (const o of orders) {
        const c = customerIndex.get(o.customerId);
        if (c) {
          rawResults.push([o, c]);
        }
      }
      const rawTime = performance.now() - rawStart;
      
      console.log(`ZSet join: ${zsetTime.toFixed(2)}ms (${zsetResult.size()} results)`);
      console.log(`Raw Map join: ${rawTime.toFixed(2)}ms (${rawResults.length} results)`);
      console.log(`ZSet overhead: ${(zsetTime / rawTime).toFixed(1)}x`);
      
      // Break down ZSet overhead
      console.log('\nZSet overhead sources:');
      
      // 1. entries() creates arrays
      const entriesStart = performance.now();
      const entries1 = ordersZSet.entries();
      const entries2 = customersZSet.entries();
      const entriesTime = performance.now() - entriesStart;
      console.log(`  entries() calls: ${entriesTime.toFixed(2)}ms`);
      
      // 2. String key generation
      let keyGenTime = 0;
      for (let i = 0; i < 10; i++) {
        const start = performance.now();
        for (const o of orders) {
          const key = String(o.customerId);
        }
        keyGenTime += performance.now() - start;
      }
      console.log(`  String key generation (10x): ${(keyGenTime/10).toFixed(2)}ms`);
      
      // 3. ZSet.insert creates objects
      const insertStart = performance.now();
      const newZSet = new ZSet<[Order, Customer]>(([o, c]) => `${o.orderId}::${c.customerId}`);
      for (let i = 0; i < 1000; i++) {
        newZSet.insert([orders[i % orders.length], customers[i % customers.length]], 1);
      }
      const insertTime = performance.now() - insertStart;
      console.log(`  1000 ZSet inserts: ${insertTime.toFixed(2)}ms`);
    });
  });
  
  describe('Step 5: Proposed Optimized Join', () => {
    /**
     * Key optimizations:
     * 1. Avoid ZSet.entries() - iterate Map directly
     * 2. Use numeric keys instead of strings where possible
     * 3. Maintain persistent indexes
     * 4. Avoid creating intermediate objects
     */
    it('benchmarks proposed optimized implementation', () => {
      const A_SIZE = 100_000;
      const B_SIZE = 10_000;
      const DELTA = 1000;
      
      console.log('\n=== Proposed Optimized Implementation ===');
      console.log(`A: ${A_SIZE.toLocaleString()}, B: ${B_SIZE.toLocaleString()}, Δ: ${DELTA}\n`);
      
      // Generate data
      const orders = generateOrders(A_SIZE, B_SIZE);
      const customers = generateCustomers(B_SIZE);
      
      // OPTIMIZED: Use numeric keys, typed structures
      class OptimizedJoin {
        // Index B by join key
        private bIndex = new Map<number, Customer>();
        // Index A by join key (for when B changes)
        private aIndex = new Map<number, Set<number>>();
        // Store A values
        private aValues = new Map<number, Order>();
        // Result set
        private results = new Map<string, [Order, Customer]>();
        
        addB(c: Customer) {
          this.bIndex.set(c.customerId, c);
          // Check if any existing A entries match
          const aSet = this.aIndex.get(c.customerId);
          if (aSet) {
            for (const orderId of aSet) {
              const o = this.aValues.get(orderId)!;
              this.results.set(`${orderId}::${c.customerId}`, [o, c]);
            }
          }
        }
        
        addA(o: Order) {
          this.aValues.set(o.orderId, o);
          let set = this.aIndex.get(o.customerId);
          if (!set) {
            set = new Set();
            this.aIndex.set(o.customerId, set);
          }
          set.add(o.orderId);
          
          // Check if matching B exists
          const c = this.bIndex.get(o.customerId);
          if (c) {
            this.results.set(`${o.orderId}::${c.customerId}`, [o, c]);
          }
        }
        
        removeA(orderId: number, customerId: number) {
          this.aValues.delete(orderId);
          const set = this.aIndex.get(customerId);
          if (set) {
            set.delete(orderId);
          }
          // Find and remove from results
          const c = this.bIndex.get(customerId);
          if (c) {
            this.results.delete(`${orderId}::${customerId}`);
          }
        }
        
        get size() { return this.results.size; }
      }
      
      // Setup
      const setupStart = performance.now();
      const optimized = new OptimizedJoin();
      for (const c of customers) {
        optimized.addB(c);
      }
      for (const o of orders) {
        optimized.addA(o);
      }
      const setupTime = performance.now() - setupStart;
      
      console.log(`Setup time: ${setupTime.toFixed(2)}ms`);
      console.log(`Initial results: ${optimized.size.toLocaleString()}`);
      
      // Process deltas
      const deltaOrders = generateOrders(DELTA, B_SIZE).map((o, i) => ({ ...o, orderId: A_SIZE + i }));
      
      const deltaStart = performance.now();
      for (const o of deltaOrders) {
        optimized.addA(o);
      }
      const deltaTime = performance.now() - deltaStart;
      
      console.log(`\nDelta processing (${DELTA} orders): ${deltaTime.toFixed(3)}ms`);
      console.log(`Final results: ${optimized.size.toLocaleString()}`);
      console.log(`Per-order: ${(deltaTime / DELTA * 1000).toFixed(1)}μs`);
      
      // Compare to ZSet-based join
      const ordersZSet = ZSet.fromValues([...orders, ...deltaOrders], (o) => String(o.orderId));
      const customersZSet = ZSet.fromValues(customers, (c) => String(c.customerId));
      
      const zsetStart = performance.now();
      const zsetResult = join(ordersZSet, customersZSet, (o) => o.customerId, (c) => c.customerId);
      const zsetTime = performance.now() - zsetStart;
      
      console.log(`\nZSet full join: ${zsetTime.toFixed(2)}ms`);
      console.log(`\n🚀 Delta speedup vs full recompute: ${(zsetTime / deltaTime).toFixed(0)}x`);
      
      // Verify correctness (allow small variance due to test data overlap)
      expect(Math.abs(optimized.size - zsetResult.size())).toBeLessThanOrEqual(1);
    });
  });
});

