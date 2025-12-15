/**
 * DBSP Examples - Demonstrating Incremental View Maintenance
 * 
 * These examples show how DBSP processes data incrementally,
 * maintaining views efficiently as changes arrive.
 */

import { ZSet } from './zset';
import { Stream } from './stream';
import {
  integrate,
  differentiate,
  numberGroup,
  zsetGroup,
  liftedFilter,
  liftedMap,
  liftedDistinct,
  IncrementalDistinct,
  compose,
} from './operators';
import { Circuit } from './circuit';

// ============================================================
// EXAMPLE 1: Basic Filter (Linear Operator)
// ============================================================

/**
 * Demonstrates that filter is a LINEAR operator.
 * For linear operators: Q^Δ = Q (they process deltas directly!)
 * 
 * Query: SELECT * FROM numbers WHERE value > 5
 */
export function filterExample() {
  console.log('=== Example 1: Filter (Linear Operator) ===\n');
  
  // Stream of deltas (changes to a set of numbers)
  const delta1 = ZSet.fromValues([3, 7, 10]);    // Insert 3, 7, 10
  const delta2 = ZSet.fromValues([8, 2]);        // Insert 8, 2
  const delta3 = ZSet.fromEntries<number>([[7, -1], [15, 1]]); // Delete 7, Insert 15
  
  const deltas = Stream.from([delta1, delta2, delta3], ZSet.zero<number>());
  
  // Apply filter: keep only values > 5
  const filter = liftedFilter<number>(x => x > 5);
  const filteredDeltas = filter(deltas);
  
  // Integrate to see cumulative state at each time
  const integratedView = integrate(zsetGroup<number>())(filteredDeltas);
  
  console.log('Time 0 - Insert [3, 7, 10]:');
  console.log('  Delta: [7, 10] (only > 5 pass)');
  console.log('  View:  {7, 10}');
  console.log(`  Actual: ${JSON.stringify(integratedView.at(0).values())}\n`);
  
  console.log('Time 1 - Insert [8, 2]:');
  console.log('  Delta: [8] (only > 5 pass)');
  console.log('  View:  {7, 8, 10}');
  console.log(`  Actual: ${JSON.stringify(integratedView.at(1).values())}\n`);
  
  console.log('Time 2 - Delete 7, Insert 15:');
  console.log('  Delta: {7: -1, 15: +1}');
  console.log('  View:  {8, 10, 15}');
  console.log(`  Actual: ${JSON.stringify(integratedView.at(2).values())}\n`);
  
  return { deltas, filteredDeltas, integratedView };
}

// ============================================================
// EXAMPLE 2: Map (Linear Operator)  
// ============================================================

/**
 * Demonstrates that map is also LINEAR.
 * map(Δ) = Δ.map(f) - just apply the function to each delta element!
 * 
 * Query: SELECT value * 2 FROM numbers
 */
export function mapExample() {
  console.log('=== Example 2: Map (Linear Operator) ===\n');
  
  const delta1 = ZSet.fromValues([1, 2, 3]);
  const delta2 = ZSet.fromValues([4, 5]);
  const delta3 = ZSet.fromEntries<number>([[2, -1]]); // Delete 2
  
  const deltas = Stream.from([delta1, delta2, delta3], ZSet.zero<number>());
  
  // Apply map: double each value
  const mapper = liftedMap<number, number>(x => x * 2);
  const mappedDeltas = mapper(deltas);
  
  const integratedView = integrate(zsetGroup<number>())(mappedDeltas);
  
  console.log('Time 0 - Insert [1, 2, 3]:');
  console.log('  Delta: [2, 4, 6] (each doubled)');
  console.log(`  Actual: ${JSON.stringify(mappedDeltas.at(0).values())}`);
  console.log(`  View:  ${JSON.stringify(integratedView.at(0).values())}\n`);
  
  console.log('Time 1 - Insert [4, 5]:');
  console.log('  Delta: [8, 10]');
  console.log(`  Actual: ${JSON.stringify(mappedDeltas.at(1).values())}`);
  console.log(`  View:  ${JSON.stringify(integratedView.at(1).values())}\n`);
  
  console.log('Time 2 - Delete 2 (outputs -1 for 4):');
  console.log(`  Delta entries: ${JSON.stringify(mappedDeltas.at(2).entries())}`);
  console.log(`  View:  ${JSON.stringify(integratedView.at(2).values())}\n`);
  
  return { deltas, mappedDeltas, integratedView };
}

// ============================================================
// EXAMPLE 3: Aggregation (Reduce/Sum)
// ============================================================

/**
 * Aggregations like SUM are also linear over Z-sets!
 * 
 * sum(Δ) = Σ (value × weight) for each (value, weight) in Δ
 * 
 * Query: SELECT SUM(value) FROM numbers
 */
export function aggregationExample() {
  console.log('=== Example 3: Aggregation (Sum) ===\n');
  
  // Using raw ZSets to demonstrate weighted sums
  const z1 = ZSet.fromEntries<number>([[10, 1], [20, 1], [30, 1]]); // sum = 60
  const z2 = ZSet.fromEntries<number>([[5, 2]]);  // sum = 10 (5 * 2)
  const z3 = ZSet.fromEntries<number>([[10, -1], [15, 1]]); // sum = -10 + 15 = 5
  
  const deltas = Stream.from([z1, z2, z3], ZSet.zero<number>());
  
  // Compute sum at each step
  const sums: number[] = [];
  let runningSum = 0;
  
  for (let t = 0; t < deltas.length(); t++) {
    const deltaSum = deltas.at(t).sum(x => x);
    runningSum += deltaSum;
    sums.push(runningSum);
  }
  
  console.log('Time 0 - Insert [10, 20, 30]:');
  console.log('  Delta sum: 60');
  console.log(`  Total: ${sums[0]}\n`);
  
  console.log('Time 1 - Insert [5] with weight 2:');
  console.log('  Delta sum: 10 (5 × 2)');
  console.log(`  Total: ${sums[1]}\n`);
  
  console.log('Time 2 - Delete 10, Insert 15:');
  console.log('  Delta sum: 5 (-10 + 15)');
  console.log(`  Total: ${sums[2]}\n`);
  
  return { deltas, sums };
}

// ============================================================
// EXAMPLE 4: Incremental Distinct (Non-Linear!)
// ============================================================

/**
 * DISTINCT is NOT linear - it requires special incremental handling.
 * 
 * We track when elements cross the threshold:
 * - weight: 0 → positive: add to distinct set
 * - weight: positive → 0: remove from distinct set
 */
export function distinctExample() {
  console.log('=== Example 4: Incremental Distinct (Non-Linear) ===\n');
  
  const inc = new IncrementalDistinct<string>();
  
  // Step 1: Insert 'a' twice, 'b' once
  const d1 = ZSet.fromEntries<string>([['a', 2], ['b', 1]]);
  const r1 = inc.step(d1);
  console.log("Step 1 - Insert a×2, b×1:");
  console.log(`  Input: ${JSON.stringify(d1.entries())}`);
  console.log(`  Distinct delta: ${JSON.stringify(r1.entries())}`);
  console.log('  (Both a and b enter distinct set)\n');
  
  // Step 2: Insert 'a' again
  const d2 = ZSet.fromEntries<string>([['a', 1]]);
  const r2 = inc.step(d2);
  console.log("Step 2 - Insert a×1:");
  console.log(`  Input: ${JSON.stringify(d2.entries())}`);
  console.log(`  Distinct delta: ${JSON.stringify(r2.entries())}`);
  console.log('  (No change - a already in distinct)\n');
  
  // Step 3: Delete 2 copies of 'a'
  const d3 = ZSet.fromEntries<string>([['a', -2]]);
  const r3 = inc.step(d3);
  console.log("Step 3 - Delete a×2:");
  console.log(`  Input: ${JSON.stringify(d3.entries())}`);
  console.log(`  Distinct delta: ${JSON.stringify(r3.entries())}`);
  console.log('  (No change - a still has weight 1)\n');
  
  // Step 4: Delete last copy of 'a'
  const d4 = ZSet.fromEntries<string>([['a', -1]]);
  const r4 = inc.step(d4);
  console.log("Step 4 - Delete a×1:");
  console.log(`  Input: ${JSON.stringify(d4.entries())}`);
  console.log(`  Distinct delta: ${JSON.stringify(r4.entries())}`);
  console.log('  (a removed from distinct set!)\n');
  
  return { steps: [r1, r2, r3, r4] };
}

// ============================================================
// EXAMPLE 5: Composed Pipeline (Chain Rule!)
// ============================================================

/**
 * Demonstrates the CHAIN RULE: (Q₁ ∘ Q₂)^Δ = Q₁^Δ ∘ Q₂^Δ
 * 
 * For composed queries, we can incrementalize each part separately!
 * 
 * Query: SELECT value * 2 FROM numbers WHERE value > 0
 */
export function pipelineExample() {
  console.log('=== Example 5: Pipeline (Chain Rule) ===\n');
  console.log('Query: SELECT value * 2 FROM numbers WHERE value > 0\n');
  
  const delta1 = ZSet.fromValues([-5, 3, 7]);
  const delta2 = ZSet.fromValues([2, -1]);
  const delta3 = ZSet.fromEntries<number>([[3, -1], [10, 1]]); // Delete 3, Insert 10
  
  const deltas = Stream.from([delta1, delta2, delta3], ZSet.zero<number>());
  
  // Compose: filter then map
  const filter = liftedFilter<number>(x => x > 0);
  const mapper = liftedMap<number, number>(x => x * 2);
  const pipeline = compose(mapper, filter);
  
  const results = pipeline(deltas);
  const view = integrate(zsetGroup<number>())(results);
  
  console.log('Time 0 - Insert [-5, 3, 7]:');
  console.log('  After filter: [3, 7]');
  console.log('  After map: [6, 14]');
  console.log(`  Result delta: ${JSON.stringify(results.at(0).values())}`);
  console.log(`  View: ${JSON.stringify(view.at(0).values())}\n`);
  
  console.log('Time 1 - Insert [2, -1]:');
  console.log('  After filter: [2] (-1 filtered out)');
  console.log('  After map: [4]');
  console.log(`  Result delta: ${JSON.stringify(results.at(1).values())}`);
  console.log(`  View: ${JSON.stringify(view.at(1).values())}\n`);
  
  console.log('Time 2 - Delete 3, Insert 10:');
  console.log('  Filter passes both (both > 0)');
  console.log('  Map: {6: -1, 20: +1}');
  console.log(`  Result entries: ${JSON.stringify(results.at(2).entries())}`);
  console.log(`  View: ${JSON.stringify(view.at(2).values())}\n`);
  
  return { deltas, results, view };
}

// ============================================================
// EXAMPLE 6: Circuit API for Real-World Queries
// ============================================================

/**
 * Using the Circuit API for a realistic database scenario.
 * 
 * Simulates: Orders table with incremental query processing
 */
export function circuitExample() {
  console.log('=== Example 6: Circuit API (Real-World) ===\n');
  
  interface Order {
    id: number;
    customer: string;
    amount: number;
    status: 'pending' | 'shipped' | 'delivered';
  }
  
  const orderKey = (o: Order) => JSON.stringify(o);
  
  const circuit = new Circuit();
  const orders = circuit.input<Order>('orders', orderKey);
  
  // Query: SELECT * FROM orders WHERE status = 'pending' AND amount > 100
  const highValuePending = orders
    .filter(o => o.status === 'pending')
    .filter(o => o.amount > 100)
    .integrate();
  
  const results: Order[][] = [];
  highValuePending.output(zset => {
    results.push((zset as ZSet<Order>).values());
  });
  
  console.log('Query: SELECT * FROM orders WHERE status = "pending" AND amount > 100\n');
  
  // Transaction 1: Initial orders
  console.log('T1: Insert initial orders');
  circuit.step(new Map([
    ['orders', ZSet.fromValues<Order>([
      { id: 1, customer: 'Alice', amount: 50, status: 'pending' },
      { id: 2, customer: 'Bob', amount: 200, status: 'pending' },
      { id: 3, customer: 'Carol', amount: 150, status: 'shipped' },
    ], orderKey)]
  ]));
  console.log(`  Result: ${results[0].map(o => `${o.customer}($${o.amount})`).join(', ') || 'none'}`);
  console.log('  (Only Bob\'s $200 pending order matches)\n');
  
  // Transaction 2: Order shipped
  console.log('T2: Bob\'s order ships');
  circuit.step(new Map([
    ['orders', ZSet.fromEntries<Order>([
      [{ id: 2, customer: 'Bob', amount: 200, status: 'pending' }, -1],
      [{ id: 2, customer: 'Bob', amount: 200, status: 'shipped' }, 1],
    ], orderKey)]
  ]));
  console.log(`  Result: ${results[1].map(o => `${o.customer}($${o.amount})`).join(', ') || 'none'}`);
  console.log('  (Bob\'s order no longer pending)\n');
  
  // Transaction 3: New high-value order
  console.log('T3: New high-value order from Dave');
  circuit.step(new Map([
    ['orders', ZSet.fromValues<Order>([
      { id: 4, customer: 'Dave', amount: 500, status: 'pending' },
    ], orderKey)]
  ]));
  console.log(`  Result: ${results[2].map(o => `${o.customer}($${o.amount})`).join(', ') || 'none'}`);
  console.log('  (Dave\'s $500 pending order now in view)\n');
  
  return { circuit, results };
}

// ============================================================
// Run All Examples
// ============================================================

export function runAllExamples() {
  console.log('╔══════════════════════════════════════════════════════════════╗');
  console.log('║          DBSP - Database Stream Processor Examples           ║');
  console.log('║     Incremental View Maintenance Made Simple                 ║');
  console.log('╚══════════════════════════════════════════════════════════════╝\n');
  
  filterExample();
  console.log('\n' + '─'.repeat(60) + '\n');
  
  mapExample();
  console.log('\n' + '─'.repeat(60) + '\n');
  
  aggregationExample();
  console.log('\n' + '─'.repeat(60) + '\n');
  
  distinctExample();
  console.log('\n' + '─'.repeat(60) + '\n');
  
  pipelineExample();
  console.log('\n' + '─'.repeat(60) + '\n');
  
  circuitExample();
  
  console.log('╔══════════════════════════════════════════════════════════════╗');
  console.log('║  Key Insights:                                               ║');
  console.log('║  • Linear operators (filter, map, project) are incremental!  ║');
  console.log('║  • Q^Δ = D ∘ Q ∘ I (incremental version formula)             ║');
  console.log('║  • For linear Q: Q^Δ = Q (process deltas directly)           ║');
  console.log('║  • Chain rule: (Q₁∘Q₂)^Δ = Q₁^Δ ∘ Q₂^Δ                      ║');
  console.log('║  • D and I are inverses: D(I(s)) = I(D(s)) = s               ║');
  console.log('╚══════════════════════════════════════════════════════════════╝');
}

// Export for use in browser console or Node
if (typeof window !== 'undefined') {
  (window as unknown as Record<string, unknown>).dbspExamples = {
    filterExample,
    mapExample,
    aggregationExample,
    distinctExample,
    pipelineExample,
    circuitExample,
    runAllExamples,
  };
}

