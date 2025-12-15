# DBSP - Database Stream Processor

A high-performance TypeScript implementation of **DBSP** (Database Stream Processor) for **Incremental View Maintenance**.

Based on the paper: **"DBSP: Automatic Incremental View Maintenance for Rich Query Languages"** by Budiu et al. (VLDB 2023)

---

## 🚀 Performance Highlights

| Optimization | Speedup | Description |
|--------------|---------|-------------|
| **DBSP (Incremental)** | **57x avg, 87x max** | Process only changes, not entire dataset |
| **Columnar Storage** | **2.8x avg, 6.7x max** | TypedArrays + cache-friendly layout |
| **Combined** | **127x avg, 212x max** | Best of both worlds |

With 1M rows and 0.1% delta, queries complete in **microseconds** instead of tens of milliseconds! This opens up the potential for realtime analytics in the browser.

---

## 📊 Benchmark Results

### Full Optimization Journey (1M rows, 22 columns, 0.1% delta)

| Query | Naive | DBSP | Columnar | **Total Speedup** |
|-------|-------|------|----------|-------------------|
| `COUNT(*)` | 10.4ms | 339μs | 50μs | **208x** |
| `SUM(3 columns)` | 25.3ms | 787μs | 231μs | **109x** |
| `COUNT WHERE AND` | 23.6ms | 328μs | 507μs | **47x** |
| `SUM WHERE BETWEEN AND IN` | 28.8ms | 557μs | 367μs | **79x** |
| `COUNT WHERE LIKE AND IN` | 25.6ms | 294μs | 223μs | **115x** |
| `AVG, MIN, MAX` | 29.8ms | 397μs | 205μs | **145x** |
| `COUNT WHERE OR` | 21.8ms | 870μs | 218μs | **100x** |
| `SUM(4 column expr)` | 25.8ms | 305μs | 122μs | **212x** |

### SQL Compiler Benchmarks (1M rows)

| Query | Speedup vs Naive |
|-------|------------------|
| `WHERE status = "pending"` | **525x avg, 1,291x max** |
| `WHERE price > 50` | **350x avg, 662x max** |
| `WHERE status AND price` | **664x avg, 2,215x max** |
| `COUNT(*)` | **586x avg, 1,246x max** |
| `SUM(price)` | **1,002x avg, 4,281x max** |
| `WHERE LIKE pattern` | **754x avg, 2,560x max** |
| `WHERE IN list` | **628x avg, 1,962x max** |
| `WHERE BETWEEN` | **579x avg, 1,318x max** |

---

## 🎯 Why DBSP?

### The Problem

Traditional databases recompute entire query results when data changes:

```
1M rows + 1 new row = Reprocess 1,000,001 rows ❌
```

### The DBSP Solution

DBSP processes only the **changes** (deltas):

```
1M rows + 1 new row = Process just 1 row ✅
```

### The Key Insight

For **linear operators** (filter, map, count, sum):

```
Δ(Q(R)) = Q(ΔR)
```

The change in the query result equals the query applied to the change!

---

## 📦 Installation

```bash
npm install
```

## 🧪 Running Benchmarks

```bash
# Quick benchmarks (100K rows)
npm run test:run -- src/dbsp/sql/sql-benchmark.test.ts

# Full 1M row benchmark
FULL_BENCHMARK=true npm run test:run -- src/dbsp/sql/sql-benchmark.test.ts

# Optimization showcase (Naive → DBSP → Columnar)
npm run test:run -- src/dbsp/optimization-showcase.test.ts

# Columnar storage benchmarks
npm run test:run -- src/dbsp/columnar.test.ts

# All tests
npm run test:run -- src/dbsp/
```

---

## 🏗️ Architecture

### Layer 1: Core DBSP Primitives

```
┌─────────────────────────────────────────────────────────┐
│                      Z-Sets                             │
│  Multisets with integer weights (+ = insert, - = delete)│
├─────────────────────────────────────────────────────────┤
│                      Streams                            │
│  Infinite sequences of Z-sets over time                 │
├─────────────────────────────────────────────────────────┤
│                     Operators                           │
│  lift(↑) | delay(z⁻¹) | integrate(I) | differentiate(D)│
└─────────────────────────────────────────────────────────┘
```

### Layer 2: Circuit API

```
┌─────────────────────────────────────────────────────────┐
│                      Circuit                            │
│  Dataflow graph builder with inputs, operators, outputs │
├─────────────────────────────────────────────────────────┤
│                   StreamHandle                          │
│  filter() | map() | join() | count() | sum() | distinct()│
└─────────────────────────────────────────────────────────┘
```

### Layer 3: SQL Compiler

```
┌─────────────────────────────────────────────────────────┐
│                    SQLParser                            │
│  node-sql-parser → Custom AST                           │
├─────────────────────────────────────────────────────────┤
│                   SQLCompiler                           │
│  AST → DBSP Circuit (automatic incrementalization)      │
└─────────────────────────────────────────────────────────┘
```

### Layer 4: Columnar Storage

```
┌─────────────────────────────────────────────────────────┐
│                  ColumnarTable                          │
│  TypedArrays | Bitmap Masks | Vectorized Aggregations   │
├─────────────────────────────────────────────────────────┤
│                  ColumnarZSet                           │
│  Columnar storage with DBSP semantics                   │
└─────────────────────────────────────────────────────────┘
```

---

## 📖 Core Concepts

### Z-Sets

A **Z-set** is a set with integer weights. Think of it as a `Map<T, number>`:

```typescript
import { ZSet } from './dbsp';

// Create from values (weight = 1)
const orders = ZSet.fromValues([
  { id: 1, status: 'pending', amount: 100 },
  { id: 2, status: 'shipped', amount: 200 },
]);

// Deltas represent changes
const delta = ZSet.fromEntries([
  [{ id: 3, status: 'pending', amount: 150 }, 1],   // +1 = insert
  [{ id: 1, status: 'pending', amount: 100 }, -1],  // -1 = delete
]);

// Add Z-sets (pointwise)
const updated = orders.add(delta);
```

### Linear Operators

These operators satisfy `Q(a + b) = Q(a) + Q(b)`:

| Operator | Description | Incremental Complexity |
|----------|-------------|------------------------|
| `filter` | Keep matching rows | O(delta) |
| `map` | Transform rows | O(delta) |
| `count` | Sum of weights | O(delta) |
| `sum` | Weighted sum | O(delta) |
| `project` | Select columns | O(delta) |

**Key insight**: Linear operators process deltas directly!

```typescript
// Δ(filter(R)) = filter(ΔR)
// Only filter the 1000 new rows, not 1M existing rows!
```

### Non-Linear Operators

These require state but DBSP handles them incrementally:

| Operator | Formula | Description |
|----------|---------|-------------|
| `distinct` | `H(a) = 1 if a > 0` | Deduplicate |
| `join` | `Δ(a⋈b) = Δa⋈Δb + a⋈Δb + Δa⋈b` | Bilinear |
| `min/max` | Requires full state | Integrate → compute → differentiate |

---

## 🔧 Usage Examples

### 1. Basic Circuit

```typescript
import { Circuit, ZSet } from './dbsp';

const circuit = new Circuit();
const orders = circuit.input<Order>('orders');

// Build incremental query
const pendingHighValue = orders
  .filter(o => o.status === 'pending')
  .filter(o => o.amount > 100);

// Collect results
pendingHighValue.output(delta => {
  console.log('Changes:', delta.values());
});

// Process initial data
circuit.step(new Map([['orders', initialOrders]]));

// Process incremental update (FAST!)
circuit.step(new Map([['orders', newOrdersDelta]]));
```

### 2. SQL Compiler

```typescript
import { SQLCompiler } from './dbsp';

const sql = `
  CREATE TABLE orders (id INT, customer VARCHAR, amount DECIMAL, status VARCHAR);
  CREATE VIEW pending AS SELECT * FROM orders WHERE status = 'pending' AND amount > 100;
`;

const compiler = new SQLCompiler();
const { circuit, views } = compiler.compile(sql);

// Subscribe to changes
views.pending.output(delta => {
  console.log('Pending orders changed:', delta.values());
});

// Process data
circuit.step(new Map([['orders', ordersZSet]]));
```

### 3. Columnar Storage

```typescript
import { ColumnarTable, type TableSchema } from './dbsp';

const schema: TableSchema = {
  columns: [
    { name: 'id', type: 'int32' },
    { name: 'price', type: 'float64' },
    { name: 'status', type: 'string' },
  ]
};

const table = new ColumnarTable(schema, 1_000_000);
table.bulkInsert(orders);

// Vectorized aggregations (10-100x faster!)
const total = table.sum('price');
const count = table.count();
const avg = table.avg('price');

// Bitmap-based filtering
const mask = table.createMaskNumeric('price', '>', 100);
const highValueSum = table.sumMasked('price', mask);
```

---

## 📈 Optimization Techniques

### 1. DBSP: Process Only Changes

**Theory**: For linear operator Q:
```
Q^Δ = D ∘ Q ∘ I = Q
```

**Practice**: Instead of recomputing 1M rows, process only the delta:

```typescript
// BAD: Naive approach - O(n) every time
function naiveCount(allData) {
  return allData.length;
}

// GOOD: DBSP approach - O(delta)
function incrementalCount(delta) {
  return delta.count(); // Just sum delta weights!
}
```

### 2. Columnar: TypedArrays

**Row-based** (slow):
```typescript
// Objects scattered in memory, poor cache locality
for (const row of rows) {
  sum += row.price; // Cache miss for each row!
}
```

**Columnar** (fast):
```typescript
// Contiguous memory, perfect cache locality
const prices = new Float64Array(1_000_000);
for (let i = 0; i < n; i++) {
  sum += prices[i]; // Sequential access = fast!
}
```

### 3. Bitmap Masks

Instead of allocating filtered arrays:

```typescript
// Create bitmap: 1 = matches, 0 = doesn't
const mask = table.createMaskNumeric('price', '>', 100);

// Use mask for aggregation (no allocation!)
const sum = table.sumMasked('price', mask);
```

---

## 🗂️ SQL Feature Support

### DDL
- [x] `CREATE TABLE`
- [x] `CREATE VIEW`

### SELECT Clauses
- [x] `SELECT *`
- [x] `SELECT columns`
- [x] `SELECT DISTINCT`
- [x] Column aliases (`AS`)
- [x] Arithmetic expressions

### WHERE Conditions
- [x] `=`, `!=`, `<`, `>`, `<=`, `>=`
- [x] `AND`, `OR`, `NOT`
- [x] `BETWEEN x AND y`
- [x] `IN (values)`
- [x] `IS NULL`, `IS NOT NULL`
- [x] `LIKE 'pattern%'`

### JOINs
- [x] `INNER JOIN`
- [x] `LEFT JOIN` (parsed)
- [x] `RIGHT JOIN` (parsed)
- [x] `CROSS JOIN` (parsed)

### Aggregations
- [x] `COUNT(*)`
- [x] `SUM(column)`
- [x] `AVG(column)`
- [x] `MIN(column)`
- [x] `MAX(column)`
- [x] `GROUP BY`
- [x] `HAVING`

### Other
- [x] `ORDER BY` (parsed)
- [x] `LIMIT` (parsed)
- [x] `CASE WHEN`
- [x] `COALESCE`
- [x] `CAST`
- [ ] `UNION` (parser limitation)
- [ ] Subqueries

---

## 🔬 How It Works

### Step 1: SQL Parsing

```sql
CREATE VIEW pending AS SELECT * FROM orders WHERE status = 'pending';
```

→ Parsed to AST:

```typescript
{
  type: 'SELECT',
  columns: ['*'],
  from: 'orders',
  where: { type: 'COMPARISON', column: 'status', op: '=', value: 'pending' }
}
```

### Step 2: Circuit Compilation

AST → DBSP operators:

```typescript
const input = circuit.input('orders');
const filtered = input.filter(row => row.status === 'pending');
```

### Step 3: Incremental Execution

```typescript
// Initial load: process all data
circuit.step(new Map([['orders', allOrders]]));

// Updates: process only changes!
circuit.step(new Map([['orders', newOrdersDelta]]));
```

### Step 4: Result Propagation

Changes flow through the circuit:
```
Input Δ → Filter Δ → Output Δ
```

Only affected rows touch each operator!

---

## 📚 Theory Reference

### Core Operators

| Symbol | Name | Definition |
|--------|------|------------|
| `↑f` | Lift | `(↑f)(s)[t] = f(s[t])` |
| `z⁻¹` | Delay | `z⁻¹(s)[t] = s[t-1]` |
| `I` | Integrate | `I(s)[t] = Σᵢ≤ₜ s[i]` |
| `D` | Differentiate | `D(s)[t] = s[t] - s[t-1]` |

### Key Theorems

**Inverse relationship:**
```
D ∘ I = I ∘ D = id
```

**Incremental operator:**
```
Q^Δ = D ∘ Q ∘ I
```

**Linear operators are their own incremental versions:**
```
Q is linear ⟹ Q^Δ = Q
```

**Chain rule:**
```
(Q₁ ∘ Q₂)^Δ = Q₁^Δ ∘ Q₂^Δ
```

---

## 📁 File Structure

```
src/dbsp/
├── zset.ts                 # Z-set implementation (abelian group)
├── zset.test.ts            # Z-set tests
├── stream.ts               # Stream abstraction
├── stream.test.ts          # Stream tests
├── operators.ts            # Core DBSP operators (lift, delay, I, D)
├── operators.test.ts       # Operator tests
├── circuit.ts              # High-level circuit builder
├── circuit.test.ts         # Circuit tests
├── columnar.ts             # Columnar storage (TypedArrays)
├── columnar.test.ts        # Columnar tests + benchmarks
├── sql/
│   ├── sql-compiler.ts     # SQL → DBSP compiler
│   ├── sql.test.ts         # SQL compiler tests (58 tests)
│   └── sql-benchmark.test.ts  # SQL performance benchmarks
├── benchmark-data.ts       # Data generation utilities
├── benchmark.test.ts       # Core DBSP benchmarks
├── optimization-showcase.test.ts  # Naive → DBSP → Columnar comparison
├── examples.ts             # Usage examples
└── index.ts                # Public exports
```

---

## 🔗 References

- [DBSP Paper (VLDB 2023)](https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf)
- [Feldera](https://github.com/feldera/feldera) - Production Rust implementation
- [DBSP Theory Formalization](https://github.com/tchajed/dbsp-theory) - Lean proofs

---

## 📄 License

MIT
