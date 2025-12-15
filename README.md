# DBSP Online

An interactive TypeScript implementation of **DBSP (Database Stream Processor)** for learning and experimenting with **Incremental View Maintenance**.

## What is DBSP?

DBSP is a mathematical framework for incremental computation over streams. It provides a principled way to maintain database views efficiently as data changes, processing only the changes (deltas) rather than recomputing from scratch.

Based on the paper: **"DBSP: Automatic Incremental View Maintenance for Rich Query Languages"** by Budiu et al. (VLDB 2023).

## Features

- ✅ **Z-Sets**: Sets with integer weights (abelian group structure)
- ✅ **Streams**: Infinite sequences of values over time
- ✅ **Core Operators**: Lift (↑), Delay (z⁻¹), Integrate (I), Differentiate (D)
- ✅ **Linear Operators**: Filter, Map, FlatMap (process deltas directly!)
- ✅ **Incremental Distinct**: Non-linear operator with proper incrementalization
- ✅ **Joins**: Bilinear incremental joins
- ✅ **Circuit API**: High-level dataflow graph builder
- ✅ **Comprehensive Tests**: 72 tests with TDD methodology
- ✅ **Interactive UI**: Visual DBSP playground

## Quick Start

```bash
# Install dependencies
npm install

# Run tests
npm run test:run

# Start development server
npm run dev
```

## Usage

```typescript
import { ZSet, Circuit } from './dbsp';

// Create a circuit
const circuit = new Circuit();
const numbers = circuit.input<number>('numbers');

// Build query: SELECT * FROM numbers WHERE value > 5
const filtered = numbers.filter(x => x > 5).integrate();

// Collect results
const results: number[][] = [];
filtered.output(zset => results.push((zset as ZSet<number>).values()));

// Process deltas
circuit.step(new Map([['numbers', ZSet.fromValues([3, 7, 10])]]));
console.log(results[0]); // [7, 10]

circuit.step(new Map([['numbers', ZSet.fromValues([8, 2])]]));
console.log(results[1]); // [7, 8, 10]
```

## Key Insights from DBSP

### 1. Incremental Version Formula
```
Q^Δ = D ∘ Q ∘ I
```
The incremental version of any operator Q is: differentiate → apply Q → integrate.

### 2. Linear Operators are "Free"
For linear operators (filter, map, project):
```
Q^Δ = Q
```
They process deltas directly without maintaining state!

### 3. Chain Rule for Composition
```
(Q₁ ∘ Q₂)^Δ = Q₁^Δ ∘ Q₂^Δ
```
Incrementalizing composed queries is just composing incremental queries.

### 4. D and I are Inverses
```
D(I(s)) = I(D(s)) = s
```
Differentiation and integration cancel out.

## Project Structure

```
src/dbsp/
├── index.ts        # Main exports
├── zset.ts         # Z-Set implementation (abelian group)
├── stream.ts       # Stream types and lift operator
├── operators.ts    # Core DBSP operators (D, I, z⁻¹)
├── circuit.ts      # High-level Circuit API
├── examples.ts     # Comprehensive examples
├── README.md       # Library documentation
└── *.test.ts       # Test files (72 tests)
```

## Tests

```bash
# Run all tests
npm run test:run

# Run tests in watch mode
npm test
```

## References

- [DBSP Paper (VLDB 2023)](https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf)
- [Feldera](https://github.com/feldera/feldera) - Production implementation in Rust
- [DBSP Theory Formalization](https://github.com/tchajed/dbsp-theory) - Lean theorem prover proofs

## License

MIT
