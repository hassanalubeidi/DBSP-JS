/**
 * DBSP Circuit - A high-level API for building streaming computations
 * 
 * A Circuit provides a builder pattern for constructing DBSP dataflow graphs.
 * It manages:
 * - Input sources (streams of deltas)
 * - Operators (transformations)
 * - State (for stateful operators like distinct)
 * - Output sinks (where results go)
 * 
 * This provides an ergonomic way to build incremental queries.
 */

import { ZSet, type Weight, join as zsetJoin } from './zset';
import {
  type GroupValue,
  zsetGroup,
  numberGroup,
  IntegrationState,
  DifferentiationState,
  IncrementalDistinct,
} from './operators';

/**
 * A handle to a stream within a circuit
 */
export class StreamHandle<T> {
  constructor(
    public readonly id: string,
    private readonly circuit: Circuit
  ) {}

  /**
   * Apply filter operator (linear - works directly on deltas)
   */
  filter(predicate: (value: T) => boolean): StreamHandle<T> {
    return this.circuit.addOperator(
      `filter_${this.id}`,
      [this.id],
      (inputs: ZSet<T>[]) => inputs[0].filter(predicate)
    );
  }

  /**
   * Apply map operator (linear - works directly on deltas)
   */
  map<U>(fn: (value: T) => U, keyFn?: (value: U) => string): StreamHandle<U> {
    return this.circuit.addOperator(
      `map_${this.id}`,
      [this.id],
      (inputs: ZSet<T>[]) => inputs[0].map(fn, keyFn)
    );
  }

  /**
   * Apply flatMap operator (linear)
   */
  flatMap<U>(fn: (value: T) => U[], keyFn?: (value: U) => string): StreamHandle<U> {
    return this.circuit.addOperator(
      `flatMap_${this.id}`,
      [this.id],
      (inputs: ZSet<T>[]) => inputs[0].flatMap(fn, keyFn)
    );
  }

  /**
   * Integrate deltas to get current state
   */
  integrate(): StreamHandle<T> {
    const state = new IntegrationState(zsetGroup<T>());
    return this.circuit.addStatefulOperator(
      `integrate_${this.id}`,
      [this.id],
      (inputs: ZSet<T>[]) => state.step(inputs[0]),
      () => state.reset()
    );
  }

  /**
   * Differentiate to get changes
   */
  differentiate(): StreamHandle<T> {
    const state = new DifferentiationState(zsetGroup<T>());
    return this.circuit.addStatefulOperator(
      `differentiate_${this.id}`,
      [this.id],
      (inputs: ZSet<T>[]) => state.step(inputs[0]),
      () => state.reset()
    );
  }

  /**
   * Incremental distinct - handles non-linear distinct incrementally
   */
  distinct(keyFn?: (value: T) => string): StreamHandle<T> {
    const state = new IncrementalDistinct<T>(keyFn);
    return this.circuit.addStatefulOperator(
      `distinct_${this.id}`,
      [this.id],
      (inputs: ZSet<T>[]) => state.step(inputs[0]),
      () => state.reset()
    );
  }

  /**
   * Aggregate: Count (returns stream of numbers, not ZSets)
   * 
   * COUNT is a LINEAR operator in DBSP!
   * Δ(COUNT(R)) = COUNT(ΔR) = Σ weights in delta
   * 
   * This means we just sum the weights in the delta - O(|delta|) not O(|R|)
   */
  count(): StreamHandle<number> {
    return this.circuit.addOperator(
      `count_${this.id}`,
      [this.id],
      (inputs: ZSet<T>[]) => inputs[0].count()  // Just count delta weights - O(delta)!
    ) as unknown as StreamHandle<number>;
  }

  /**
   * Aggregate: Sum with value extractor
   * 
   * SUM is a LINEAR operator in DBSP!
   * Δ(SUM(R)) = SUM(ΔR) = Σ (value * weight) in delta
   * 
   * This means we just sum the delta values - O(|delta|) not O(|R|)
   */
  sum(getValue: (value: T) => number): StreamHandle<number> {
    return this.circuit.addOperator(
      `sum_${this.id}`,
      [this.id],
      (inputs: ZSet<T>[]) => inputs[0].sum(getValue)  // Just sum delta - O(delta)!
    ) as unknown as StreamHandle<number>;
  }

  /**
   * Join with another stream
   */
  join<U, K>(
    other: StreamHandle<U>,
    keyA: (value: T) => K,
    keyB: (value: U) => K,
    keyToString?: (key: K) => string
  ): StreamHandle<[T, U]> {
    // For incremental join, we need:
    // Δ(a ⋈ b) = Δa ⋈ Δb + a ⋈ Δb + Δa ⋈ b
    // This requires maintaining integrated versions of both inputs
    
    const intA = new IntegrationState(zsetGroup<T>());
    const intB = new IntegrationState(zsetGroup<U>());
    
    return this.circuit.addStatefulOperator(
      `join_${this.id}_${other.id}`,
      [this.id, other.id],
      (inputs: [ZSet<T>, ZSet<U>]) => {
        const [deltaA, deltaB] = inputs;
        const prevA = intA.getState();
        const prevB = intB.getState();
        
        // Update state
        intA.step(deltaA);
        intB.step(deltaB);
        
        // Δ(a ⋈ b) = Δa ⋈ Δb + prevA ⋈ Δb + Δa ⋈ prevB
        const join1 = zsetJoin(deltaA, deltaB, keyA, keyB, keyToString);
        const join2 = zsetJoin(prevA, deltaB, keyA, keyB, keyToString);
        const join3 = zsetJoin(deltaA, prevB, keyA, keyB, keyToString);
        
        return join1.add(join2).add(join3);
      },
      () => {
        intA.reset();
        intB.reset();
      }
    ) as StreamHandle<[T, U]>;
  }

  /**
   * Union with another stream (just addition of ZSets)
   */
  union(other: StreamHandle<T>): StreamHandle<T> {
    return this.circuit.addOperator(
      `union_${this.id}_${other.id}`,
      [this.id, other.id],
      (inputs: ZSet<T>[]) => inputs[0].add(inputs[1])
    );
  }

  /**
   * Add an output sink
   */
  output(callback: (value: ZSet<T>) => void): void {
    this.circuit.addOutput(this.id, callback as (value: unknown) => void);
  }

  /**
   * Collect integrated results (current state at each step)
   */
  collect(): T[][] {
    const results: T[][] = [];
    const intState = new IntegrationState(zsetGroup<T>());
    
    this.circuit.addOutput(this.id, (delta: unknown) => {
      const integrated = intState.step(delta as ZSet<T>);
      results.push(integrated.values());
    });
    
    return results;
  }
}

// Operator node types
type OperatorFn<I, O> = (inputs: I) => O;

interface OperatorNode {
  id: string;
  inputIds: string[];
  compute: OperatorFn<unknown[], unknown>;
  reset?: () => void;
}

interface OutputSink {
  streamId: string;
  callback: (value: unknown) => void;
}

/**
 * Circuit - builds and executes DBSP dataflow graphs
 */
export class Circuit {
  private inputs = new Map<string, { value: unknown; keyFn?: (v: unknown) => string }>();
  private operators = new Map<string, OperatorNode>();
  private outputs: OutputSink[] = [];
  private executionOrder: string[] = [];
  private values = new Map<string, unknown>();
  private stepCount = 0;

  /**
   * Create an input source for the circuit
   */
  input<T>(id: string, keyFn?: (value: T) => string): StreamHandle<T> {
    this.inputs.set(id, { value: ZSet.zero<T>(keyFn), keyFn: keyFn as ((v: unknown) => string) | undefined });
    return new StreamHandle<T>(id, this);
  }

  /**
   * Add an operator to the circuit (internal)
   */
  addOperator<I, O>(
    id: string,
    inputIds: string[],
    compute: OperatorFn<I[], O>
  ): StreamHandle<O> {
    this.operators.set(id, {
      id,
      inputIds,
      compute: compute as OperatorFn<unknown[], unknown>,
    });
    this.updateExecutionOrder();
    return new StreamHandle<O>(id, this);
  }

  /**
   * Add a stateful operator to the circuit (internal)
   */
  addStatefulOperator<I, O>(
    id: string,
    inputIds: string[],
    compute: OperatorFn<I[], O>,
    reset: () => void
  ): StreamHandle<O> {
    this.operators.set(id, {
      id,
      inputIds,
      compute: compute as OperatorFn<unknown[], unknown>,
      reset,
    });
    this.updateExecutionOrder();
    return new StreamHandle<O>(id, this);
  }

  /**
   * Add an output sink (internal)
   */
  addOutput(streamId: string, callback: (value: unknown) => void): void {
    this.outputs.push({ streamId, callback });
  }

  /**
   * Update topological execution order
   */
  private updateExecutionOrder(): void {
    const visited = new Set<string>();
    const order: string[] = [];

    const visit = (id: string) => {
      if (visited.has(id)) return;
      visited.add(id);

      const op = this.operators.get(id);
      if (op) {
        for (const inputId of op.inputIds) {
          visit(inputId);
        }
      }
      order.push(id);
    };

    // Visit all operators
    for (const id of this.operators.keys()) {
      visit(id);
    }

    this.executionOrder = order;
  }

  /**
   * Process one step (one batch of deltas)
   */
  step(deltas: Map<string, unknown>): void {
    // Set input values
    for (const [id, delta] of deltas) {
      if (this.inputs.has(id)) {
        this.values.set(id, delta);
      }
    }

    // Execute operators in topological order
    for (const id of this.executionOrder) {
      const op = this.operators.get(id);
      if (op) {
        const inputs = op.inputIds.map(inputId => this.values.get(inputId));
        const output = op.compute(inputs);
        this.values.set(id, output);
      }
    }

    // Call output sinks
    for (const sink of this.outputs) {
      const value = this.values.get(sink.streamId);
      if (value !== undefined) {
        sink.callback(value);
      }
    }

    this.stepCount++;
  }

  /**
   * Reset all stateful operators
   */
  reset(): void {
    for (const op of this.operators.values()) {
      if (op.reset) {
        op.reset();
      }
    }
    this.values.clear();
    this.stepCount = 0;
  }

  /**
   * Get current step count
   */
  getStepCount(): number {
    return this.stepCount;
  }
}

// ============ EXAMPLE BUILDER FUNCTIONS ============

/**
 * Create a simple filter query circuit
 */
export function createFilterQuery<T>(
  predicate: (value: T) => boolean,
  keyFn?: (value: T) => string
): { circuit: Circuit; input: StreamHandle<T>; output: StreamHandle<T> } {
  const circuit = new Circuit();
  const input = circuit.input<T>('input', keyFn);
  const output = input.filter(predicate);
  return { circuit, input, output };
}

/**
 * Create a map query circuit
 */
export function createMapQuery<T, U>(
  fn: (value: T) => U,
  inputKeyFn?: (value: T) => string,
  outputKeyFn?: (value: U) => string
): { circuit: Circuit; input: StreamHandle<T>; output: StreamHandle<U> } {
  const circuit = new Circuit();
  const input = circuit.input<T>('input', inputKeyFn);
  const output = input.map(fn, outputKeyFn);
  return { circuit, input, output };
}

/**
 * Create a filter-map-reduce pipeline
 */
export function createFilterMapReduceQuery<T, U>(
  filterPred: (value: T) => boolean,
  mapFn: (value: T) => U,
  inputKeyFn?: (value: T) => string,
  outputKeyFn?: (value: U) => string
): { circuit: Circuit; input: StreamHandle<T>; mapped: StreamHandle<U> } {
  const circuit = new Circuit();
  const input = circuit.input<T>('input', inputKeyFn);
  const filtered = input.filter(filterPred);
  const mapped = filtered.map(mapFn, outputKeyFn);
  return { circuit, input, mapped };
}

