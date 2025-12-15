/**
 * DBSP Core Operators
 * 
 * The four fundamental operators of DBSP:
 * 1. Lift (↑): Apply a function pointwise to streams
 * 2. Delay (z^-1): Delay stream by one timestamp
 * 3. Integration (I): Cumulative sum of stream values
 * 4. Differentiation (D): Compute differences between consecutive values
 * 
 * Key theorems:
 * - D and I are inverses: D(I(s)) = I(D(s)) = s
 * - Linear operators are their own incremental versions: Q^Δ = Q
 * - Chain rule: (Q₁ ∘ Q₂)^Δ = Q₁^Δ ∘ Q₂^Δ
 */

import { Stream, type StreamOperator, lift, lift2 } from './stream';
import { ZSet, type Weight } from './zset';

// ============ GROUP OPERATIONS ON STREAMS ============

/**
 * Check if a type has group operations
 */
export interface GroupValue<T> {
  zero: () => T;
  add: (a: T, b: T) => T;
  negate: (a: T) => T;
}

/**
 * Group operations for ZSet
 */
export function zsetGroup<T>(): GroupValue<ZSet<T>> {
  return {
    zero: () => ZSet.zero<T>(),
    add: (a, b) => a.add(b),
    negate: (a) => a.negate(),
  };
}

/**
 * Group operations for numbers
 */
export function numberGroup(): GroupValue<number> {
  return {
    zero: () => 0,
    add: (a, b) => a + b,
    negate: (a) => -a,
  };
}

// ============ DELAY OPERATOR (z^-1) ============

/**
 * Delay operator (z^-1): Delays input by one timestamp
 * 
 * z^-1(s)[t] = { zero     when t = 0
 *              { s[t-1]   when t ≥ 1
 * 
 * Properties:
 * - Strict: output[t] only depends on input[0..t-1]
 * - Time-invariant
 * - Linear
 */
export function delay<T>(zero: T): StreamOperator<T, T> {
  return (input: Stream<T>) => {
    const values = input.getValues();
    // Prepend zero, shift everything by one
    const delayed = [zero, ...values.slice(0, values.length)];
    return Stream.from(delayed, zero);
  };
}

/**
 * Delay operator that maintains state across calls (for incremental processing)
 */
export class DelayState<T> {
  private previous: T;
  
  constructor(private zero: T) {
    this.previous = zero;
  }

  /** Process one value, return the delayed value */
  step(input: T): T {
    const output = this.previous;
    this.previous = input;
    return output;
  }

  /** Reset state */
  reset(): void {
    this.previous = this.zero;
  }
}

// ============ INTEGRATION OPERATOR (I) ============

/**
 * Integration operator (I): Cumulative sum of stream values
 * 
 * I(s)[t] = Σ_{i≤t} s[i]
 * 
 * Properties:
 * - Causal and Linear (LTI)
 * - I(s)[t] = s[t] + z^-1(I(s))[t]  (feedback definition)
 */
export function integrate<T>(group: GroupValue<T>): StreamOperator<T, T> {
  return (input: Stream<T>) => {
    const values = input.getValues();
    const integrated: T[] = [];
    let sum = group.zero();
    
    for (const value of values) {
      sum = group.add(sum, value);
      integrated.push(sum);
    }
    
    return Stream.from(integrated, sum);
  };
}

/**
 * Stateful integration for incremental processing
 */
export class IntegrationState<T> {
  private sum: T;
  
  constructor(private group: GroupValue<T>) {
    this.sum = group.zero();
  }

  /** Process one value, return the integrated value */
  step(input: T): T {
    this.sum = this.group.add(this.sum, input);
    return this.sum;
  }

  /** Get current state */
  getState(): T {
    return this.sum;
  }

  /** Reset state */
  reset(): void {
    this.sum = this.group.zero();
  }
}

// ============ DIFFERENTIATION OPERATOR (D) ============

/**
 * Differentiation operator (D): Compute differences between consecutive values
 * 
 * D(s)[t] = s[t] - s[t-1]  (with s[-1] = 0)
 * D(s) = s - z^-1(s)
 * 
 * Properties:
 * - Causal and Linear (LTI)
 * - D and I are inverses: D(I(s)) = I(D(s)) = s
 */
export function differentiate<T>(group: GroupValue<T>): StreamOperator<T, T> {
  return (input: Stream<T>) => {
    const values = input.getValues();
    const differentiated: T[] = [];
    let previous = group.zero();
    
    for (const value of values) {
      // D(s)[t] = s[t] - s[t-1]
      const diff = group.add(value, group.negate(previous));
      differentiated.push(diff);
      previous = value;
    }
    
    return Stream.from(differentiated, group.zero());
  };
}

/**
 * Stateful differentiation for incremental processing
 */
export class DifferentiationState<T> {
  private previous: T;
  
  constructor(private group: GroupValue<T>) {
    this.previous = group.zero();
  }

  /** Process one value, return the differentiated value */
  step(input: T): T {
    const diff = this.group.add(input, this.group.negate(this.previous));
    this.previous = input;
    return diff;
  }

  /** Reset state */
  reset(): void {
    this.previous = this.group.zero();
  }
}

// ============ INCREMENTAL VERSION OPERATOR (^Δ) ============

/**
 * The incremental version of an operator Q is defined as:
 * Q^Δ = D ∘ Q ∘ I
 * 
 * Key insight: Q^Δ transforms streams of changes into streams of changes.
 * 
 * For LINEAR operators (like filter, map, projection):
 * Q^Δ = Q (they are their own incremental versions!)
 * 
 * This means linear operators can process deltas directly without
 * maintaining the full state.
 */

/**
 * Create the incremental version of an operator
 * Q^Δ = D ∘ Q ∘ I
 */
export function incrementalize<T, U>(
  operator: StreamOperator<T, U>,
  groupT: GroupValue<T>,
  groupU: GroupValue<U>
): StreamOperator<T, U> {
  return (deltaInput: Stream<T>) => {
    // I: integrate the deltas to get full values
    const integrated = integrate(groupT)(deltaInput);
    // Q: apply the operator
    const result = operator(integrated);
    // D: differentiate to get deltas
    return differentiate(groupU)(result);
  };
}

// ============ COMPOSED OPERATORS FOR ZSETS ============

/**
 * Lifted filter on streams of ZSets
 * Since filter is LINEAR, the incremental version is the same!
 */
export function liftedFilter<T>(
  predicate: (value: T) => boolean
): StreamOperator<ZSet<T>, ZSet<T>> {
  return lift(
    (zset: ZSet<T>) => zset.filter(predicate),
    ZSet.zero<T>()
  );
}

/**
 * Lifted map on streams of ZSets
 * Since map is LINEAR, the incremental version is the same!
 */
export function liftedMap<T, U>(
  fn: (value: T) => U,
  keyFn?: (value: U) => string
): StreamOperator<ZSet<T>, ZSet<U>> {
  return lift(
    (zset: ZSet<T>) => zset.map(fn, keyFn),
    ZSet.zero<U>(keyFn)
  );
}

/**
 * Lifted aggregation (reduce) on streams of ZSets
 * 
 * Note: Aggregation in general is NOT linear!
 * The incremental version needs special handling.
 */
export function liftedReduce<T, U>(
  fn: (acc: U, value: T, weight: Weight) => U,
  initial: U
): StreamOperator<ZSet<T>, U> {
  return (input: Stream<ZSet<T>>) => {
    const values = input.getValues().map(zset => zset.reduce(fn, initial));
    return Stream.from(values, initial);
  };
}

/**
 * Lifted count aggregation
 */
export function liftedCount<T>(): StreamOperator<ZSet<T>, number> {
  return lift(
    (zset: ZSet<T>) => zset.count(),
    0
  );
}

/**
 * Lifted sum aggregation
 */
export function liftedSum<T>(getValue: (value: T) => number): StreamOperator<ZSet<T>, number> {
  return lift(
    (zset: ZSet<T>) => zset.sum(getValue),
    0
  );
}

/**
 * Lifted distinct operation
 */
export function liftedDistinct<T>(): StreamOperator<ZSet<T>, ZSet<T>> {
  return lift(
    (zset: ZSet<T>) => zset.distinct(),
    ZSet.zero<T>()
  );
}

// ============ INCREMENTAL DISTINCT ============

/**
 * Incremental distinct is more complex because distinct is NOT linear.
 * 
 * We need to track the integrated input and detect when elements
 * cross the threshold (0 → positive or positive → 0).
 * 
 * H(i, d)[x] = { -1 if i[x] > 0 and (i+d)[x] ≤ 0
 *             {  1 if i[x] ≤ 0 and (i+d)[x] > 0
 *             {  0 otherwise
 */
export class IncrementalDistinct<T> {
  private integrated: ZSet<T>;

  constructor(private keyFn: (value: T) => string = JSON.stringify) {
    this.integrated = ZSet.zero<T>(keyFn);
  }

  /**
   * Process a delta, return the change to the distinct output
   */
  step(delta: ZSet<T>): ZSet<T> {
    const result = ZSet.zero<T>(this.keyFn);
    
    for (const [value, deltaWeight] of delta.entries()) {
      const oldWeight = this.integrated.getWeight(value);
      const newWeight = oldWeight + deltaWeight;
      
      // Detect threshold crossing
      if (oldWeight > 0 && newWeight <= 0) {
        // Was present, now removed
        result.insert(value, -1);
      } else if (oldWeight <= 0 && newWeight > 0) {
        // Was not present, now added
        result.insert(value, 1);
      }
      // If both sides of threshold are same, no change to output
    }
    
    // Update integrated state
    this.integrated = this.integrated.add(delta);
    
    return result;
  }

  /** Reset state */
  reset(): void {
    this.integrated = ZSet.zero<T>(this.keyFn);
  }
}

// ============ STREAM COMPOSITION UTILITIES ============

/**
 * Compose two stream operators
 * (f ∘ g)(x) = f(g(x))
 */
export function compose<A, B, C>(
  f: StreamOperator<B, C>,
  g: StreamOperator<A, B>
): StreamOperator<A, C> {
  return (input: Stream<A>) => f(g(input));
}

/**
 * Chain multiple stream operators
 */
export function chain<T>(...operators: StreamOperator<T, T>[]): StreamOperator<T, T> {
  return (input: Stream<T>) => {
    let current = input;
    for (const op of operators) {
      current = op(current);
    }
    return current;
  };
}

