/**
 * Z-Set: A set with integer weights
 * 
 * Z-sets generalize database tables: each element has an associated weight.
 * - Positive weights represent presence (multiplicity in multisets)
 * - Negative weights represent deletions/removals
 * - Zero weights are not stored (implicit)
 * 
 * Z-sets form an abelian group with pointwise addition.
 */

/** Weight type - integers from ℤ */
export type Weight = number;

/** 
 * ZSet<T> represents a multiset with integer weights
 * Conceptually: Map<T, Weight> where we only store non-zero weights
 */
export class ZSet<T> {
  private readonly data: Map<string, { value: T; weight: Weight }>;
  private readonly keyFn: (value: T) => string;

  constructor(keyFn: (value: T) => string = JSON.stringify) {
    this.data = new Map();
    this.keyFn = keyFn;
  }

  /** Create a ZSet from an array of (value, weight) pairs */
  static fromEntries<T>(
    entries: [T, Weight][],
    keyFn: (value: T) => string = JSON.stringify
  ): ZSet<T> {
    const zset = new ZSet<T>(keyFn);
    for (const [value, weight] of entries) {
      if (weight !== 0) {
        zset.insert(value, weight);
      }
    }
    return zset;
  }

  /** Create a ZSet from values (all with weight 1) - represents a set */
  static fromValues<T>(
    values: T[],
    keyFn: (value: T) => string = JSON.stringify
  ): ZSet<T> {
    return ZSet.fromEntries(values.map(v => [v, 1]), keyFn);
  }

  /** Create an empty ZSet (identity element for addition) */
  static zero<T>(keyFn: (value: T) => string = JSON.stringify): ZSet<T> {
    return new ZSet<T>(keyFn);
  }

  /** Insert or add weight to an element */
  insert(value: T, weight: Weight = 1): void {
    const key = this.keyFn(value);
    const existing = this.data.get(key);
    const newWeight = (existing?.weight ?? 0) + weight;
    
    if (newWeight === 0) {
      this.data.delete(key);
    } else {
      this.data.set(key, { value, weight: newWeight });
    }
  }

  /** Get the weight of an element (0 if not present) */
  getWeight(value: T): Weight {
    const key = this.keyFn(value);
    return this.data.get(key)?.weight ?? 0;
  }

  /** Check if an element is present (non-zero weight) */
  has(value: T): boolean {
    return this.getWeight(value) !== 0;
  }

  /** Get all entries as [value, weight] pairs */
  entries(): [T, Weight][] {
    return Array.from(this.data.values()).map(({ value, weight }) => [value, weight]);
  }

  /** Get all values (ignoring weights) */
  values(): T[] {
    return Array.from(this.data.values()).map(({ value }) => value);
  }

  /** Get the number of distinct elements (non-zero weights) */
  size(): number {
    return this.data.size;
  }

  /** Check if this is the zero element (empty) */
  isZero(): boolean {
    return this.data.size === 0;
  }

  // ============ GROUP OPERATIONS ============

  /** 
   * Add two Z-sets (pointwise addition of weights)
   * This is the group operation.
   */
  add(other: ZSet<T>): ZSet<T> {
    const result = new ZSet<T>(this.keyFn);
    
    // Add all entries from this
    for (const [value, weight] of this.entries()) {
      result.insert(value, weight);
    }
    
    // Add all entries from other
    for (const [value, weight] of other.entries()) {
      result.insert(value, weight);
    }
    
    return result;
  }

  /**
   * Negate a Z-set (negate all weights)
   * This is the group inverse operation.
   */
  negate(): ZSet<T> {
    const result = new ZSet<T>(this.keyFn);
    for (const [value, weight] of this.entries()) {
      result.insert(value, -weight);
    }
    return result;
  }

  /**
   * Subtract another Z-set (this - other)
   * Equivalent to this.add(other.negate())
   */
  subtract(other: ZSet<T>): ZSet<T> {
    return this.add(other.negate());
  }

  // ============ LINEAR OPERATORS ============

  /**
   * Filter: Keep only elements satisfying a predicate
   * This is a LINEAR operator: filter(a + b) = filter(a) + filter(b)
   */
  filter(predicate: (value: T) => boolean): ZSet<T> {
    const result = new ZSet<T>(this.keyFn);
    for (const [value, weight] of this.entries()) {
      if (predicate(value)) {
        result.insert(value, weight);
      }
    }
    return result;
  }

  /**
   * Map: Transform each element
   * This is a LINEAR operator: map(a + b) = map(a) + map(b)
   */
  map<U>(fn: (value: T) => U, keyFn: (value: U) => string = JSON.stringify): ZSet<U> {
    const result = new ZSet<U>(keyFn);
    for (const [value, weight] of this.entries()) {
      result.insert(fn(value), weight);
    }
    return result;
  }

  /**
   * FlatMap: Transform each element to multiple elements
   * This is a LINEAR operator
   */
  flatMap<U>(fn: (value: T) => U[], keyFn: (value: U) => string = JSON.stringify): ZSet<U> {
    const result = new ZSet<U>(keyFn);
    for (const [value, weight] of this.entries()) {
      for (const newValue of fn(value)) {
        result.insert(newValue, weight);
      }
    }
    return result;
  }

  // ============ AGGREGATION ============

  /**
   * Reduce: Aggregate all elements to a single value
   * Weights are considered in the aggregation.
   */
  reduce<U>(fn: (acc: U, value: T, weight: Weight) => U, initial: U): U {
    let result = initial;
    for (const [value, weight] of this.entries()) {
      result = fn(result, value, weight);
    }
    return result;
  }

  /**
   * Count: Sum of all weights
   */
  count(): Weight {
    return this.reduce((acc, _, weight) => acc + weight, 0);
  }

  /**
   * Sum: Weighted sum of numeric values
   */
  sum(getValue: (value: T) => number): number {
    return this.reduce((acc, value, weight) => acc + getValue(value) * weight, 0);
  }

  // ============ SET OPERATIONS ============

  /**
   * Distinct: Convert to a set (all positive weights become 1, negatives removed)
   * distinct(m)[x] = 1 if m[x] > 0, 0 otherwise
   */
  distinct(): ZSet<T> {
    const result = new ZSet<T>(this.keyFn);
    for (const [value, weight] of this.entries()) {
      if (weight > 0) {
        result.insert(value, 1);
      }
    }
    return result;
  }

  /**
   * Check if this represents a set (all weights are 1)
   */
  isSet(): boolean {
    for (const [_, weight] of this.entries()) {
      if (weight !== 1) return false;
    }
    return true;
  }

  /**
   * Check if all weights are positive (represents a bag/multiset)
   */
  isPositive(): boolean {
    for (const [_, weight] of this.entries()) {
      if (weight < 0) return false;
    }
    return true;
  }

  /**
   * Equality check
   */
  equals(other: ZSet<T>): boolean {
    if (this.size() !== other.size()) return false;
    for (const [value, weight] of this.entries()) {
      if (other.getWeight(value) !== weight) return false;
    }
    return true;
  }

  /**
   * Clone this ZSet
   */
  clone(): ZSet<T> {
    return ZSet.fromEntries(this.entries(), this.keyFn);
  }

  /**
   * Debug string representation
   */
  toString(): string {
    const entries = this.entries()
      .map(([v, w]) => `${JSON.stringify(v)} → ${w}`)
      .join(', ');
    return `ZSet { ${entries} }`;
  }
}

// ============ BILINEAR OPERATIONS ============

/**
 * Cartesian product of two Z-sets
 * (a × b)((x, y)) = a[x] × b[y]
 * This is a BILINEAR operator
 */
export function cartesianProduct<T, U>(
  a: ZSet<T>,
  b: ZSet<U>
): ZSet<[T, U]> {
  const result = new ZSet<[T, U]>(([x, y]) => JSON.stringify([x, y]));
  for (const [valueA, weightA] of a.entries()) {
    for (const [valueB, weightB] of b.entries()) {
      result.insert([valueA, valueB], weightA * weightB);
    }
  }
  return result;
}

/**
 * Equi-join of two Z-sets on a key
 * This is a BILINEAR operator
 */
export function join<T, U, K>(
  a: ZSet<T>,
  b: ZSet<U>,
  keyA: (value: T) => K,
  keyB: (value: U) => K,
  keyToString: (key: K) => string = JSON.stringify
): ZSet<[T, U]> {
  // Build an index on b
  const indexB = new Map<string, { value: U; weight: Weight }[]>();
  for (const [value, weight] of b.entries()) {
    const key = keyToString(keyB(value));
    const list = indexB.get(key) ?? [];
    list.push({ value, weight });
    indexB.set(key, list);
  }

  // Join
  const result = new ZSet<[T, U]>(([x, y]) => JSON.stringify([x, y]));
  for (const [valueA, weightA] of a.entries()) {
    const key = keyToString(keyA(valueA));
    const matches = indexB.get(key) ?? [];
    for (const { value: valueB, weight: weightB } of matches) {
      result.insert([valueA, valueB], weightA * weightB);
    }
  }
  return result;
}

