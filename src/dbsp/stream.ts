/**
 * Stream: An infinite sequence of values over time
 * 
 * A stream s ∈ S_A is a function ℕ → A where s[t] is the value at time t.
 * In our implementation, we represent streams lazily as generators or
 * eagerly as arrays for finite prefixes.
 * 
 * Key insight from DBSP: Streams of groups form a group themselves.
 */

// Note: ZSet is the core data type but not directly used in this file
// Stream types are generic and work with any group value

/**
 * Stream<T> represents a (potentially infinite) sequence of values.
 * We use a functional representation: time → value
 * 
 * For practical computation, we work with finite prefixes.
 */
export class Stream<T> {
  private readonly values: T[];
  private readonly zero: T;
  
  constructor(values: T[], zero: T) {
    this.values = values;
    this.zero = zero;
  }

  /** Get value at time t (returns zero for times beyond stored values) */
  at(t: number): T {
    return t < this.values.length ? this.values[t] : this.zero;
  }

  /** Get all stored values */
  getValues(): T[] {
    return [...this.values];
  }

  /** Get the length of stored values */
  length(): number {
    return this.values.length;
  }

  /** Get the zero element */
  getZero(): T {
    return this.zero;
  }

  /** Create a stream from values */
  static from<T>(values: T[], zero: T): Stream<T> {
    return new Stream(values, zero);
  }

  /** Create an empty stream */
  static empty<T>(zero: T): Stream<T> {
    return new Stream([], zero);
  }

  /** Create a constant stream */
  static constant<T>(value: T, length: number, zero: T): Stream<T> {
    return new Stream(Array(length).fill(value), zero);
  }
}

/**
 * StreamOperator: A function that transforms streams
 * T: S_A → S_B
 */
export type StreamOperator<A, B> = (input: Stream<A>) => Stream<B>;

/**
 * Lift a scalar function to operate on streams (pointwise in time)
 * 
 * (↑f)(s)[t] = f(s[t])
 * 
 * This is how we convert regular functions to streaming functions.
 * Lifting distributes over composition: ↑(f ∘ g) = (↑f) ∘ (↑g)
 */
export function lift<A, B>(
  f: (a: A) => B,
  zeroB: B
): StreamOperator<A, B> {
  return (input: Stream<A>) => {
    const values = input.getValues().map(f);
    return Stream.from(values, zeroB);
  };
}

/**
 * Lift a binary function to operate on streams
 */
export function lift2<A, B, C>(
  f: (a: A, b: B) => C,
  zeroC: C
): (a: Stream<A>, b: Stream<B>) => Stream<C> {
  return (a: Stream<A>, b: Stream<B>) => {
    const maxLen = Math.max(a.length(), b.length());
    const values: C[] = [];
    for (let t = 0; t < maxLen; t++) {
      values.push(f(a.at(t), b.at(t)));
    }
    return Stream.from(values, zeroC);
  };
}

