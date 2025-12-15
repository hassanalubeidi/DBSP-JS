import { describe, it, expect } from 'vitest';
import { Stream } from './stream';
import { ZSet } from './zset';
import {
  delay,
  integrate,
  differentiate,
  incrementalize,
  numberGroup,
  zsetGroup,
  liftedFilter,
  liftedMap,
  liftedCount,
  liftedDistinct,
  IncrementalDistinct,
  DelayState,
  IntegrationState,
  DifferentiationState,
  compose,
} from './operators';

describe('Delay Operator (z^-1)', () => {
  it('should delay by one timestamp', () => {
    const s = Stream.from([1, 2, 3, 4], 0);
    const delayed = delay(0)(s);
    
    // z^-1([1,2,3,4]) = [0,1,2,3,4]
    expect(delayed.at(0)).toBe(0);  // zero at t=0
    expect(delayed.at(1)).toBe(1);
    expect(delayed.at(2)).toBe(2);
    expect(delayed.at(3)).toBe(3);
    expect(delayed.at(4)).toBe(4);
  });

  it('should work with ZSets', () => {
    const z1 = ZSet.fromValues([1, 2]);
    const z2 = ZSet.fromValues([3, 4]);
    const s = Stream.from([z1, z2], ZSet.zero<number>());
    const delayed = delay(ZSet.zero<number>())(s);
    
    expect(delayed.at(0).isZero()).toBe(true);
    expect(delayed.at(1).equals(z1)).toBe(true);
    expect(delayed.at(2).equals(z2)).toBe(true);
  });

  describe('DelayState (stateful)', () => {
    it('should maintain state across steps', () => {
      const state = new DelayState(0);
      
      expect(state.step(1)).toBe(0);  // returns previous (zero)
      expect(state.step(2)).toBe(1);  // returns previous (1)
      expect(state.step(3)).toBe(2);  // returns previous (2)
    });
  });
});

describe('Integration Operator (I)', () => {
  it('should compute cumulative sum', () => {
    // I([1,1,1,1]) = [1,2,3,4]
    const s = Stream.from([1, 1, 1, 1], 0);
    const integrated = integrate(numberGroup())(s);
    
    expect(integrated.getValues()).toEqual([1, 2, 3, 4]);
  });

  it('should work with negative values', () => {
    // I([1, -1, 2, -2]) = [1, 0, 2, 0]
    const s = Stream.from([1, -1, 2, -2], 0);
    const integrated = integrate(numberGroup())(s);
    
    expect(integrated.getValues()).toEqual([1, 0, 2, 0]);
  });

  it('should work with ZSets', () => {
    const d1 = ZSet.fromEntries<number>([[1, 1]]);  // insert 1
    const d2 = ZSet.fromEntries<number>([[2, 1]]);  // insert 2
    const d3 = ZSet.fromEntries<number>([[1, -1]]); // delete 1
    
    const s = Stream.from([d1, d2, d3], ZSet.zero<number>());
    const integrated = integrate(zsetGroup<number>())(s);
    
    // After t=0: {1}
    // After t=1: {1, 2}
    // After t=2: {2}
    expect(integrated.at(0).getWeight(1)).toBe(1);
    expect(integrated.at(0).getWeight(2)).toBe(0);
    
    expect(integrated.at(1).getWeight(1)).toBe(1);
    expect(integrated.at(1).getWeight(2)).toBe(1);
    
    expect(integrated.at(2).getWeight(1)).toBe(0);
    expect(integrated.at(2).getWeight(2)).toBe(1);
  });

  describe('IntegrationState (stateful)', () => {
    it('should maintain state across steps', () => {
      const state = new IntegrationState(numberGroup());
      
      expect(state.step(1)).toBe(1);
      expect(state.step(1)).toBe(2);
      expect(state.step(1)).toBe(3);
      expect(state.getState()).toBe(3);
    });
  });
});

describe('Differentiation Operator (D)', () => {
  it('should compute differences', () => {
    // D([1,2,3,4]) = [1,1,1,1] (assuming start from 0)
    const s = Stream.from([1, 2, 3, 4], 0);
    const differentiated = differentiate(numberGroup())(s);
    
    expect(differentiated.getValues()).toEqual([1, 1, 1, 1]);
  });

  it('should work with decreasing values', () => {
    // D([4,3,2,1]) = [4, -1, -1, -1]
    const s = Stream.from([4, 3, 2, 1], 0);
    const differentiated = differentiate(numberGroup())(s);
    
    expect(differentiated.getValues()).toEqual([4, -1, -1, -1]);
  });

  it('should work with ZSets', () => {
    const z0 = ZSet.zero<number>();
    const z1 = ZSet.fromValues([1]);
    const z2 = ZSet.fromValues([1, 2]);
    const z3 = ZSet.fromValues([2]);
    
    const s = Stream.from([z1, z2, z3], z0);
    const differentiated = differentiate(zsetGroup<number>())(s);
    
    // D at t=0: {1} - {} = {1}
    // D at t=1: {1,2} - {1} = {2}
    // D at t=2: {2} - {1,2} = {1: -1}
    expect(differentiated.at(0).getWeight(1)).toBe(1);
    expect(differentiated.at(1).getWeight(2)).toBe(1);
    expect(differentiated.at(2).getWeight(1)).toBe(-1);
    expect(differentiated.at(2).getWeight(2)).toBe(0);
  });

  describe('DifferentiationState (stateful)', () => {
    it('should maintain state across steps', () => {
      const state = new DifferentiationState(numberGroup());
      
      expect(state.step(1)).toBe(1);   // 1 - 0
      expect(state.step(3)).toBe(2);   // 3 - 1
      expect(state.step(6)).toBe(3);   // 6 - 3
    });
  });
});

describe('D and I are inverses', () => {
  it('D(I(s)) = s', () => {
    const s = Stream.from([1, 2, 3, 4, 5], 0);
    const group = numberGroup();
    
    // D(I(s)) should equal s
    const result = differentiate(group)(integrate(group)(s));
    
    expect(result.getValues()).toEqual(s.getValues());
  });

  it('I(D(s)) = s', () => {
    const s = Stream.from([1, 2, 3, 4, 5], 0);
    const group = numberGroup();
    
    // I(D(s)) should equal s
    const result = integrate(group)(differentiate(group)(s));
    
    expect(result.getValues()).toEqual(s.getValues());
  });

  it('D and I are inverses for ZSets', () => {
    const z1 = ZSet.fromValues([1, 2]);
    const z2 = ZSet.fromValues([2, 3]);
    const z3 = ZSet.fromValues([1, 3]);
    const s = Stream.from([z1, z2, z3], ZSet.zero<number>());
    const group = zsetGroup<number>();
    
    const dI = differentiate(group)(integrate(group)(s));
    const iD = integrate(group)(differentiate(group)(s));
    
    for (let t = 0; t < s.length(); t++) {
      expect(dI.at(t).equals(s.at(t))).toBe(true);
      expect(iD.at(t).equals(s.at(t))).toBe(true);
    }
  });
});

describe('Incrementalization (Q^Δ = D ∘ Q ∘ I)', () => {
  it('should incrementalize a non-incremental operator', () => {
    // For a simple doubling operator
    const doubler = (s: Stream<number>) => {
      return Stream.from(s.getValues().map(x => x * 2), 0);
    };
    
    const group = numberGroup();
    const incrementalDoubler = incrementalize(doubler, group, group);
    
    // Input deltas: [1, 1, 1] → integrated: [1, 2, 3] → doubled: [2, 4, 6] → differentiated: [2, 2, 2]
    const deltas = Stream.from([1, 1, 1], 0);
    const result = incrementalDoubler(deltas);
    
    expect(result.getValues()).toEqual([2, 2, 2]);
  });
});

describe('Linear operators are their own incremental versions', () => {
  it('filter: incremental filter = filter (on deltas)', () => {
    const pred = (x: number) => x > 5;
    const filter = liftedFilter(pred);
    const group = zsetGroup<number>();
    
    // Create some deltas
    const d1 = ZSet.fromEntries<number>([[3, 1], [7, 1]]);
    const d2 = ZSet.fromEntries<number>([[8, 1], [4, 1]]);
    const deltas = Stream.from([d1, d2], ZSet.zero<number>());
    
    // Apply filter directly to deltas
    const directResult = filter(deltas);
    
    // Apply incrementalized filter
    const incrementalFilter = incrementalize(filter, group, group);
    const incrementalResult = incrementalFilter(deltas);
    
    // For linear operators, results should match!
    for (let t = 0; t < deltas.length(); t++) {
      expect(directResult.at(t).equals(incrementalResult.at(t))).toBe(true);
    }
  });

  it('map: incremental map = map (on deltas)', () => {
    const mapper = liftedMap((x: number) => x * 2);
    const group = zsetGroup<number>();
    
    const d1 = ZSet.fromEntries<number>([[1, 1], [2, 1]]);
    const d2 = ZSet.fromEntries<number>([[3, 1]]);
    const deltas = Stream.from([d1, d2], ZSet.zero<number>());
    
    const directResult = mapper(deltas);
    const incrementalResult = incrementalize(mapper, group, group)(deltas);
    
    for (let t = 0; t < deltas.length(); t++) {
      expect(directResult.at(t).equals(incrementalResult.at(t))).toBe(true);
    }
  });
});

describe('Incremental Distinct', () => {
  it('should handle insertions correctly', () => {
    const inc = new IncrementalDistinct<number>();
    
    // Insert 1
    const d1 = ZSet.fromEntries<number>([[1, 1]]);
    const r1 = inc.step(d1);
    expect(r1.getWeight(1)).toBe(1);  // 1 appears in distinct
    
    // Insert 1 again (duplicate)
    const d2 = ZSet.fromEntries<number>([[1, 1]]);
    const r2 = inc.step(d2);
    expect(r2.getWeight(1)).toBe(0);  // no change to distinct
    
    // Insert 2
    const d3 = ZSet.fromEntries<number>([[2, 1]]);
    const r3 = inc.step(d3);
    expect(r3.getWeight(1)).toBe(0);  // 1 unchanged
    expect(r3.getWeight(2)).toBe(1);  // 2 appears
  });

  it('should handle deletions correctly', () => {
    const inc = new IncrementalDistinct<number>();
    
    // Insert two copies of 1
    inc.step(ZSet.fromEntries<number>([[1, 2]]));
    
    // Delete one copy
    const r1 = inc.step(ZSet.fromEntries<number>([[1, -1]]));
    expect(r1.getWeight(1)).toBe(0);  // still present
    
    // Delete last copy
    const r2 = inc.step(ZSet.fromEntries<number>([[1, -1]]));
    expect(r2.getWeight(1)).toBe(-1);  // removed from distinct
  });

  it('should correctly track threshold crossings', () => {
    const inc = new IncrementalDistinct<string>();
    
    // Start with nothing
    // Insert 'a' with weight 3
    const r1 = inc.step(ZSet.fromEntries<string>([['a', 3]]));
    expect(r1.getWeight('a')).toBe(1);  // crossed 0 → positive
    
    // Reduce 'a' weight by 2 (still positive)
    const r2 = inc.step(ZSet.fromEntries<string>([['a', -2]]));
    expect(r2.getWeight('a')).toBe(0);  // no change (still positive)
    
    // Reduce 'a' weight by 2 (now negative)
    const r3 = inc.step(ZSet.fromEntries<string>([['a', -2]]));
    expect(r3.getWeight('a')).toBe(-1);  // crossed positive → non-positive
  });
});

describe('Composition', () => {
  it('should compose operators correctly', () => {
    const filter = liftedFilter<number>(x => x > 0);
    const mapper = liftedMap<number, number>(x => x * 2);
    
    const composed = compose(mapper, filter);
    
    const input = Stream.from([
      ZSet.fromEntries<number>([[-1, 1], [1, 1], [2, 1]]),
      ZSet.fromEntries<number>([[3, 1], [-2, 1]]),
    ], ZSet.zero<number>());
    
    const result = composed(input);
    
    // Filter removes negatives, then map doubles
    expect(result.at(0).getWeight(2)).toBe(1);  // 1 * 2
    expect(result.at(0).getWeight(4)).toBe(1);  // 2 * 2
    expect(result.at(0).has(-2)).toBe(false);
    
    expect(result.at(1).getWeight(6)).toBe(1);  // 3 * 2
  });

  it('incrementalizing composed operators follows chain rule', () => {
    // (Q₁ ∘ Q₂)^Δ = Q₁^Δ ∘ Q₂^Δ
    const filter = liftedFilter<number>(x => x > 0);
    const mapper = liftedMap<number, number>(x => x * 2);
    const group = zsetGroup<number>();
    
    const composed = compose(mapper, filter);
    
    const deltas = Stream.from([
      ZSet.fromEntries<number>([[-1, 1], [1, 1], [2, 1]]),
      ZSet.fromEntries<number>([[1, 1], [3, 1]]),
    ], ZSet.zero<number>());
    
    // Since both are linear, composing then incrementalizing = incrementalizing then composing
    const incComposed = incrementalize(composed, group, group);
    const composedInc = compose(
      incrementalize(mapper, group, group),
      incrementalize(filter, group, group)
    );
    
    const result1 = incComposed(deltas);
    const result2 = composedInc(deltas);
    
    for (let t = 0; t < deltas.length(); t++) {
      expect(result1.at(t).equals(result2.at(t))).toBe(true);
    }
  });
});

describe('Example: Incremental Query Processing', () => {
  it('should process database changes incrementally', () => {
    // Simulate a database table of (id, value) pairs
    interface Record {
      id: number;
      value: number;
    }
    
    // Key function for records - use JSON.stringify for full object identity
    const keyFn = (r: Record) => JSON.stringify(r);
    
    // Initial state: empty
    // Transaction 1: Insert records
    const t1 = ZSet.fromEntries<Record>([
      [{ id: 1, value: 10 }, 1],
      [{ id: 2, value: 20 }, 1],
      [{ id: 3, value: 5 }, 1],
    ], keyFn);
    
    // Transaction 2: Update and delete
    const t2 = ZSet.fromEntries<Record>([
      [{ id: 1, value: 10 }, -1],  // delete old value
      [{ id: 1, value: 15 }, 1],   // insert new value
      [{ id: 3, value: 5 }, -1],   // delete
    ], keyFn);
    
    // Transaction 3: Insert new
    const t3 = ZSet.fromEntries<Record>([
      [{ id: 4, value: 25 }, 1],
    ], keyFn);
    
    // Stream of deltas (transactions)
    const transactions = Stream.from([t1, t2, t3], ZSet.zero<Record>(keyFn));
    
    // Define query: SELECT * FROM records WHERE value > 10
    const query = liftedFilter<Record>(r => r.value > 10);
    
    // Process incrementally
    const results = query(transactions);
    
    // Check results using the values() to find matching records
    // T1: {id:2, value:20} passes filter
    const t1Results = results.at(0).values();
    expect(t1Results.some(r => r.id === 2 && r.value === 20)).toBe(true);
    expect(t1Results.some(r => r.id === 1 && r.value === 10)).toBe(false);
    
    // T2: {id:1, value:15} is added to result
    const t2Results = results.at(1).values();
    expect(t2Results.some(r => r.id === 1 && r.value === 15)).toBe(true);
    
    // T3: {id:4, value:25} is added to result
    const t3Results = results.at(2).values();
    expect(t3Results.some(r => r.id === 4 && r.value === 25)).toBe(true);
    
    // Integrate to get cumulative view at each time
    const integrated = integrate(zsetGroup<Record>())(results);
    
    // After T1: {(2, 20)}
    // After T2: {(1, 15), (2, 20)}
    // After T3: {(1, 15), (2, 20), (4, 25)}
    expect(integrated.at(0).size()).toBe(1);
    expect(integrated.at(1).size()).toBe(2);
    expect(integrated.at(2).size()).toBe(3);
  });
});

