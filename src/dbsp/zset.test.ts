import { describe, it, expect } from 'vitest';
import { ZSet, cartesianProduct, join } from './zset';

describe('ZSet', () => {
  describe('construction', () => {
    it('should create an empty ZSet', () => {
      const zset = ZSet.zero<number>();
      expect(zset.isZero()).toBe(true);
      expect(zset.size()).toBe(0);
    });

    it('should create ZSet from entries', () => {
      const zset = ZSet.fromEntries<number>([
        [1, 2],
        [2, 3],
        [3, -1],
      ]);
      expect(zset.getWeight(1)).toBe(2);
      expect(zset.getWeight(2)).toBe(3);
      expect(zset.getWeight(3)).toBe(-1);
      expect(zset.getWeight(4)).toBe(0); // Not present
    });

    it('should create ZSet from values (as set)', () => {
      const zset = ZSet.fromValues([1, 2, 3]);
      expect(zset.getWeight(1)).toBe(1);
      expect(zset.getWeight(2)).toBe(1);
      expect(zset.getWeight(3)).toBe(1);
      expect(zset.isSet()).toBe(true);
    });

    it('should ignore zero weights', () => {
      const zset = ZSet.fromEntries<number>([
        [1, 0],
        [2, 1],
      ]);
      expect(zset.has(1)).toBe(false);
      expect(zset.has(2)).toBe(true);
    });
  });

  describe('group operations', () => {
    it('should add two ZSets (pointwise)', () => {
      const a = ZSet.fromEntries<number>([[1, 2], [2, 3]]);
      const b = ZSet.fromEntries<number>([[1, 1], [3, 4]]);
      const c = a.add(b);
      
      expect(c.getWeight(1)).toBe(3); // 2 + 1
      expect(c.getWeight(2)).toBe(3); // 3 + 0
      expect(c.getWeight(3)).toBe(4); // 0 + 4
    });

    it('should negate ZSet', () => {
      const a = ZSet.fromEntries<number>([[1, 2], [2, -3]]);
      const neg = a.negate();
      
      expect(neg.getWeight(1)).toBe(-2);
      expect(neg.getWeight(2)).toBe(3);
    });

    it('should subtract ZSets', () => {
      const a = ZSet.fromEntries<number>([[1, 5], [2, 3]]);
      const b = ZSet.fromEntries<number>([[1, 2], [3, 1]]);
      const c = a.subtract(b);
      
      expect(c.getWeight(1)).toBe(3);  // 5 - 2
      expect(c.getWeight(2)).toBe(3);  // 3 - 0
      expect(c.getWeight(3)).toBe(-1); // 0 - 1
    });

    it('should satisfy group axioms', () => {
      const a = ZSet.fromEntries<number>([[1, 2], [2, 3]]);
      const b = ZSet.fromEntries<number>([[1, 1], [3, 4]]);
      const zero = ZSet.zero<number>();

      // Identity: a + 0 = a
      expect(a.add(zero).equals(a)).toBe(true);

      // Inverse: a + (-a) = 0
      expect(a.add(a.negate()).isZero()).toBe(true);

      // Commutativity: a + b = b + a
      expect(a.add(b).equals(b.add(a))).toBe(true);
    });

    it('cancellation removes elements with zero weight', () => {
      const a = ZSet.fromEntries<number>([[1, 2], [2, 3]]);
      const b = ZSet.fromEntries<number>([[1, -2], [3, 1]]);
      const c = a.add(b);
      
      expect(c.has(1)).toBe(false); // 2 + (-2) = 0, removed
      expect(c.getWeight(2)).toBe(3);
      expect(c.getWeight(3)).toBe(1);
    });
  });

  describe('linear operators', () => {
    it('filter should be linear', () => {
      const a = ZSet.fromEntries<number>([[1, 2], [2, 3], [3, 1]]);
      const b = ZSet.fromEntries<number>([[2, 1], [4, 2]]);
      const pred = (x: number) => x % 2 === 0;

      // filter(a + b) = filter(a) + filter(b)
      const left = a.add(b).filter(pred);
      const right = a.filter(pred).add(b.filter(pred));

      expect(left.equals(right)).toBe(true);
    });

    it('filter should preserve weights', () => {
      const zset = ZSet.fromEntries<number>([[1, 2], [2, 3], [3, -1]]);
      const filtered = zset.filter(x => x >= 2);
      
      expect(filtered.has(1)).toBe(false);
      expect(filtered.getWeight(2)).toBe(3);
      expect(filtered.getWeight(3)).toBe(-1);
    });

    it('map should be linear', () => {
      const a = ZSet.fromEntries<number>([[1, 2], [2, 3]]);
      const b = ZSet.fromEntries<number>([[1, 1], [3, 1]]);
      const fn = (x: number) => x * 2;

      // map(a + b) = map(a) + map(b)
      const left = a.add(b).map(fn);
      const right = a.map(fn).add(b.map(fn));

      expect(left.equals(right)).toBe(true);
    });

    it('map should combine weights for collisions', () => {
      const zset = ZSet.fromEntries<number>([[1, 2], [2, 3]]);
      const mapped = zset.map(x => x % 2); // Both map to same values
      
      // 1 % 2 = 1 with weight 2
      // 2 % 2 = 0 with weight 3
      expect(mapped.getWeight(1)).toBe(2);
      expect(mapped.getWeight(0)).toBe(3);
    });
  });

  describe('aggregation', () => {
    it('count should sum weights', () => {
      const zset = ZSet.fromEntries<number>([[1, 2], [2, 3], [3, -1]]);
      expect(zset.count()).toBe(4); // 2 + 3 + (-1) = 4
    });

    it('sum should compute weighted sum', () => {
      const zset = ZSet.fromEntries<number>([[10, 2], [20, 3]]);
      // 10 * 2 + 20 * 3 = 20 + 60 = 80
      expect(zset.sum(x => x)).toBe(80);
    });

    it('reduce should work with custom aggregation', () => {
      const zset = ZSet.fromEntries<number>([[1, 2], [2, 3], [3, 1]]);
      const max = zset.reduce(
        (acc, val, weight) => (weight > 0 ? Math.max(acc, val) : acc),
        -Infinity
      );
      expect(max).toBe(3);
    });
  });

  describe('distinct', () => {
    it('should convert to set (all weights become 1)', () => {
      const zset = ZSet.fromEntries<number>([[1, 5], [2, 3], [3, -1]]);
      const distinct = zset.distinct();
      
      expect(distinct.getWeight(1)).toBe(1);
      expect(distinct.getWeight(2)).toBe(1);
      expect(distinct.has(3)).toBe(false); // Negative weight removed
      expect(distinct.isSet()).toBe(true);
    });
  });

  describe('cartesian product (bilinear)', () => {
    it('should compute product with weight multiplication', () => {
      const a = ZSet.fromEntries<string>([['x', 2], ['y', 3]]);
      const b = ZSet.fromEntries<number>([[1, 1], [2, 2]]);
      const product = cartesianProduct(a, b);
      
      expect(product.getWeight(['x', 1])).toBe(2);  // 2 * 1
      expect(product.getWeight(['x', 2])).toBe(4);  // 2 * 2
      expect(product.getWeight(['y', 1])).toBe(3);  // 3 * 1
      expect(product.getWeight(['y', 2])).toBe(6);  // 3 * 2
    });

    it('cartesian product should be bilinear', () => {
      const a = ZSet.fromEntries<number>([[1, 1]]);
      const b = ZSet.fromEntries<number>([[2, 1]]);
      const c = ZSet.fromEntries<number>([[10, 1]]);

      // (a + b) × c = a × c + b × c (right distributive)
      const left = cartesianProduct(a.add(b), c);
      const right = cartesianProduct(a, c).add(cartesianProduct(b, c));
      
      expect(left.equals(right)).toBe(true);
    });
  });

  describe('join (bilinear)', () => {
    it('should join on matching keys', () => {
      interface Person { name: string; deptId: number }
      interface Dept { id: number; name: string }

      const people = ZSet.fromEntries<Person>([
        [{ name: 'Alice', deptId: 1 }, 1],
        [{ name: 'Bob', deptId: 1 }, 1],
        [{ name: 'Carol', deptId: 2 }, 1],
      ], JSON.stringify);

      const depts = ZSet.fromEntries<Dept>([
        [{ id: 1, name: 'Engineering' }, 1],
        [{ id: 2, name: 'Sales' }, 1],
      ], JSON.stringify);

      const joined = join(
        people,
        depts,
        p => p.deptId,
        d => d.id
      );

      expect(joined.size()).toBe(3);
      expect(joined.has([{ name: 'Alice', deptId: 1 }, { id: 1, name: 'Engineering' }])).toBe(true);
      expect(joined.has([{ name: 'Bob', deptId: 1 }, { id: 1, name: 'Engineering' }])).toBe(true);
      expect(joined.has([{ name: 'Carol', deptId: 2 }, { id: 2, name: 'Sales' }])).toBe(true);
    });
  });
});

