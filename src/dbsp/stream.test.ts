import { describe, it, expect } from 'vitest';
import { Stream, lift, lift2 } from './stream';

describe('Stream', () => {
  describe('construction', () => {
    it('should create stream from values', () => {
      const s = Stream.from([1, 2, 3, 4], 0);
      expect(s.at(0)).toBe(1);
      expect(s.at(1)).toBe(2);
      expect(s.at(2)).toBe(3);
      expect(s.at(3)).toBe(4);
    });

    it('should return zero for times beyond stored values', () => {
      const s = Stream.from([1, 2], 0);
      expect(s.at(5)).toBe(0);
      expect(s.at(100)).toBe(0);
    });

    it('should create empty stream', () => {
      const s = Stream.empty<number>(0);
      expect(s.length()).toBe(0);
      expect(s.at(0)).toBe(0);
    });

    it('should create constant stream', () => {
      const s = Stream.constant(5, 4, 0);
      expect(s.getValues()).toEqual([5, 5, 5, 5]);
    });
  });

  describe('lift', () => {
    it('should apply function pointwise to stream', () => {
      const s = Stream.from([1, 2, 3], 0);
      const doubled = lift((x: number) => x * 2, 0)(s);
      
      expect(doubled.getValues()).toEqual([2, 4, 6]);
    });

    it('should work with different input/output types', () => {
      const s = Stream.from([1, 2, 3], 0);
      const asStrings = lift((x: number) => x.toString(), '')(s);
      
      expect(asStrings.getValues()).toEqual(['1', '2', '3']);
    });

    it('lift should distribute over composition', () => {
      const s = Stream.from([1, 2, 3], 0);
      const f = (x: number) => x * 2;
      const g = (x: number) => x + 1;
      
      // ↑(f ∘ g) = (↑f) ∘ (↑g)
      const composed = lift((x: number) => f(g(x)), 0)(s);
      const separate = lift(f, 0)(lift(g, 0)(s));
      
      expect(composed.getValues()).toEqual(separate.getValues());
    });
  });

  describe('lift2 (binary)', () => {
    it('should apply binary function pointwise', () => {
      const a = Stream.from([1, 2, 3], 0);
      const b = Stream.from([10, 20, 30], 0);
      const sum = lift2((x: number, y: number) => x + y, 0)(a, b);
      
      expect(sum.getValues()).toEqual([11, 22, 33]);
    });

    it('should handle streams of different lengths', () => {
      const a = Stream.from([1, 2, 3, 4], 0);
      const b = Stream.from([10, 20], 0);
      const sum = lift2((x: number, y: number) => x + y, 0)(a, b);
      
      // Shorter stream returns zero beyond its length
      expect(sum.getValues()).toEqual([11, 22, 3, 4]);
    });
  });
});

