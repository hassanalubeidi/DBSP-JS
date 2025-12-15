import { describe, it, expect } from 'vitest';
import {
  filterExample,
  mapExample,
  aggregationExample,
  distinctExample,
  pipelineExample,
  circuitExample,
} from './examples';

describe('DBSP Examples', () => {
  describe('filterExample', () => {
    it('should correctly filter values > 5', () => {
      const { integratedView } = filterExample();
      
      // After t=0: {7, 10}
      expect(integratedView.at(0).values()).toEqual(expect.arrayContaining([7, 10]));
      expect(integratedView.at(0).has(3)).toBe(false);
      
      // After t=1: {7, 8, 10}
      expect(integratedView.at(1).values()).toEqual(expect.arrayContaining([7, 8, 10]));
      
      // After t=2: {8, 10, 15}
      expect(integratedView.at(2).values()).toEqual(expect.arrayContaining([8, 10, 15]));
      expect(integratedView.at(2).has(7)).toBe(false);
    });
  });

  describe('mapExample', () => {
    it('should double all values', () => {
      const { mappedDeltas, integratedView } = mapExample();
      
      // First delta doubles [1, 2, 3] → [2, 4, 6]
      expect(mappedDeltas.at(0).values()).toEqual(expect.arrayContaining([2, 4, 6]));
      
      // Final view should have [2, 6, 8, 10] (4 was deleted when 2 was deleted)
      expect(integratedView.at(2).values()).toEqual(expect.arrayContaining([2, 6, 8, 10]));
    });
  });

  describe('aggregationExample', () => {
    it('should compute running sum correctly', () => {
      const { sums } = aggregationExample();
      
      expect(sums[0]).toBe(60);  // 10 + 20 + 30
      expect(sums[1]).toBe(70);  // 60 + 10
      expect(sums[2]).toBe(75);  // 70 + 5
    });
  });

  describe('distinctExample', () => {
    it('should track distinct set correctly', () => {
      const { steps } = distinctExample();
      
      // Step 1: both 'a' and 'b' enter
      expect(steps[0].getWeight('a')).toBe(1);
      expect(steps[0].getWeight('b')).toBe(1);
      
      // Step 2: no change (a already in)
      expect(steps[1].getWeight('a')).toBe(0);
      
      // Step 3: no change (a still has weight 1)
      expect(steps[2].getWeight('a')).toBe(0);
      
      // Step 4: a removed
      expect(steps[3].getWeight('a')).toBe(-1);
    });
  });

  describe('pipelineExample', () => {
    it('should compose filter and map correctly', () => {
      const { view } = pipelineExample();
      
      // After all operations: {4, 14, 20}
      expect(view.at(2).values()).toEqual(expect.arrayContaining([4, 14, 20]));
      expect(view.at(2).has(6)).toBe(false); // 3 was deleted, so 6 should be gone
    });
  });

  describe('circuitExample', () => {
    it('should track high-value pending orders', () => {
      const { results } = circuitExample();
      
      // T1: Only Bob's order matches
      expect(results[0].length).toBe(1);
      expect(results[0][0].customer).toBe('Bob');
      
      // T2: Bob's order shipped, no matches
      expect(results[1].length).toBe(0);
      
      // T3: Dave's order matches
      expect(results[2].length).toBe(1);
      expect(results[2][0].customer).toBe('Dave');
    });
  });
});

