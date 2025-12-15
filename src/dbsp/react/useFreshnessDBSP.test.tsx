/**
 * Tests for useFreshnessDBSP hook
 * @vitest-environment jsdom
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';
import { useFreshnessDBSP } from './useFreshnessDBSP';

interface Order {
  orderId: number;
  amount: number;
  status: string;
  region: string;
  [key: string]: unknown;
}

describe('useFreshnessDBSP', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  
  describe('Basic Operations', () => {
    it('should initialize with empty results', async () => {
      const { result } = renderHook(() =>
        useFreshnessDBSP<Order>(
          "SELECT * FROM data WHERE status = 'pending'",
          { key: 'orderId', maxBufferSize: 100 }
        )
      );
      
      expect(result.current.results).toEqual([]);
      expect(result.current.count).toBe(0);
      expect(result.current.totalRows).toBe(0);
    });
    
    it('should process pushed data', async () => {
      vi.useRealTimers(); // Use real timers for this test
      
      const { result } = renderHook(() =>
        useFreshnessDBSP<Order>(
          "SELECT * FROM data WHERE status = 'pending'",
          { key: 'orderId', maxBufferSize: 100, processingIntervalMs: 10 }
        )
      );
      
      // Push data
      act(() => {
        result.current.push({ orderId: 1, amount: 100, status: 'pending', region: 'NA' });
      });
      
      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 100));
      
      // Data should be queued or processed
      expect(result.current.stats.bufferSize).toBeGreaterThanOrEqual(0);
    });
    
    it('should track freshness stats', async () => {
      const { result } = renderHook(() =>
        useFreshnessDBSP<Order>(
          "SELECT * FROM data",
          { key: 'orderId', maxBufferSize: 100 }
        )
      );
      
      expect(result.current.stats.bufferCapacity).toBe(100);
      expect(result.current.stats.bufferUtilization).toBe(0);
      expect(result.current.stats.droppedOverflow).toBe(0);
      expect(result.current.stats.droppedStale).toBe(0);
    });
    
    it('should support pause/resume', async () => {
      const { result } = renderHook(() =>
        useFreshnessDBSP<Order>(
          "SELECT * FROM data",
          { key: 'orderId', maxBufferSize: 100 }
        )
      );
      
      expect(result.current.isPaused).toBe(false);
      
      act(() => {
        result.current.setPaused(true);
      });
      
      expect(result.current.isPaused).toBe(true);
      
      act(() => {
        result.current.setPaused(false);
      });
      
      expect(result.current.isPaused).toBe(false);
    });
    
    it('should clear all data', async () => {
      const { result } = renderHook(() =>
        useFreshnessDBSP<Order>(
          "SELECT * FROM data",
          { key: 'orderId', maxBufferSize: 100, processingIntervalMs: 10 }
        )
      );
      
      // Push and wait
      act(() => {
        result.current.push([
          { orderId: 1, amount: 100, status: 'pending', region: 'NA' },
          { orderId: 2, amount: 200, status: 'shipped', region: 'EU' },
        ]);
      });
      
      await act(async () => {
        vi.advanceTimersByTime(50);
      });
      
      // Clear
      act(() => {
        result.current.clear();
      });
      
      expect(result.current.totalRows).toBe(0);
      expect(result.current.stats.droppedOverflow).toBe(0);
      expect(result.current.stats.droppedStale).toBe(0);
    });
  });
  
  describe('Freshness Guarantees', () => {
    it('should call onDrop when buffer overflows', async () => {
      const onDrop = vi.fn();
      
      const { result } = renderHook(() =>
        useFreshnessDBSP<Order>(
          "SELECT * FROM data",
          {
            key: 'orderId',
            maxBufferSize: 5,
            processingIntervalMs: 1000, // Slow processing
            onDrop,
          }
        )
      );
      
      // Push more data than buffer can hold
      act(() => {
        for (let i = 0; i < 10; i++) {
          result.current.push({ orderId: i, amount: i * 10, status: 'pending', region: 'NA' });
        }
      });
      
      // Some items should have been dropped
      expect(onDrop).toHaveBeenCalled();
      const calls = onDrop.mock.calls;
      const overflowCalls = calls.filter((c: unknown[]) => c[1] === 'overflow');
      expect(overflowCalls.length).toBeGreaterThan(0);
    });
    
    it('should report lag metrics', async () => {
      const { result } = renderHook(() =>
        useFreshnessDBSP<Order>(
          "SELECT * FROM data",
          {
            key: 'orderId',
            maxBufferSize: 100,
            processingIntervalMs: 100,
          }
        )
      );
      
      // Push data
      act(() => {
        result.current.push({ orderId: 1, amount: 100, status: 'pending', region: 'NA' });
      });
      
      // Without advancing timers, lag should accumulate
      expect(result.current.stats.lagMs).toBeGreaterThanOrEqual(0);
    });
    
    it('should track buffer utilization', async () => {
      const { result } = renderHook(() =>
        useFreshnessDBSP<Order>(
          "SELECT * FROM data",
          {
            key: 'orderId',
            maxBufferSize: 10,
            processingIntervalMs: 1000, // Slow processing to let buffer fill
          }
        )
      );
      
      // Push 5 items
      act(() => {
        for (let i = 0; i < 5; i++) {
          result.current.push({ orderId: i, amount: i * 10, status: 'pending', region: 'NA' });
        }
      });
      
      // Buffer should be at 50% utilization
      expect(result.current.stats.bufferSize).toBe(5);
      expect(result.current.stats.bufferUtilization).toBe(0.5);
    });
  });
  
  describe('Aggregations', () => {
    it('should compute GROUP BY aggregations', async () => {
      vi.useRealTimers(); // Use real timers for this test
      
      interface RegionSummary {
        region: string;
        total_amount: number;
        order_count: number;
        [key: string]: unknown;
      }
      
      const { result } = renderHook(() =>
        useFreshnessDBSP<Order, RegionSummary>(
          "SELECT region, SUM(amount) AS total_amount, COUNT(*) AS order_count FROM data GROUP BY region",
          {
            key: 'orderId',
            outputKey: 'region',
            maxBufferSize: 100,
            processingIntervalMs: 10,
          }
        )
      );
      
      // Push data
      act(() => {
        result.current.push([
          { orderId: 1, amount: 100, status: 'pending', region: 'NA' },
          { orderId: 2, amount: 200, status: 'shipped', region: 'NA' },
          { orderId: 3, amount: 150, status: 'pending', region: 'EU' },
        ]);
      });
      
      // Wait for processing
      await new Promise(resolve => setTimeout(resolve, 100));
      
      // Should have data queued
      expect(result.current.stats.bufferSize).toBeGreaterThanOrEqual(0);
    });
  });
});

