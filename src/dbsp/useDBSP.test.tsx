/**
 * useDBSP Hook Tests
 * 
 * @vitest-environment jsdom
 */

import { describe, it, expect } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useDBSP, useDBSPQuery } from './useDBSP';

// ============ TEST DATA ============

interface Order {
  orderId: number;
  customerId: number;
  product: string;
  amount: number;
  status: string;
  region: string;
}

const sampleOrders: Order[] = [
  { orderId: 1, customerId: 100, product: 'Widget', amount: 50, status: 'pending', region: 'NA' },
  { orderId: 2, customerId: 100, product: 'Gadget', amount: 150, status: 'shipped', region: 'NA' },
  { orderId: 3, customerId: 200, product: 'Widget', amount: 75, status: 'pending', region: 'EU' },
  { orderId: 4, customerId: 300, product: 'Gizmo', amount: 200, status: 'delivered', region: 'APAC' },
  { orderId: 5, customerId: 200, product: 'Widget', amount: 25, status: 'cancelled', region: 'EU' },
];

// ============ TESTS ============

describe('useDBSP', () => {
  describe('Basic Operations', () => {
    it('should initialize with data', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          primaryKey: ['orderId'],
        })
      );

      expect(result.current.count).toBe(5);
      expect(result.current.rawData).toHaveLength(5);
    });

    it('should insert a new row', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          primaryKey: ['orderId'],
        })
      );

      act(() => {
        result.current.insert({
          orderId: 6,
          customerId: 400,
          product: 'NewProduct',
          amount: 300,
          status: 'pending',
          region: 'LATAM',
        });
      });

      expect(result.current.count).toBe(6);
      expect(result.current.exists({ orderId: 6 })).toBe(true);
    });

    it('should upsert (insert new)', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          primaryKey: ['orderId'],
        })
      );

      act(() => {
        result.current.upsert({
          orderId: 10,
          customerId: 500,
          product: 'UpsertNew',
          amount: 100,
          status: 'pending',
          region: 'NA',
        });
      });

      expect(result.current.count).toBe(6);
      expect(result.current.get({ orderId: 10 })?.product).toBe('UpsertNew');
    });

    it('should upsert (update existing)', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          primaryKey: ['orderId'],
        })
      );

      act(() => {
        result.current.upsert({
          orderId: 1,
          customerId: 100,
          product: 'UpdatedWidget',
          amount: 999,
          status: 'shipped',
          region: 'NA',
        });
      });

      expect(result.current.count).toBe(5); // Same count
      expect(result.current.get({ orderId: 1 })?.amount).toBe(999);
      expect(result.current.get({ orderId: 1 })?.product).toBe('UpdatedWidget');
    });

    it('should remove a row', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          primaryKey: ['orderId'],
        })
      );

      act(() => {
        result.current.remove({ orderId: 1 });
      });

      expect(result.current.count).toBe(4);
      expect(result.current.exists({ orderId: 1 })).toBe(false);
    });

    it('should removeWhere with predicate', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          primaryKey: ['orderId'],
        })
      );

      act(() => {
        result.current.removeWhere(o => o.status === 'pending');
      });

      expect(result.current.count).toBe(3);
      expect(result.current.rawData.every(o => o.status !== 'pending')).toBe(true);
    });

    it('should update a row', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          primaryKey: ['orderId'],
        })
      );

      act(() => {
        result.current.update({ orderId: 1 }, { status: 'shipped', amount: 60 });
      });

      const updated = result.current.get({ orderId: 1 });
      expect(updated?.status).toBe('shipped');
      expect(updated?.amount).toBe(60);
      expect(updated?.product).toBe('Widget'); // Unchanged
    });

    it('should clear all data', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          primaryKey: ['orderId'],
        })
      );

      act(() => {
        result.current.clear();
      });

      expect(result.current.count).toBe(0);
      expect(result.current.rawData).toHaveLength(0);
    });

    it('should setData to replace all', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          primaryKey: ['orderId'],
        })
      );

      const newData: Order[] = [
        { orderId: 100, customerId: 1, product: 'New', amount: 1, status: 'new', region: 'X' },
      ];

      act(() => {
        result.current.setData(newData);
      });

      expect(result.current.count).toBe(1);
      expect(result.current.get({ orderId: 100 })?.product).toBe('New');
    });
  });

  describe('Composite Primary Keys', () => {
    interface LineItem {
      orderId: number;
      lineNumber: number;
      productId: number;
      quantity: number;
    }

    const lineItems: LineItem[] = [
      { orderId: 1, lineNumber: 1, productId: 100, quantity: 2 },
      { orderId: 1, lineNumber: 2, productId: 200, quantity: 1 },
      { orderId: 2, lineNumber: 1, productId: 100, quantity: 5 },
    ];

    it('should support composite primary keys', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'line_items',
          initialData: lineItems,
          primaryKey: ['orderId', 'lineNumber'], // Composite key
        })
      );

      expect(result.current.count).toBe(3);
      expect(result.current.exists({ orderId: 1, lineNumber: 1 })).toBe(true);
      expect(result.current.exists({ orderId: 1, lineNumber: 3 })).toBe(false);
    });

    it('should upsert with composite key', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'line_items',
          initialData: lineItems,
          primaryKey: ['orderId', 'lineNumber'],
        })
      );

      act(() => {
        // Update existing
        result.current.upsert({ orderId: 1, lineNumber: 1, productId: 100, quantity: 10 });
      });

      expect(result.current.count).toBe(3);
      expect(result.current.get({ orderId: 1, lineNumber: 1 })?.quantity).toBe(10);

      act(() => {
        // Insert new
        result.current.upsert({ orderId: 1, lineNumber: 3, productId: 300, quantity: 1 });
      });

      expect(result.current.count).toBe(4);
    });

    it('should remove with composite key', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'line_items',
          initialData: lineItems,
          primaryKey: ['orderId', 'lineNumber'],
        })
      );

      act(() => {
        result.current.remove({ orderId: 1, lineNumber: 2 });
      });

      expect(result.current.count).toBe(2);
      expect(result.current.exists({ orderId: 1, lineNumber: 2 })).toBe(false);
    });
  });

  describe('SQL Transformations', () => {
    it('should apply SQL filter', async () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          sql: "SELECT * FROM orders WHERE status = 'pending'",
          primaryKey: ['orderId'],
        })
      );

      // Wait for async processing
      await new Promise(resolve => setTimeout(resolve, 50));

      expect(result.current.data.length).toBeLessThanOrEqual(result.current.rawData.length);
      expect(result.current.data.every(o => o.status === 'pending')).toBe(true);
    });

    it('should apply SQL with numeric filter', async () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          sql: 'SELECT * FROM orders WHERE amount > 100',
          primaryKey: ['orderId'],
        })
      );

      await new Promise(resolve => setTimeout(resolve, 50));

      expect(result.current.data.every(o => o.amount > 100)).toBe(true);
    });

    it('should apply SQL with compound conditions', async () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          sql: "SELECT * FROM orders WHERE status = 'pending' AND amount > 50",
          primaryKey: ['orderId'],
        })
      );

      await new Promise(resolve => setTimeout(resolve, 50));

      expect(result.current.data.every(o => o.status === 'pending' && o.amount > 50)).toBe(true);
    });
  });

  describe('Batch Operations', () => {
    it('should insertMany', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: [],
          primaryKey: ['orderId'],
        })
      );

      act(() => {
        result.current.insertMany(sampleOrders);
      });

      expect(result.current.count).toBe(5);
    });

    it('should upsertMany', () => {
      const { result } = renderHook(() =>
        useDBSP({
          tableName: 'orders',
          initialData: sampleOrders,
          primaryKey: ['orderId'],
        })
      );

      const updates: Order[] = [
        { orderId: 1, customerId: 100, product: 'Updated1', amount: 1000, status: 'shipped', region: 'NA' },
        { orderId: 10, customerId: 999, product: 'New10', amount: 500, status: 'pending', region: 'EU' },
      ];

      act(() => {
        result.current.upsertMany(updates);
      });

      expect(result.current.count).toBe(6); // 5 + 1 new
      expect(result.current.get({ orderId: 1 })?.product).toBe('Updated1');
      expect(result.current.get({ orderId: 10 })?.product).toBe('New10');
    });
  });
});

describe('useDBSPQuery', () => {
  it('should filter data with SQL', async () => {
    const { result } = renderHook(() =>
      useDBSPQuery(
        sampleOrders,
        "SELECT * FROM data WHERE status = 'pending'",
        ['orderId']
      )
    );

    await new Promise(resolve => setTimeout(resolve, 50));

    expect(result.current.every(o => o.status === 'pending')).toBe(true);
  });
});

