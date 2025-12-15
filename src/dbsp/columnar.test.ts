/**
 * Columnar Storage Tests
 * 
 * Tests correctness and benchmarks performance of columnar vs row-based storage.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { ColumnarTable, ColumnarZSet, inferSchema, type TableSchema } from './columnar';
import { ZSet } from './zset';

// ============ TEST DATA ============

interface Order {
  id: number;
  customerId: number;
  price: number;
  quantity: number;
  status: string;
  region: string;
}

const orderSchema: TableSchema = {
  columns: [
    { name: 'id', type: 'int32' },
    { name: 'customerId', type: 'int32' },
    { name: 'price', type: 'float64' },
    { name: 'quantity', type: 'int32' },
    { name: 'status', type: 'string' },
    { name: 'region', type: 'string' },
  ]
};

function generateOrders(count: number): Order[] {
  const statuses = ['pending', 'processing', 'shipped', 'delivered'];
  const regions = ['NA', 'EU', 'APAC', 'LATAM'];
  
  const orders: Order[] = [];
  for (let i = 0; i < count; i++) {
    orders.push({
      id: i + 1,
      customerId: Math.floor(Math.random() * 10000) + 1,
      price: Math.random() * 100,
      quantity: Math.floor(Math.random() * 10) + 1,
      status: statuses[i % statuses.length],
      region: regions[i % regions.length],
    });
  }
  return orders;
}

// ============ CORRECTNESS TESTS ============

describe('ColumnarTable', () => {
  describe('Basic Operations', () => {
    it('should insert and retrieve data', () => {
      const table = new ColumnarTable(orderSchema);
      
      table.insert({ id: 1, customerId: 100, price: 50.0, quantity: 2, status: 'pending', region: 'NA' });
      table.insert({ id: 2, customerId: 200, price: 75.0, quantity: 3, status: 'shipped', region: 'EU' });
      
      expect(table.size).toBe(2);
      
      const idCol = table.getColumn<Int32Array>('id');
      expect(idCol[0]).toBe(1);
      expect(idCol[1]).toBe(2);
      
      const priceCol = table.getColumn<Float64Array>('price');
      expect(priceCol[0]).toBe(50.0);
      expect(priceCol[1]).toBe(75.0);
    });

    it('should bulk insert efficiently', () => {
      const table = new ColumnarTable(orderSchema);
      const orders = generateOrders(1000);
      
      table.bulkInsert(orders);
      
      expect(table.size).toBe(1000);
    });

    it('should grow capacity automatically', () => {
      const table = new ColumnarTable(orderSchema, 4); // Start small
      const orders = generateOrders(100);
      
      table.bulkInsert(orders);
      
      expect(table.size).toBe(100);
    });
  });

  describe('Aggregations', () => {
    it('should compute COUNT correctly', () => {
      const table = new ColumnarTable(orderSchema);
      table.insert({ id: 1, customerId: 100, price: 50.0, quantity: 2, status: 'pending', region: 'NA' }, 1);
      table.insert({ id: 2, customerId: 200, price: 75.0, quantity: 3, status: 'shipped', region: 'EU' }, 2);
      table.insert({ id: 3, customerId: 300, price: 25.0, quantity: 1, status: 'pending', region: 'NA' }, -1);
      
      expect(table.count()).toBe(2); // 1 + 2 - 1 = 2
    });

    it('should compute SUM correctly with weights', () => {
      const table = new ColumnarTable(orderSchema);
      table.insert({ id: 1, customerId: 100, price: 50.0, quantity: 2, status: 'pending', region: 'NA' }, 1);
      table.insert({ id: 2, customerId: 200, price: 100.0, quantity: 3, status: 'shipped', region: 'EU' }, 2);
      
      // SUM(price) = 50*1 + 100*2 = 250
      expect(table.sum('price')).toBe(250);
    });

    it('should compute AVG correctly', () => {
      const table = new ColumnarTable(orderSchema);
      table.insert({ id: 1, customerId: 100, price: 50.0, quantity: 2, status: 'pending', region: 'NA' }, 1);
      table.insert({ id: 2, customerId: 200, price: 100.0, quantity: 3, status: 'shipped', region: 'EU' }, 1);
      
      // AVG(price) = (50 + 100) / 2 = 75
      expect(table.avg('price')).toBe(75);
    });

    it('should compute MIN correctly', () => {
      const table = new ColumnarTable(orderSchema);
      table.insert({ id: 1, customerId: 100, price: 50.0, quantity: 2, status: 'pending', region: 'NA' }, 1);
      table.insert({ id: 2, customerId: 200, price: 25.0, quantity: 3, status: 'shipped', region: 'EU' }, 1);
      table.insert({ id: 3, customerId: 300, price: 75.0, quantity: 1, status: 'pending', region: 'NA' }, 1);
      
      expect(table.min('price')).toBe(25);
    });

    it('should compute MAX correctly', () => {
      const table = new ColumnarTable(orderSchema);
      table.insert({ id: 1, customerId: 100, price: 50.0, quantity: 2, status: 'pending', region: 'NA' }, 1);
      table.insert({ id: 2, customerId: 200, price: 25.0, quantity: 3, status: 'shipped', region: 'EU' }, 1);
      table.insert({ id: 3, customerId: 300, price: 75.0, quantity: 1, status: 'pending', region: 'NA' }, 1);
      
      expect(table.max('price')).toBe(75);
    });
  });

  describe('Bitmap Filtering', () => {
    it('should create numeric masks correctly', () => {
      const table = new ColumnarTable(orderSchema);
      table.insert({ id: 1, customerId: 100, price: 25.0, quantity: 2, status: 'pending', region: 'NA' });
      table.insert({ id: 2, customerId: 200, price: 50.0, quantity: 3, status: 'shipped', region: 'EU' });
      table.insert({ id: 3, customerId: 300, price: 75.0, quantity: 1, status: 'pending', region: 'NA' });
      
      const gtMask = table.createMaskNumeric('price', '>', 30);
      expect(Array.from(gtMask)).toEqual([0, 1, 1]);
      
      const betweenMask = table.createMaskNumeric('price', 'between', 30, 60);
      expect(Array.from(betweenMask)).toEqual([0, 1, 0]);
    });

    it('should create string masks correctly', () => {
      const table = new ColumnarTable(orderSchema);
      table.insert({ id: 1, customerId: 100, price: 25.0, quantity: 2, status: 'pending', region: 'NA' });
      table.insert({ id: 2, customerId: 200, price: 50.0, quantity: 3, status: 'shipped', region: 'EU' });
      table.insert({ id: 3, customerId: 300, price: 75.0, quantity: 1, status: 'pending', region: 'APAC' });
      
      const eqMask = table.createMaskString('status', '=', 'pending');
      expect(Array.from(eqMask)).toEqual([1, 0, 1]);
      
      const inMask = table.createMaskString('region', 'in', ['NA', 'EU']);
      expect(Array.from(inMask)).toEqual([1, 1, 0]);
      
      const likeMask = table.createMaskString('region', 'like', 'A%');
      expect(Array.from(likeMask)).toEqual([0, 0, 1]); // APAC starts with A
    });

    it('should combine masks with AND/OR', () => {
      const a = new Uint8Array([1, 1, 0, 0]);
      const b = new Uint8Array([1, 0, 1, 0]);
      
      expect(Array.from(ColumnarTable.andMasks(a, b))).toEqual([1, 0, 0, 0]);
      expect(Array.from(ColumnarTable.orMasks(a, b))).toEqual([1, 1, 1, 0]);
      expect(Array.from(ColumnarTable.notMask(a))).toEqual([0, 0, 1, 1]);
    });

    it('should compute masked aggregations', () => {
      const table = new ColumnarTable(orderSchema);
      table.insert({ id: 1, customerId: 100, price: 25.0, quantity: 2, status: 'pending', region: 'NA' }, 1);
      table.insert({ id: 2, customerId: 200, price: 50.0, quantity: 3, status: 'shipped', region: 'EU' }, 1);
      table.insert({ id: 3, customerId: 300, price: 75.0, quantity: 1, status: 'pending', region: 'NA' }, 1);
      
      const pendingMask = table.createMaskString('status', '=', 'pending');
      
      expect(table.countMasked(pendingMask)).toBe(2);
      expect(table.sumMasked('price', pendingMask)).toBe(100); // 25 + 75
      expect(table.avgMasked('price', pendingMask)).toBe(50); // 100 / 2
    });

    it('should filter table using mask', () => {
      const table = new ColumnarTable(orderSchema);
      table.insert({ id: 1, customerId: 100, price: 25.0, quantity: 2, status: 'pending', region: 'NA' });
      table.insert({ id: 2, customerId: 200, price: 50.0, quantity: 3, status: 'shipped', region: 'EU' });
      table.insert({ id: 3, customerId: 300, price: 75.0, quantity: 1, status: 'pending', region: 'NA' });
      
      const mask = table.createMaskString('status', '=', 'pending');
      const filtered = table.filter(mask);
      
      expect(filtered.size).toBe(2);
      expect(filtered.getColumn<Int32Array>('id')[0]).toBe(1);
      expect(filtered.getColumn<Int32Array>('id')[1]).toBe(3);
    });
  });

  describe('Conversion', () => {
    it('should convert from ZSet', () => {
      const zset = new ZSet<Order>((o) => o.id.toString());
      zset.insert({ id: 1, customerId: 100, price: 50.0, quantity: 2, status: 'pending', region: 'NA' });
      zset.insert({ id: 2, customerId: 200, price: 75.0, quantity: 3, status: 'shipped', region: 'EU' });
      
      const table = ColumnarTable.fromZSet(zset, orderSchema);
      
      expect(table.size).toBe(2);
      expect(table.sum('price')).toBe(125);
    });

    it('should convert to ZSet', () => {
      const table = new ColumnarTable(orderSchema);
      table.insert({ id: 1, customerId: 100, price: 50.0, quantity: 2, status: 'pending', region: 'NA' });
      table.insert({ id: 2, customerId: 200, price: 75.0, quantity: 3, status: 'shipped', region: 'EU' });
      
      const zset = table.toZSet<Order>((o) => o.id.toString());
      
      expect(zset.size()).toBe(2);
      expect(zset.sum((o) => o.price)).toBe(125);
    });
  });
});

describe('ColumnarZSet', () => {
  it('should work with DBSP-style operations', () => {
    const czset = new ColumnarZSet<Order>(orderSchema, (o) => o.id.toString());
    
    czset.insert({ id: 1, customerId: 100, price: 50.0, quantity: 2, status: 'pending', region: 'NA' });
    czset.insert({ id: 2, customerId: 200, price: 75.0, quantity: 3, status: 'shipped', region: 'EU' });
    
    expect(czset.count()).toBe(2);
    expect(czset.sum('price')).toBe(125);
  });

  it('should filter with optimized column predicates', () => {
    const czset = new ColumnarZSet<Order>(orderSchema, (o) => o.id.toString());
    
    czset.bulkInsert([
      { id: 1, customerId: 100, price: 25.0, quantity: 2, status: 'pending', region: 'NA' },
      { id: 2, customerId: 200, price: 50.0, quantity: 3, status: 'shipped', region: 'EU' },
      { id: 3, customerId: 300, price: 75.0, quantity: 1, status: 'pending', region: 'NA' },
    ]);
    
    const highValue = czset.filterNumeric('price', '>', 30);
    expect(highValue.count()).toBe(2);
    
    const pending = czset.filterString('status', '=', 'pending');
    expect(pending.count()).toBe(2);
  });
});

describe('Schema Inference', () => {
  it('should infer schema from sample row', () => {
    const sample = {
      id: 1,
      name: 'test',
      price: 50.5,
      active: true,
    };
    
    const schema = inferSchema(sample);
    
    expect(schema.columns).toHaveLength(4);
    expect(schema.columns.find(c => c.name === 'id')?.type).toBe('int32');
    expect(schema.columns.find(c => c.name === 'name')?.type).toBe('string');
    expect(schema.columns.find(c => c.name === 'price')?.type).toBe('float64');
    expect(schema.columns.find(c => c.name === 'active')?.type).toBe('boolean');
  });
});

// ============ PERFORMANCE BENCHMARKS ============

describe('Columnar Performance Benchmarks', { timeout: 60000 }, () => {
  const SIZES = [10_000, 100_000, 1_000_000];
  
  for (const size of SIZES) {
    describe(`${(size / 1000).toLocaleString()}K rows`, () => {
      let orders: Order[];
      let rowZSet: ZSet<Order>;
      let columnarTable: ColumnarTable;
      
      beforeAll(() => {
        console.log(`\n📊 Generating ${size.toLocaleString()} orders...`);
        orders = generateOrders(size);
        
        // Build row-based ZSet
        const rowStart = performance.now();
        rowZSet = ZSet.fromValues(orders, (o) => o.id.toString());
        const rowTime = performance.now() - rowStart;
        console.log(`   Row ZSet build: ${rowTime.toFixed(2)}ms`);
        
        // Build columnar table
        const colStart = performance.now();
        columnarTable = new ColumnarTable(orderSchema, size);
        columnarTable.bulkInsert(orders);
        const colTime = performance.now() - colStart;
        console.log(`   Columnar build: ${colTime.toFixed(2)}ms`);
      });

      it('COUNT(*) should be faster with columnar', () => {
        // Row-based
        const rowStart = performance.now();
        const rowCount = rowZSet.count();
        const rowTime = performance.now() - rowStart;
        
        // Columnar
        const colStart = performance.now();
        const colCount = columnarTable.count();
        const colTime = performance.now() - colStart;
        
        expect(rowCount).toBe(colCount);
        
        const speedup = rowTime / colTime;
        console.log(`   COUNT(*): Row=${rowTime.toFixed(2)}ms, Col=${colTime.toFixed(2)}ms, Speedup=${speedup.toFixed(1)}x`);
      });

      it('SUM(price) should be faster with columnar', () => {
        // Row-based
        const rowStart = performance.now();
        const rowSum = rowZSet.sum((o) => o.price);
        const rowTime = performance.now() - rowStart;
        
        // Columnar
        const colStart = performance.now();
        const colSum = columnarTable.sum('price');
        const colTime = performance.now() - colStart;
        
        expect(Math.abs(rowSum - colSum)).toBeLessThan(0.01);
        
        const speedup = rowTime / colTime;
        console.log(`   SUM(price): Row=${rowTime.toFixed(2)}ms, Col=${colTime.toFixed(2)}ms, Speedup=${speedup.toFixed(1)}x`);
      });

      it('AVG(price) should be faster with columnar', () => {
        // Row-based
        const rowStart = performance.now();
        const rowSum = rowZSet.sum((o) => o.price);
        const rowCount = rowZSet.count();
        const rowAvg = rowSum / rowCount;
        const rowTime = performance.now() - rowStart;
        
        // Columnar
        const colStart = performance.now();
        const colAvg = columnarTable.avg('price');
        const colTime = performance.now() - colStart;
        
        expect(Math.abs(rowAvg - colAvg)).toBeLessThan(0.01);
        
        const speedup = rowTime / colTime;
        console.log(`   AVG(price): Row=${rowTime.toFixed(2)}ms, Col=${colTime.toFixed(2)}ms, Speedup=${speedup.toFixed(1)}x`);
      });

      it('Masked SUM (WHERE price > 50) should be faster with columnar', () => {
        // Row-based filter + sum
        const rowStart = performance.now();
        const filtered = rowZSet.filter((o) => o.price > 50);
        const rowSum = filtered.sum((o) => o.price);
        const rowTime = performance.now() - rowStart;
        
        // Columnar: create mask + masked sum
        const colStart = performance.now();
        const mask = columnarTable.createMaskNumeric('price', '>', 50);
        const colSum = columnarTable.sumMasked('price', mask);
        const colTime = performance.now() - colStart;
        
        expect(Math.abs(rowSum - colSum)).toBeLessThan(0.01);
        
        const speedup = rowTime / colTime;
        console.log(`   SUM WHERE price>50: Row=${rowTime.toFixed(2)}ms, Col=${colTime.toFixed(2)}ms, Speedup=${speedup.toFixed(1)}x`);
      });

      it('String filter (WHERE status = pending) should be fast', () => {
        // Row-based
        const rowStart = performance.now();
        const rowFiltered = rowZSet.filter((o) => o.status === 'pending');
        const rowCount = rowFiltered.count();
        const rowTime = performance.now() - rowStart;
        
        // Columnar
        const colStart = performance.now();
        const mask = columnarTable.createMaskString('status', '=', 'pending');
        const colCount = columnarTable.countMasked(mask);
        const colTime = performance.now() - colStart;
        
        expect(rowCount).toBe(colCount);
        
        const speedup = rowTime / colTime;
        console.log(`   COUNT WHERE status=pending: Row=${rowTime.toFixed(2)}ms, Col=${colTime.toFixed(2)}ms, Speedup=${speedup.toFixed(1)}x`);
      });
    });
  }
});

