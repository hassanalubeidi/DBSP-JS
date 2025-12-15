/**
 * Async Stream Processing Tests
 * 
 * Tests for race conditions, ordering, correctness, and FRESHNESS.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { 
  AsyncQueue, 
  AsyncStreamProcessor, 
  AsyncBatcher,
  createMutex,
  CircularBuffer,
  FreshnessQueue,
  FreshnessStreamProcessor,
} from './async-stream';
import { ZSet } from './zset';

// ============ TEST DATA ============

interface Order {
  orderId: number;
  amount: number;
  region: string;
}

const keyFn = (o: Order) => String(o.orderId);

// ============ CIRCULAR BUFFER TESTS ============

describe('CircularBuffer', () => {
  it('should push and pop in FIFO order', () => {
    const buffer = new CircularBuffer<number>(5);
    
    buffer.push(1);
    buffer.push(2);
    buffer.push(3);
    
    expect(buffer.pop()).toBe(1);
    expect(buffer.pop()).toBe(2);
    expect(buffer.pop()).toBe(3);
    expect(buffer.pop()).toBeUndefined();
  });
  
  it('should drop oldest when full', () => {
    const dropped: number[] = [];
    const buffer = new CircularBuffer<number>(3, (item) => dropped.push(item));
    
    buffer.push(1);
    buffer.push(2);
    buffer.push(3);
    expect(buffer.isFull()).toBe(true);
    expect(dropped).toHaveLength(0);
    
    // This should drop 1
    buffer.push(4);
    expect(dropped).toEqual([1]);
    expect(buffer.size()).toBe(3);
    
    // Pop should return 2, 3, 4
    expect(buffer.pop()).toBe(2);
    expect(buffer.pop()).toBe(3);
    expect(buffer.pop()).toBe(4);
  });
  
  it('should track dropped count', () => {
    const buffer = new CircularBuffer<number>(2);
    
    buffer.push(1);
    buffer.push(2);
    buffer.push(3); // Drops 1
    buffer.push(4); // Drops 2
    buffer.push(5); // Drops 3
    
    expect(buffer.getDroppedCount()).toBe(3);
    expect(buffer.size()).toBe(2);
    expect(buffer.toArray()).toEqual([4, 5]);
  });
  
  it('should handle batch push', () => {
    const buffer = new CircularBuffer<number>(5);
    
    const dropped = buffer.pushBatch([1, 2, 3, 4, 5, 6, 7]);
    
    expect(dropped).toBe(2); // 1 and 2 were dropped
    expect(buffer.toArray()).toEqual([3, 4, 5, 6, 7]);
  });
  
  it('should handle batch pop', () => {
    const buffer = new CircularBuffer<number>(10);
    buffer.pushBatch([1, 2, 3, 4, 5]);
    
    const items = buffer.popBatch(3);
    expect(items).toEqual([1, 2, 3]);
    expect(buffer.size()).toBe(2);
  });
  
  it('should peek without removing', () => {
    const buffer = new CircularBuffer<number>(5);
    buffer.push(1);
    buffer.push(2);
    
    expect(buffer.peek()).toBe(1);
    expect(buffer.peek()).toBe(1);
    expect(buffer.size()).toBe(2);
  });
  
  it('should report utilization', () => {
    const buffer = new CircularBuffer<number>(10);
    
    expect(buffer.getUtilization()).toBe(0);
    
    buffer.pushBatch([1, 2, 3, 4, 5]);
    expect(buffer.getUtilization()).toBe(0.5);
    
    buffer.pushBatch([6, 7, 8, 9, 10]);
    expect(buffer.getUtilization()).toBe(1);
  });
  
  it('should wrap around correctly', () => {
    const buffer = new CircularBuffer<number>(3);
    
    buffer.push(1);
    buffer.push(2);
    buffer.push(3);
    buffer.pop(); // Remove 1
    buffer.push(4); // Should wrap
    
    expect(buffer.toArray()).toEqual([2, 3, 4]);
    
    buffer.pop(); // Remove 2
    buffer.push(5); // Wrap again
    
    expect(buffer.toArray()).toEqual([3, 4, 5]);
  });
});

// ============ FRESHNESS QUEUE TESTS ============

describe('FreshnessQueue', () => {
  it('should drop oldest messages when full', async () => {
    let droppedCount = 0;
    const queue = new FreshnessQueue<number>(
      3, // capacity
      undefined, // no age limit
      (count, reason) => {
        if (reason === 'overflow') droppedCount += count;
      }
    );
    
    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);
    queue.enqueue(4); // Drops 1
    queue.enqueue(5); // Drops 2
    
    expect(droppedCount).toBe(2);
    expect(queue.size()).toBe(3);
    
    const messages = await queue.dequeue(10);
    expect(messages.map(m => m.data)).toEqual([3, 4, 5]);
  });
  
  it('should drop stale messages', async () => {
    let staleDropped = 0;
    const queue = new FreshnessQueue<number>(
      100,
      50, // 50ms max age
      (count, reason) => {
        if (reason === 'stale') staleDropped += count;
      }
    );
    
    queue.enqueue(1);
    queue.enqueue(2);
    
    // Wait for messages to become stale
    await new Promise(resolve => setTimeout(resolve, 100));
    
    queue.enqueue(3); // Fresh message
    
    const messages = await queue.dequeue(10);
    
    expect(staleDropped).toBe(2);
    expect(messages.map(m => m.data)).toEqual([3]);
  });
  
  it('should report lag correctly', async () => {
    const queue = new FreshnessQueue<number>(100, undefined);
    
    queue.enqueue(1);
    await new Promise(resolve => setTimeout(resolve, 50));
    
    const lag = queue.getLag();
    expect(lag).toBeGreaterThanOrEqual(45);
    expect(lag).toBeLessThan(100);
  });
  
  it('should detect lagging condition', async () => {
    const queue = new FreshnessQueue<number>(100, undefined);
    
    queue.enqueue(1);
    expect(queue.isLagging(100)).toBe(false);
    
    await new Promise(resolve => setTimeout(resolve, 60));
    expect(queue.isLagging(50)).toBe(true);
  });
  
  it('should drop stale on demand', async () => {
    const queue = new FreshnessQueue<number>(100, 100);
    
    queue.enqueue(1);
    queue.enqueue(2);
    await new Promise(resolve => setTimeout(resolve, 60));
    queue.enqueue(3);
    
    const dropped = queue.dropStale(50);
    expect(dropped).toBe(2);
    expect(queue.size()).toBe(1);
  });
  
  it('should provide comprehensive stats', () => {
    const queue = new FreshnessQueue<number>(10, undefined);
    queue.enqueueBatch([1, 2, 3, 4, 5]);
    
    const stats = queue.getStats();
    
    expect(stats.size).toBe(5);
    expect(stats.capacity).toBe(10);
    expect(stats.utilization).toBe(0.5);
    expect(stats.lagMs).toBeGreaterThanOrEqual(0);
  });
});

// ============ FRESHNESS STREAM PROCESSOR TESTS ============

describe('FreshnessStreamProcessor', () => {
  it('should never lag behind - drops old data', async () => {
    const processed: number[] = [];
    const dropped: { count: number; reason: string }[] = [];
    
    const processor = new FreshnessStreamProcessor<number, number>(
      (input) => {
        for (const [value] of input.entries()) {
          processed.push(value);
        }
        return input;
      },
      String,
      {
        maxBufferSize: 5, // Very small buffer
        maxBatchSize: 2,
        maxBatchDelayMs: 10,
        onDrop: (count, reason) => dropped.push({ count, reason }),
      }
    );
    processor.start();
    
    // Push more data than buffer can hold
    for (let i = 0; i < 20; i++) {
      await processor.push(i);
    }
    
    await new Promise(resolve => setTimeout(resolve, 100));
    await processor.flush();
    await processor.stop();
    
    // Not all items should be processed (some dropped for freshness)
    const stats = processor.getStats();
    expect(stats.queueStats.totalDropped).toBeGreaterThan(0);
    
    // Processed items should be from the "fresher" end
    const maxProcessed = Math.max(...processed);
    expect(maxProcessed).toBe(19); // Most recent should be there
  });
  
  it('should drop stale messages based on age', async () => {
    const processed: number[] = [];
    let staleDropped = 0;
    
    // Create processor but DON'T start it yet - let messages become stale first
    const processor = new FreshnessStreamProcessor<number, number>(
      (input) => {
        for (const [value] of input.entries()) {
          processed.push(value);
        }
        return input;
      },
      String,
      {
        maxBufferSize: 100,
        maxMessageAgeMs: 30, // Very short age limit
        maxBatchSize: 10,
        maxBatchDelayMs: 5,
        onDrop: (count, reason) => {
          if (reason === 'stale') staleDropped += count;
        },
      }
    );
    
    // Push messages BEFORE starting (they'll sit in queue)
    await processor.push(1);
    await processor.push(2);
    
    // Wait for messages to become stale
    await new Promise(resolve => setTimeout(resolve, 60));
    
    // Now start - stale messages should be dropped when dequeued
    processor.start();
    
    // Push fresh message
    await processor.push(3);
    
    await new Promise(resolve => setTimeout(resolve, 50));
    await processor.flush();
    await processor.stop();
    
    // Stale messages should have been dropped
    expect(staleDropped).toBe(2);
    expect(processed).toContain(3);
  });
  
  it('should track lag metrics', async () => {
    const processor = new FreshnessStreamProcessor<number, number>(
      async (input) => {
        // Simulate slow processing
        await new Promise(resolve => setTimeout(resolve, 30));
        return input;
      },
      String,
      {
        maxBufferSize: 100,
        maxBatchSize: 1,
        maxBatchDelayMs: 1,
      }
    );
    processor.start();
    
    // Push many items quickly
    for (let i = 0; i < 10; i++) {
      await processor.push(i);
    }
    
    await new Promise(resolve => setTimeout(resolve, 100));
    
    const stats = processor.getStats();
    expect(stats.maxLagObserved).toBeGreaterThan(0);
    
    await processor.stop();
  });
  
  it('should report isLagging correctly', async () => {
    const processor = new FreshnessStreamProcessor<number, number>(
      async (input) => {
        await new Promise(resolve => setTimeout(resolve, 20));
        return input;
      },
      String,
      {
        maxBufferSize: 100,
        maxBatchSize: 1,
        maxBatchDelayMs: 5,
      }
    );
    processor.start();
    
    // Push items
    for (let i = 0; i < 5; i++) {
      await processor.push(i);
    }
    
    // Should be lagging due to slow processing
    await new Promise(resolve => setTimeout(resolve, 30));
    
    // After processing completes, should not be lagging
    await new Promise(resolve => setTimeout(resolve, 200));
    expect(processor.isLagging(500)).toBe(false);
    
    await processor.stop();
  });
  
  it('should handle pause/resume without accumulating lag', async () => {
    const processed: number[] = [];
    let dropCount = 0;
    
    const processor = new FreshnessStreamProcessor<number, number>(
      (input) => {
        for (const [value] of input.entries()) {
          processed.push(value);
        }
        return input;
      },
      String,
      {
        maxBufferSize: 10,
        maxMessageAgeMs: 100,
        maxBatchSize: 5,
        maxBatchDelayMs: 10,
        onDrop: (count) => { dropCount += count; },
      }
    );
    processor.start();
    
    // Push some data
    for (let i = 0; i < 5; i++) {
      await processor.push(i);
    }
    
    // Simulate "pause" by stopping
    await processor.stop();
    
    // Wait - data should become stale
    await new Promise(resolve => setTimeout(resolve, 150));
    
    // Resume
    processor.start();
    
    // Push fresh data
    for (let i = 100; i < 105; i++) {
      await processor.push(i);
    }
    
    await processor.flush();
    await processor.stop();
    
    // Old data should be dropped, fresh data processed
    expect(processed.filter(v => v >= 100)).toHaveLength(5);
  });
});

// ============ ASYNC QUEUE TESTS ============

describe('AsyncQueue', () => {
  it('should enqueue and dequeue in order', async () => {
    const queue = new AsyncQueue<number>();
    
    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);
    
    const messages = await queue.dequeue(3);
    
    expect(messages).toHaveLength(3);
    expect(messages[0].data).toBe(1);
    expect(messages[1].data).toBe(2);
    expect(messages[2].data).toBe(3);
    expect(messages[0].sequence).toBeLessThan(messages[1].sequence);
  });
  
  it('should handle batch enqueue atomically', async () => {
    const queue = new AsyncQueue<number>();
    
    const sequences = queue.enqueueBatch([1, 2, 3, 4, 5]);
    
    expect(sequences).toHaveLength(5);
    expect(sequences).toEqual([0, 1, 2, 3, 4]);
  });
  
  it('should wait for data when queue is empty', async () => {
    const queue = new AsyncQueue<number>();
    
    const dequeuePromise = queue.dequeue(1, 100);
    setTimeout(() => queue.enqueue(42), 20);
    
    const messages = await dequeuePromise;
    expect(messages).toHaveLength(1);
    expect(messages[0].data).toBe(42);
  });
  
  it('should handle concurrent enqueues', async () => {
    const queue = new AsyncQueue<number>();
    
    const promises = [];
    for (let i = 0; i < 100; i++) {
      promises.push(Promise.resolve().then(() => queue.enqueue(i)));
    }
    
    await Promise.all(promises);
    expect(queue.size()).toBe(100);
    
    const messages = await queue.dequeue(100);
    const values = messages.map(m => m.data).sort((a, b) => a - b);
    expect(values).toEqual(Array.from({ length: 100 }, (_, i) => i));
  });
});

// ============ ASYNC STREAM PROCESSOR TESTS ============

describe('AsyncStreamProcessor', () => {
  let processor: AsyncStreamProcessor<Order, Order>;
  
  beforeEach(() => {
    processor = new AsyncStreamProcessor(
      (input) => input,
      keyFn,
      { maxBatchSize: 10, maxBatchDelayMs: 5 }
    );
    processor.start();
  });
  
  afterEach(async () => {
    await processor.stop();
  });
  
  it('should process single items', async () => {
    const results: ZSet<Order>[] = [];
    processor.onOutput((result) => {
      results.push(result);
    });
    
    await processor.pushAndWait({ orderId: 1, amount: 100, region: 'NA' });
    await new Promise(resolve => setTimeout(resolve, 20));
    
    expect(results.length).toBeGreaterThan(0);
    const allValues = results.flatMap(r => r.values());
    expect(allValues).toContainEqual({ orderId: 1, amount: 100, region: 'NA' });
  });
  
  it('should process batches', async () => {
    const results: ZSet<Order>[] = [];
    processor.onOutput((result) => {
      results.push(result);
    });
    
    const orders = Array.from({ length: 20 }, (_, i) => ({
      orderId: i + 1,
      amount: 100 + i,
      region: 'NA',
    }));
    
    await processor.push(orders);
    await processor.flush();
    await new Promise(resolve => setTimeout(resolve, 50));
    
    const allValues = results.flatMap(r => r.values());
    expect(allValues).toHaveLength(20);
  });
  
  it('should deduplicate messages', async () => {
    const results: Order[] = [];
    processor.onOutput((result) => {
      results.push(...result.values());
    });
    
    await processor.push({ orderId: 1, amount: 100, region: 'NA' }, 'msg-1');
    await processor.push({ orderId: 1, amount: 200, region: 'EU' }, 'msg-1');
    
    await processor.flush();
    await new Promise(resolve => setTimeout(resolve, 20));
    
    expect(results).toHaveLength(1);
    expect(results[0].amount).toBe(100);
  });
  
  it('should handle high throughput', async () => {
    const results: Order[] = [];
    const highThroughput = new AsyncStreamProcessor<Order, Order>(
      (input) => input,
      keyFn,
      { maxBatchSize: 500, maxBatchDelayMs: 1 }
    );
    highThroughput.start();
    
    highThroughput.onOutput((result) => {
      results.push(...result.values());
    });
    
    const orders = Array.from({ length: 1000 }, (_, i) => ({
      orderId: i + 1,
      amount: i,
      region: 'NA',
    }));
    
    await highThroughput.push(orders);
    await new Promise(resolve => setTimeout(resolve, 100));
    await highThroughput.flush();
    await highThroughput.stop();
    
    expect(results).toHaveLength(1000);
    const ids = new Set(results.map(r => r.orderId));
    expect(ids.size).toBe(1000);
  });
});

// ============ RACE CONDITION TESTS ============

describe('Race Condition Tests', () => {
  it('should handle concurrent updates to same key', async () => {
    let totalUpdates = 0;
    
    const processor = new AsyncStreamProcessor<{ updateId: number; targetId: number; value: number }, unknown>(
      (input) => {
        for (const [item, weight] of input.entries()) {
          if (weight > 0) {
            totalUpdates += item.value;
          }
        }
        return input;
      },
      (c) => String(c.updateId),
      { strictOrdering: true, maxBatchSize: 10, maxBatchDelayMs: 1 }
    );
    processor.start();
    
    const promises = [];
    for (let i = 0; i < 100; i++) {
      promises.push(processor.push({ updateId: i, targetId: 1, value: 1 }));
    }
    await Promise.all(promises);
    await new Promise(resolve => setTimeout(resolve, 50));
    await processor.flush();
    await processor.stop();
    
    expect(totalUpdates).toBe(100);
  });
  
  it('should serialize with mutex', async () => {
    const mutex = createMutex();
    let counter = 0;
    const results: number[] = [];
    
    const increment = async () => {
      const release = await mutex.acquire();
      try {
        const current = counter;
        await new Promise(resolve => setTimeout(resolve, Math.random() * 5));
        counter = current + 1;
        results.push(counter);
      } finally {
        release();
      }
    };
    
    await Promise.all(Array.from({ length: 50 }, () => increment()));
    
    expect(counter).toBe(50);
    expect(results).toEqual(Array.from({ length: 50 }, (_, i) => i + 1));
  });
  
  it('should not lose updates under concurrency', async () => {
    const allUpdates: number[] = [];
    
    const processor = new AsyncStreamProcessor<number, number>(
      (input) => {
        for (const [value] of input.entries()) {
          allUpdates.push(value);
        }
        return input;
      },
      String,
      { maxBatchSize: 50, maxBatchDelayMs: 1 }
    );
    processor.start();
    
    const producers = Array.from({ length: 10 }, async (_, producerId) => {
      for (let i = 0; i < 100; i++) {
        await processor.push(producerId * 1000 + i);
        if (i % 10 === 0) {
          await new Promise(resolve => setTimeout(resolve, 0));
        }
      }
    });
    
    await Promise.all(producers);
    await processor.flush();
    await processor.stop();
    
    expect(allUpdates).toHaveLength(1000);
    expect(new Set(allUpdates).size).toBe(1000);
  });
  
  it('should maintain state consistency', async () => {
    const state = { sum: 0, count: 0, values: [] as number[] };
    let inconsistent = 0;
    
    const processor = new AsyncStreamProcessor<number, number>(
      (input) => {
        for (const [value] of input.entries()) {
          state.values.push(value);
          state.count++;
          state.sum += value;
          
          if (state.count !== state.values.length || 
              state.sum !== state.values.reduce((a, b) => a + b, 0)) {
            inconsistent++;
          }
        }
        return input;
      },
      String,
      { maxBatchSize: 10, maxBatchDelayMs: 1 }
    );
    processor.start();
    
    for (let i = 0; i < 1000; i++) {
      await processor.push(i);
    }
    
    await processor.flush();
    await processor.stop();
    
    expect(inconsistent).toBe(0);
    expect(state.count).toBe(1000);
    expect(state.sum).toBe(499500);
  });
});

// ============ ASYNC BATCHER TESTS ============

describe('AsyncBatcher', () => {
  it('should batch operations', async () => {
    let batchCount = 0;
    
    const batcher = new AsyncBatcher<number, number>(
      async (items) => {
        batchCount++;
        return items.map(i => i * 2);
      },
      10,
      5
    );
    
    const results = await Promise.all(
      Array.from({ length: 25 }, (_, i) => batcher.add(i))
    );
    
    expect(batchCount).toBeLessThanOrEqual(3);
    expect(results).toHaveLength(25);
    expect(results[0]).toBe(0);
    expect(results[24]).toBe(48);
  });
});

// ============ FRESHNESS STRESS TESTS ============

describe('Freshness Stress Tests', () => {
  it('should maintain freshness under burst load', async () => {
    const processed: number[] = [];
    let totalDropped = 0;
    
    const processor = new FreshnessStreamProcessor<number, number>(
      async (input) => {
        // Simulate processing delay
        await new Promise(resolve => setTimeout(resolve, 5));
        for (const [value] of input.entries()) {
          processed.push(value);
        }
        return input;
      },
      String,
      {
        maxBufferSize: 50,
        maxBatchSize: 10,
        maxBatchDelayMs: 5,
        onDrop: (count) => { totalDropped += count; },
      }
    );
    processor.start();
    
    // Burst: push 500 items as fast as possible
    const items = Array.from({ length: 500 }, (_, i) => i);
    await processor.push(items);
    
    await new Promise(resolve => setTimeout(resolve, 200));
    await processor.stop();
    
    // Buffer should have dropped items to maintain freshness
    expect(totalDropped).toBeGreaterThan(0);
    
    // Most recent items should have been processed
    const maxProcessed = Math.max(...processed);
    expect(maxProcessed).toBe(499);
  });
  
  it('should recover from temporary slowdown', async () => {
    let slowMode = true;
    const processed: number[] = [];
    
    const processor = new FreshnessStreamProcessor<number, number>(
      async (input) => {
        if (slowMode) {
          await new Promise(resolve => setTimeout(resolve, 20));
        }
        for (const [value] of input.entries()) {
          processed.push(value);
        }
        return input;
      },
      String,
      {
        maxBufferSize: 20,
        maxMessageAgeMs: 100,
        maxBatchSize: 5,
        maxBatchDelayMs: 5,
      }
    );
    processor.start();
    
    // Push during slow mode
    for (let i = 0; i < 10; i++) {
      await processor.push(i);
    }
    
    await new Promise(resolve => setTimeout(resolve, 50));
    
    // Speed up processing
    slowMode = false;
    
    // Push fresh data
    for (let i = 100; i < 110; i++) {
      await processor.push(i);
    }
    
    await new Promise(resolve => setTimeout(resolve, 100));
    await processor.stop();
    
    // Fresh data (100+) should be processed
    const freshProcessed = processed.filter(v => v >= 100);
    expect(freshProcessed.length).toBeGreaterThan(0);
  });
  
  it('should report accurate statistics', async () => {
    const processor = new FreshnessStreamProcessor<number, number>(
      async (input) => {
        await new Promise(resolve => setTimeout(resolve, 2));
        return input;
      },
      String,
      {
        maxBufferSize: 100,
        maxBatchSize: 10,
        maxBatchDelayMs: 5,
      }
    );
    processor.start();
    
    for (let i = 0; i < 50; i++) {
      await processor.push(i);
    }
    
    await new Promise(resolve => setTimeout(resolve, 200));
    await processor.stop();
    
    const stats = processor.getStats();
    
    expect(stats.processedCount).toBe(50);
    expect(stats.avgProcessingTimeMs).toBeGreaterThan(0);
    expect(stats.queueStats.capacity).toBe(100);
  });
});
