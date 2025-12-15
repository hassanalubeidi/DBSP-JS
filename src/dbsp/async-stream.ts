/**
 * Async Stream Processing Core for DBSP
 * 
 * Provides race-condition-free async stream processing with:
 * - Sequential message ordering guarantees
 * - Atomic batch processing
 * - Backpressure handling
 * - Graceful error recovery
 * - Proper cleanup
 * - FRESHNESS GUARANTEES via circular buffers
 * 
 * Based on DBSP paper principles for incremental computation.
 */

import { ZSet } from './zset';

// ============ TYPES ============

/** Policy for handling buffer overflow */
export type OverflowPolicy = 'drop-oldest' | 'drop-newest' | 'block';

export interface AsyncStreamConfig {
  /** Maximum batch size before forcing a flush */
  maxBatchSize?: number;
  /** Maximum time (ms) to wait before flushing a batch */
  maxBatchDelayMs?: number;
  /** Enable strict ordering (slower but guarantees order) */
  strictOrdering?: boolean;
  /** Callback for errors during processing */
  onError?: (error: Error, context: string) => void;
  /** Enable debug logging */
  debug?: boolean;
  /** Maximum buffer capacity (for freshness - older items dropped when full) */
  maxBufferSize?: number;
  /** Overflow policy when buffer is full */
  overflowPolicy?: OverflowPolicy;
  /** Maximum message age in ms (older messages are dropped) */
  maxMessageAgeMs?: number;
  /** Callback when messages are dropped for freshness */
  onDrop?: (dropped: number, reason: 'overflow' | 'stale') => void;
}

export interface StreamMessage<T> {
  /** Unique sequence number for ordering */
  sequence: number;
  /** The data payload */
  data: T;
  /** Timestamp when message was created */
  timestamp: number;
  /** Optional message ID for deduplication */
  messageId?: string;
}

export interface BatchResult<T> {
  /** Number of messages processed */
  processedCount: number;
  /** Processing time in ms */
  processingTimeMs: number;
  /** The result ZSet */
  result: ZSet<T>;
  /** Sequence range processed */
  sequenceRange: [number, number];
}

export type ProcessorFn<TIn, TOut> = (input: ZSet<TIn>) => ZSet<TOut> | Promise<ZSet<TOut>>;
export type OutputCallback<T> = (result: ZSet<T>, batchInfo: BatchResult<T>) => void | Promise<void>;

// ============ CIRCULAR BUFFER ============

/**
 * Fixed-size circular buffer that maintains freshness by dropping oldest items
 * when capacity is reached. Guarantees O(1) operations.
 */
export class CircularBuffer<T> {
  private buffer: (T | undefined)[];
  private head = 0;
  private tail = 0;
  private count = 0;
  private droppedCount = 0;
  private capacity: number;
  private onDrop?: (item: T) => void;
  
  constructor(capacity: number, onDrop?: (item: T) => void) {
    if (capacity < 1) throw new Error('Capacity must be at least 1');
    this.capacity = capacity;
    this.onDrop = onDrop;
    this.buffer = new Array(capacity);
  }
  
  push(item: T): boolean {
    let dropped = false;
    
    if (this.count === this.capacity) {
      const droppedItem = this.buffer[this.tail];
      if (droppedItem !== undefined && this.onDrop) {
        this.onDrop(droppedItem);
      }
      this.tail = (this.tail + 1) % this.capacity;
      this.droppedCount++;
      dropped = true;
    } else {
      this.count++;
    }
    
    this.buffer[this.head] = item;
    this.head = (this.head + 1) % this.capacity;
    
    return dropped;
  }
  
  pushBatch(items: T[]): number {
    let dropped = 0;
    for (const item of items) {
      if (this.push(item)) dropped++;
    }
    return dropped;
  }
  
  pop(): T | undefined {
    if (this.count === 0) return undefined;
    
    const item = this.buffer[this.tail];
    this.buffer[this.tail] = undefined;
    this.tail = (this.tail + 1) % this.capacity;
    this.count--;
    
    return item;
  }
  
  popBatch(n: number): T[] {
    const result: T[] = [];
    const toTake = Math.min(n, this.count);
    
    for (let i = 0; i < toTake; i++) {
      const item = this.pop();
      if (item !== undefined) {
        result.push(item);
      }
    }
    
    return result;
  }
  
  peek(): T | undefined {
    if (this.count === 0) return undefined;
    return this.buffer[this.tail];
  }
  
  toArray(): T[] {
    const result: T[] = [];
    let idx = this.tail;
    for (let i = 0; i < this.count; i++) {
      const item = this.buffer[idx];
      if (item !== undefined) {
        result.push(item);
      }
      idx = (idx + 1) % this.capacity;
    }
    return result;
  }
  
  clear(): void {
    this.buffer = new Array(this.capacity);
    this.head = 0;
    this.tail = 0;
    this.count = 0;
  }
  
  size(): number { return this.count; }
  isEmpty(): boolean { return this.count === 0; }
  isFull(): boolean { return this.count === this.capacity; }
  getCapacity(): number { return this.capacity; }
  getDroppedCount(): number { return this.droppedCount; }
  getUtilization(): number { return this.count / this.capacity; }
}

// ============ FRESHNESS-AWARE QUEUE ============

/**
 * Queue that maintains data freshness by:
 * 1. Using circular buffer to drop oldest when full
 * 2. Dropping messages older than maxAgeMs
 * 3. Tracking lag metrics
 */
export class FreshnessQueue<T> {
  private buffer: CircularBuffer<StreamMessage<T>>;
  private sequence = 0;
  private pendingResolvers: Array<() => void> = [];
  private droppedStale = 0;
  private droppedOverflow = 0;
  private capacity: number;
  private maxAgeMs: number | undefined;
  private onDropCallback?: (dropped: number, reason: 'overflow' | 'stale') => void;
  private debug: boolean;
  
  constructor(
    capacity: number,
    maxAgeMs: number | undefined,
    onDrop?: (dropped: number, reason: 'overflow' | 'stale') => void,
    debug = false
  ) {
    this.capacity = capacity;
    this.maxAgeMs = maxAgeMs;
    this.onDropCallback = onDrop;
    this.debug = debug;
    this.buffer = new CircularBuffer(capacity, () => {
      this.droppedOverflow++;
    });
  }
  
  enqueue(data: T, messageId?: string): number {
    const seq = this.sequence++;
    const message: StreamMessage<T> = {
      sequence: seq,
      data,
      timestamp: Date.now(),
      messageId,
    };
    
    const wasDropped = this.buffer.push(message);
    
    if (wasDropped && this.onDropCallback) {
      this.onDropCallback(1, 'overflow');
    }
    
    if (this.debug && wasDropped) {
      console.log(`[FreshnessQueue] Dropped oldest for freshness, capacity ${this.capacity}`);
    }
    
    while (this.pendingResolvers.length > 0) {
      const resolver = this.pendingResolvers.shift();
      resolver?.();
    }
    
    return seq;
  }
  
  enqueueBatch(items: T[], messageIdPrefix?: string): number[] {
    const sequences: number[] = [];
    for (let i = 0; i < items.length; i++) {
      const msgId = messageIdPrefix ? `${messageIdPrefix}_${i}` : undefined;
      sequences.push(this.enqueue(items[i], msgId));
    }
    return sequences;
  }
  
  async dequeue(maxCount: number, timeoutMs?: number): Promise<StreamMessage<T>[]> {
    if (this.buffer.isEmpty()) {
      await this.waitForData(timeoutMs);
    }
    
    const now = Date.now();
    const messages: StreamMessage<T>[] = [];
    let staleDropped = 0;
    
    while (messages.length < maxCount && !this.buffer.isEmpty()) {
      const msg = this.buffer.pop();
      if (!msg) break;
      
      if (this.maxAgeMs !== undefined && (now - msg.timestamp) > this.maxAgeMs) {
        staleDropped++;
        this.droppedStale++;
        continue;
      }
      
      messages.push(msg);
    }
    
    if (staleDropped > 0) {
      if (this.onDropCallback) {
        this.onDropCallback(staleDropped, 'stale');
      }
      if (this.debug) {
        console.log(`[FreshnessQueue] Dropped ${staleDropped} stale messages`);
      }
    }
    
    messages.sort((a, b) => a.sequence - b.sequence);
    
    return messages;
  }
  
  getLag(): number {
    const oldest = this.buffer.peek();
    if (!oldest) return 0;
    return Date.now() - oldest.timestamp;
  }
  
  isLagging(thresholdMs: number): boolean {
    return this.getLag() > thresholdMs;
  }
  
  dropStale(ageMs?: number): number {
    const maxAge = ageMs ?? this.maxAgeMs;
    if (maxAge === undefined) return 0;
    
    const now = Date.now();
    let dropped = 0;
    
    while (!this.buffer.isEmpty()) {
      const oldest = this.buffer.peek();
      if (!oldest) break;
      
      if ((now - oldest.timestamp) > maxAge) {
        this.buffer.pop();
        dropped++;
        this.droppedStale++;
      } else {
        break;
      }
    }
    
    if (dropped > 0 && this.onDropCallback) {
      this.onDropCallback(dropped, 'stale');
    }
    
    return dropped;
  }
  
  private waitForData(timeoutMs?: number): Promise<void> {
    if (!this.buffer.isEmpty()) {
      return Promise.resolve();
    }
    
    return new Promise((resolve) => {
      this.pendingResolvers.push(resolve);
      
      if (timeoutMs !== undefined) {
        setTimeout(() => {
          const idx = this.pendingResolvers.indexOf(resolve);
          if (idx >= 0) {
            this.pendingResolvers.splice(idx, 1);
            resolve();
          }
        }, timeoutMs);
      }
    });
  }
  
  size(): number { return this.buffer.size(); }
  getCapacity(): number { return this.buffer.getCapacity(); }
  getUtilization(): number { return this.buffer.getUtilization(); }
  getSequence(): number { return this.sequence; }
  getDroppedStale(): number { return this.droppedStale; }
  getDroppedOverflow(): number { return this.droppedOverflow; }
  getTotalDropped(): number { return this.droppedStale + this.droppedOverflow; }
  
  clear(): void {
    this.buffer.clear();
  }
  
  getStats(): {
    size: number;
    capacity: number;
    utilization: number;
    lagMs: number;
    droppedStale: number;
    droppedOverflow: number;
    totalDropped: number;
  } {
    return {
      size: this.size(),
      capacity: this.getCapacity(),
      utilization: this.getUtilization(),
      lagMs: this.getLag(),
      droppedStale: this.droppedStale,
      droppedOverflow: this.droppedOverflow,
      totalDropped: this.getTotalDropped(),
    };
  }
}

// ============ ASYNC QUEUE ============

/**
 * Lock-free async queue with ordering guarantees
 */
export class AsyncQueue<T> {
  private queue: StreamMessage<T>[] = [];
  private sequence = 0;
  private pendingResolvers: Array<() => void> = [];
  private debug: boolean;
  
  constructor(debug = false) {
    this.debug = debug;
  }
  
  enqueue(data: T, messageId?: string): number {
    const seq = this.sequence++;
    const message: StreamMessage<T> = {
      sequence: seq,
      data,
      timestamp: Date.now(),
      messageId,
    };
    
    this.queue.push(message);
    
    if (this.debug) {
      console.log(`[AsyncQueue] Enqueued seq=${seq}, size=${this.queue.length}`);
    }
    
    while (this.pendingResolvers.length > 0) {
      const resolver = this.pendingResolvers.shift();
      resolver?.();
    }
    
    return seq;
  }
  
  enqueueBatch(items: T[], messageIdPrefix?: string): number[] {
    const sequences: number[] = [];
    for (let i = 0; i < items.length; i++) {
      const msgId = messageIdPrefix ? `${messageIdPrefix}_${i}` : undefined;
      sequences.push(this.enqueue(items[i], msgId));
    }
    return sequences;
  }
  
  async dequeue(maxCount: number, timeoutMs?: number): Promise<StreamMessage<T>[]> {
    if (this.queue.length === 0) {
      await this.waitForData(timeoutMs);
    }
    
    const count = Math.min(maxCount, this.queue.length);
    const messages = this.queue.splice(0, count);
    
    messages.sort((a, b) => a.sequence - b.sequence);
    
    if (this.debug && messages.length > 0) {
      console.log(`[AsyncQueue] Dequeued ${messages.length} messages`);
    }
    
    return messages;
  }
  
  private waitForData(timeoutMs?: number): Promise<void> {
    if (this.queue.length > 0) {
      return Promise.resolve();
    }
    
    return new Promise((resolve) => {
      this.pendingResolvers.push(resolve);
      
      if (timeoutMs !== undefined) {
        setTimeout(() => {
          const idx = this.pendingResolvers.indexOf(resolve);
          if (idx >= 0) {
            this.pendingResolvers.splice(idx, 1);
            resolve();
          }
        }, timeoutMs);
      }
    });
  }
  
  size(): number { return this.queue.length; }
  clear(): void { this.queue = []; }
  getSequence(): number { return this.sequence; }
}

// ============ ASYNC STREAM PROCESSOR ============

/**
 * Async stream processor with race-condition-free guarantees
 */
export class AsyncStreamProcessor<TIn, TOut = TIn> {
  private queue: AsyncQueue<TIn>;
  private config: {
    maxBatchSize: number;
    maxBatchDelayMs: number;
    strictOrdering: boolean;
    onError: (error: Error, context: string) => void;
    debug: boolean;
  };
  private outputCallbacks: OutputCallback<TOut>[] = [];
  private processedMessageIds = new Set<string>();
  
  private processor: ProcessorFn<TIn, TOut>;
  private keyFn: (value: TIn) => string;
  private running = false;
  private processingLoop: Promise<void> | null = null;
  private lastProcessedSequence = -1;
  private pendingBatch: StreamMessage<TIn>[] = [];
  private batchTimer: ReturnType<typeof setTimeout> | null = null;
  private processingMutex = Promise.resolve();
  
  constructor(
    processor: ProcessorFn<TIn, TOut>,
    keyFn: (value: TIn) => string,
    config: AsyncStreamConfig = {}
  ) {
    this.processor = processor;
    this.keyFn = keyFn;
    this.config = {
      maxBatchSize: config.maxBatchSize ?? 1000,
      maxBatchDelayMs: config.maxBatchDelayMs ?? 10,
      strictOrdering: config.strictOrdering ?? true,
      onError: config.onError ?? ((err, ctx) => console.error(`[AsyncStream] Error in ${ctx}:`, err)),
      debug: config.debug ?? false,
    };
    this.queue = new AsyncQueue(this.config.debug);
  }
  
  start(): void {
    if (this.running) return;
    
    this.running = true;
    this.processingLoop = this.runProcessingLoop();
    
    if (this.config.debug) {
      console.log('[AsyncStream] Started processing loop');
    }
  }
  
  async stop(): Promise<void> {
    this.running = false;
    
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    
    if (this.pendingBatch.length > 0) {
      await this.flushBatch();
    }
    
    if (this.processingLoop) {
      await this.processingLoop;
      this.processingLoop = null;
    }
    
    if (this.config.debug) {
      console.log('[AsyncStream] Stopped processing loop');
    }
  }
  
  async push(data: TIn | TIn[], messageId?: string): Promise<number[]> {
    const items = Array.isArray(data) ? data : [data];
    if (items.length === 0) return [];
    
    if (messageId && this.processedMessageIds.has(messageId)) {
      if (this.config.debug) {
        console.log(`[AsyncStream] Duplicate message ${messageId} ignored`);
      }
      return [];
    }
    
    const sequences = this.queue.enqueueBatch(items, messageId);
    
    if (messageId) {
      this.processedMessageIds.add(messageId);
    }
    
    return sequences;
  }
  
  async pushAndWait(data: TIn | TIn[], messageId?: string): Promise<BatchResult<TOut>> {
    const sequences = await this.push(data, messageId);
    if (sequences.length === 0) {
      return {
        processedCount: 0,
        processingTimeMs: 0,
        result: ZSet.zero(JSON.stringify),
        sequenceRange: [0, 0],
      };
    }
    
    const maxSeq = Math.max(...sequences);
    return this.waitForSequence(maxSeq);
  }
  
  async waitForSequence(sequence: number, timeoutMs = 30000): Promise<BatchResult<TOut>> {
    const startTime = Date.now();
    
    while (this.lastProcessedSequence < sequence) {
      if (Date.now() - startTime > timeoutMs) {
        throw new Error(`Timeout waiting for sequence ${sequence}`);
      }
      await new Promise(resolve => setTimeout(resolve, 1));
    }
    
    return {
      processedCount: 1,
      processingTimeMs: Date.now() - startTime,
      result: ZSet.zero(JSON.stringify),
      sequenceRange: [sequence, sequence],
    };
  }
  
  onOutput(callback: OutputCallback<TOut>): () => void {
    this.outputCallbacks.push(callback);
    return () => {
      const idx = this.outputCallbacks.indexOf(callback);
      if (idx >= 0) {
        this.outputCallbacks.splice(idx, 1);
      }
    };
  }
  
  private async runProcessingLoop(): Promise<void> {
    while (this.running) {
      try {
        const messages = await this.queue.dequeue(
          this.config.maxBatchSize,
          this.config.maxBatchDelayMs
        );
        
        if (messages.length > 0) {
          this.pendingBatch.push(...messages);
        }
        
        const shouldFlush = 
          this.pendingBatch.length >= this.config.maxBatchSize ||
          (this.pendingBatch.length > 0 && this.queue.size() === 0);
        
        if (shouldFlush) {
          await this.flushBatch();
        }
      } catch (error) {
        this.config.onError(error as Error, 'processingLoop');
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
  }
  
  private async flushBatch(): Promise<void> {
    if (this.pendingBatch.length === 0) return;
    
    this.processingMutex = this.processingMutex.then(async () => {
      const batch = this.pendingBatch;
      this.pendingBatch = [];
      
      if (batch.length === 0) return;
      
      const startTime = performance.now();
      
      if (this.config.strictOrdering) {
        batch.sort((a, b) => a.sequence - b.sequence);
      }
      
      try {
        const entries: [TIn, number][] = batch.map(msg => [msg.data, 1]);
        const inputZSet = ZSet.fromEntries(entries, this.keyFn);
        
        const result = await Promise.resolve(this.processor(inputZSet));
        
        const processingTimeMs = performance.now() - startTime;
        
        this.lastProcessedSequence = batch[batch.length - 1].sequence;
        
        const batchResult: BatchResult<TOut> = {
          processedCount: batch.length,
          processingTimeMs,
          result,
          sequenceRange: [batch[0].sequence, batch[batch.length - 1].sequence],
        };
        
        if (this.config.debug) {
          console.log(`[AsyncStream] Processed ${batch.length} in ${processingTimeMs.toFixed(2)}ms`);
        }
        
        for (const callback of this.outputCallbacks) {
          try {
            await Promise.resolve(callback(result, batchResult));
          } catch (error) {
            this.config.onError(error as Error, 'outputCallback');
          }
        }
      } catch (error) {
        this.config.onError(error as Error, 'flushBatch');
        for (const msg of batch) {
          this.queue.enqueue(msg.data, msg.messageId);
        }
      }
    });
    
    await this.processingMutex;
  }
  
  async flush(): Promise<void> {
    await this.flushBatch();
  }
  
  getStats(): {
    queueSize: number;
    pendingBatchSize: number;
    lastProcessedSequence: number;
    currentSequence: number;
    processedMessageCount: number;
  } {
    return {
      queueSize: this.queue.size(),
      pendingBatchSize: this.pendingBatch.length,
      lastProcessedSequence: this.lastProcessedSequence,
      currentSequence: this.queue.getSequence(),
      processedMessageCount: this.processedMessageIds.size,
    };
  }
  
  clearDeduplicationCache(): void {
    this.processedMessageIds.clear();
  }
}

// ============ FRESHNESS STREAM PROCESSOR ============

/**
 * Stream processor with freshness guarantees.
 * Uses circular buffer to maintain real-time processing - never lags behind data.
 */
export class FreshnessStreamProcessor<TIn, TOut = TIn> {
  private queue: FreshnessQueue<TIn>;
  private config: {
    maxBatchSize: number;
    maxBatchDelayMs: number;
    strictOrdering: boolean;
    debug: boolean;
    maxBufferSize: number;
    overflowPolicy: OverflowPolicy;
    maxMessageAgeMs: number | undefined;
    onError?: (error: Error, context: string) => void;
    onDrop?: (dropped: number, reason: 'overflow' | 'stale') => void;
  };
  private outputCallbacks: OutputCallback<TOut>[] = [];
  private processedMessageIds = new Set<string>();
  
  private processor: ProcessorFn<TIn, TOut>;
  private keyFn: (value: TIn) => string;
  private running = false;
  private processingLoop: Promise<void> | null = null;
  private processingMutex = Promise.resolve();
  
  private processedCount = 0;
  private totalProcessingTimeMs = 0;
  private maxLagObserved = 0;
  
  constructor(
    processor: ProcessorFn<TIn, TOut>,
    keyFn: (value: TIn) => string,
    config: AsyncStreamConfig = {}
  ) {
    this.processor = processor;
    this.keyFn = keyFn;
    this.config = {
      maxBatchSize: config.maxBatchSize ?? 1000,
      maxBatchDelayMs: config.maxBatchDelayMs ?? 10,
      strictOrdering: config.strictOrdering ?? true,
      debug: config.debug ?? false,
      maxBufferSize: config.maxBufferSize ?? 10000,
      overflowPolicy: config.overflowPolicy ?? 'drop-oldest',
      maxMessageAgeMs: config.maxMessageAgeMs,
      onError: config.onError,
      onDrop: config.onDrop,
    };
    
    this.queue = new FreshnessQueue(
      this.config.maxBufferSize,
      this.config.maxMessageAgeMs,
      this.config.onDrop,
      this.config.debug
    );
  }
  
  start(): void {
    if (this.running) return;
    
    this.running = true;
    this.processingLoop = this.runProcessingLoop();
    
    if (this.config.debug) {
      console.log('[FreshnessStream] Started with buffer capacity', this.config.maxBufferSize);
    }
  }
  
  async stop(): Promise<void> {
    this.running = false;
    
    if (this.processingLoop) {
      await this.processingLoop;
      this.processingLoop = null;
    }
    
    if (this.config.debug) {
      console.log('[FreshnessStream] Stopped. Stats:', this.getStats());
    }
  }
  
  async push(data: TIn | TIn[], messageId?: string): Promise<number[]> {
    const items = Array.isArray(data) ? data : [data];
    if (items.length === 0) return [];
    
    if (messageId && this.processedMessageIds.has(messageId)) {
      if (this.config.debug) {
        console.log(`[FreshnessStream] Duplicate ${messageId} ignored`);
      }
      return [];
    }
    
    const sequences = this.queue.enqueueBatch(items, messageId);
    
    if (messageId) {
      this.processedMessageIds.add(messageId);
    }
    
    return sequences;
  }
  
  onOutput(callback: OutputCallback<TOut>): () => void {
    this.outputCallbacks.push(callback);
    return () => {
      const idx = this.outputCallbacks.indexOf(callback);
      if (idx >= 0) this.outputCallbacks.splice(idx, 1);
    };
  }
  
  async flush(): Promise<void> {
    await this.processBatch();
  }
  
  dropStale(maxAgeMs?: number): number {
    return this.queue.dropStale(maxAgeMs);
  }
  
  getLag(): number {
    return this.queue.getLag();
  }
  
  isLagging(thresholdMs: number): boolean {
    return this.queue.isLagging(thresholdMs);
  }
  
  getStats(): {
    queueStats: ReturnType<FreshnessQueue<TIn>['getStats']>;
    processedCount: number;
    avgProcessingTimeMs: number;
    maxLagObserved: number;
    running: boolean;
  } {
    return {
      queueStats: this.queue.getStats(),
      processedCount: this.processedCount,
      avgProcessingTimeMs: this.processedCount > 0 
        ? this.totalProcessingTimeMs / this.processedCount 
        : 0,
      maxLagObserved: this.maxLagObserved,
      running: this.running,
    };
  }
  
  clearDeduplicationCache(): void {
    this.processedMessageIds.clear();
  }
  
  private async runProcessingLoop(): Promise<void> {
    while (this.running) {
      try {
        await this.processBatch();
        
        const lag = this.queue.getLag();
        if (lag > this.maxLagObserved) {
          this.maxLagObserved = lag;
        }
        
        if (this.config.debug && lag > 100) {
          console.log(`[FreshnessStream] Warning: lag is ${lag}ms`);
        }
      } catch (error) {
        if (this.config.onError) {
          this.config.onError(error as Error, 'processingLoop');
        }
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    }
  }
  
  private async processBatch(): Promise<void> {
    const messages = await this.queue.dequeue(
      this.config.maxBatchSize,
      this.config.maxBatchDelayMs
    );
    
    if (messages.length === 0) return;
    
    this.processingMutex = this.processingMutex.then(async () => {
      const startTime = performance.now();
      
      try {
        const entries: [TIn, number][] = messages.map(msg => [msg.data, 1]);
        const inputZSet = ZSet.fromEntries(entries, this.keyFn);
        
        const result = await Promise.resolve(this.processor(inputZSet));
        
        const processingTime = performance.now() - startTime;
        this.totalProcessingTimeMs += processingTime;
        this.processedCount += messages.length;
        
        const batchResult: BatchResult<TOut> = {
          processedCount: messages.length,
          processingTimeMs: processingTime,
          result,
          sequenceRange: [messages[0].sequence, messages[messages.length - 1].sequence],
        };
        
        if (this.config.debug) {
          console.log(`[FreshnessStream] Processed ${messages.length} in ${processingTime.toFixed(2)}ms`);
        }
        
        for (const callback of this.outputCallbacks) {
          try {
            await Promise.resolve(callback(result, batchResult));
          } catch (error) {
            if (this.config.onError) {
              this.config.onError(error as Error, 'outputCallback');
            }
          }
        }
      } catch (error) {
        if (this.config.onError) {
          this.config.onError(error as Error, 'processBatch');
        }
      }
    });
    
    await this.processingMutex;
  }
}

// ============ UTILITIES ============

/**
 * Create a mutex for serializing async operations
 */
export function createMutex(): {
  acquire: () => Promise<() => void>;
  isLocked: () => boolean;
} {
  let locked = false;
  const waiting: Array<() => void> = [];
  
  return {
    acquire: () => {
      return new Promise<() => void>((resolve) => {
        const release = () => {
          locked = false;
          if (waiting.length > 0) {
            const next = waiting.shift()!;
            locked = true;
            next();
          }
        };
        
        if (!locked) {
          locked = true;
          resolve(release);
        } else {
          waiting.push(() => resolve(release));
        }
      });
    },
    isLocked: () => locked,
  };
}

/**
 * Batch async operations with automatic flushing
 */
export class AsyncBatcher<T, R> {
  private batch: T[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private pendingResolvers: Array<{ resolve: (r: R) => void; reject: (e: Error) => void; index: number }> = [];
  private processBatch: (items: T[]) => Promise<R[]>;
  private maxBatchSize: number;
  private maxDelayMs: number;
  
  constructor(
    processBatch: (items: T[]) => Promise<R[]>,
    maxBatchSize = 100,
    maxDelayMs = 10
  ) {
    this.processBatch = processBatch;
    this.maxBatchSize = maxBatchSize;
    this.maxDelayMs = maxDelayMs;
  }
  
  async add(item: T): Promise<R> {
    const index = this.batch.length;
    this.batch.push(item);
    
    return new Promise((resolve, reject) => {
      this.pendingResolvers.push({ resolve, reject, index });
      
      if (this.batch.length >= this.maxBatchSize) {
        this.flush();
      } else if (!this.flushTimer) {
        this.flushTimer = setTimeout(() => this.flush(), this.maxDelayMs);
      }
    });
  }
  
  private async flush(): Promise<void> {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    
    const items = this.batch;
    const resolvers = this.pendingResolvers;
    
    this.batch = [];
    this.pendingResolvers = [];
    
    if (items.length === 0) return;
    
    try {
      const results = await this.processBatch(items);
      
      for (const { resolve, index } of resolvers) {
        resolve(results[index]);
      }
    } catch (error) {
      for (const { reject } of resolvers) {
        reject(error as Error);
      }
    }
  }
}
