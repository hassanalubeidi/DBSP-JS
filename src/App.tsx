import { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import './App.css';
import { useFreshnessDBSP } from './dbsp/react/useFreshnessDBSP';

// ============ SQL POPOVER COMPONENT ============

interface SQLPopoverProps {
  sql: string;
}

function SQLPopover({ sql }: SQLPopoverProps) {
  const [isOpen, setIsOpen] = useState(false);
  const popoverRef = useRef<HTMLDivElement>(null);
  
  // Close on click outside
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => document.removeEventListener('mousedown', handleClickOutside);
    }
  }, [isOpen]);
  
  return (
    <div className="sql-popover-container" ref={popoverRef}>
      <button 
        className="sql-icon-btn" 
        onClick={() => setIsOpen(!isOpen)}
        title="View SQL"
      >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
          <polyline points="14 2 14 8 20 8"/>
          <line x1="16" y1="13" x2="8" y2="13"/>
          <line x1="16" y1="17" x2="8" y2="17"/>
          <polyline points="10 9 9 9 8 9"/>
        </svg>
      </button>
      {isOpen && (
        <div className="sql-popover">
          <div className="sql-popover-header">
            <span>SQL Query</span>
            <button className="sql-popover-close" onClick={() => setIsOpen(false)}>×</button>
          </div>
          <pre className="sql-popover-code">{sql}</pre>
        </div>
      )}
    </div>
  );
}

// ============ TYPES ============

interface Order {
  orderId: number;
  customerId: number;
  productId: number;
  amount: number;
  quantity: number;
  status: string;
  region: string;
  category: string;
  [key: string]: unknown; // Index signature for Record<string, unknown>
}


interface DeltaOp {
  op: 'insert' | 'update' | 'delete';
  row?: Order;
  orderId?: number;
}

interface ConnectionStats {
  state: 'disconnected' | 'connecting' | 'connected';
  snapshotLoaded: boolean;
  snapshotSize: number;
  snapshotTimeMs: number;
  totalDeltas: number;
  deltasPerSecond: number;
  lastDeltaBatchMs: number;
  totalRowsProcessed: number;
}

// ============ CONSTANTS ============

const WS_URL = 'ws://localhost:8765';

// ============ APP ============

function App() {
  // ============ FRESHNESS CONFIG ============
  // NOTE: processingIntervalMs of 16ms creates ~60 renders/sec per hook × 7 hooks
  // Increased to 100ms to reduce DOM churn while still maintaining responsiveness
  const freshnessConfig = {
    maxBufferSize: 5000,       // Circular buffer capacity
    maxMessageAgeMs: 2000,     // Drop messages older than 2s
    maxBatchSize: 500,         // Process up to 500 at a time
    processingIntervalMs: 100, // ~10fps processing (reduced from 16ms to prevent DOM node accumulation)
  };
  
  // Shared drop handler for all views
  const handleDrop = useCallback((count: number, reason: 'overflow' | 'stale') => {
    console.log(`[App] Dropped ${count} messages: ${reason}`);
  }, []);
  
  // ============ DBSP HOOKS (with Freshness Guarantees) ============
  
  // Filter view: pending orders
  const pendingOrders = useFreshnessDBSP<Order>(
    "SELECT * FROM data WHERE status = 'pending'",
    { key: 'orderId', ...freshnessConfig, onDrop: handleDrop }
  );
  
  // Filter view: high-value orders
  const highValueOrders = useFreshnessDBSP<Order>(
    "SELECT * FROM data WHERE amount > 300",
    { key: 'orderId', ...freshnessConfig, onDrop: handleDrop }
  );
  
  // GROUP BY aggregation view: orders by region
  interface RegionalSummary { region: string; total_amount: number; order_count: number; avg_amount: number; [key: string]: unknown; }
  const ordersByRegion = useFreshnessDBSP<Order, RegionalSummary>(
    "SELECT region, SUM(amount) AS total_amount, COUNT(*) AS order_count, AVG(amount) AS avg_amount FROM data GROUP BY region",
    { key: 'orderId', outputKey: 'region', ...freshnessConfig, onDrop: handleDrop, debug: false }
  );

  // Orders by Status aggregation
  interface StatusSummary { status: string; total_amount: number; order_count: number; avg_amount: number; [key: string]: unknown; }
  const ordersByStatus = useFreshnessDBSP<Order, StatusSummary>(
    "SELECT status, SUM(amount) AS total_amount, COUNT(*) AS order_count, AVG(amount) AS avg_amount FROM data GROUP BY status",
    { key: 'orderId', outputKey: 'status', ...freshnessConfig, onDrop: handleDrop }
  );

  // Orders by Category aggregation
  interface CategorySummary { category: string; total_amount: number; order_count: number; avg_amount: number; total_quantity: number; [key: string]: unknown; }
  const ordersByCategory = useFreshnessDBSP<Order, CategorySummary>(
    "SELECT category, SUM(amount) AS total_amount, COUNT(*) AS order_count, AVG(amount) AS avg_amount, SUM(quantity) AS total_quantity FROM data GROUP BY category",
    { key: 'orderId', outputKey: 'category', ...freshnessConfig, onDrop: handleDrop }
  );

  // Weighted average by category
  interface WeightedAvgSummary { category: string; weighted_total: number; total_weight: number; [key: string]: unknown; }
  const weightedAvgByCategory = useFreshnessDBSP<Order, WeightedAvgSummary>(
    "SELECT category, SUM(amount * quantity) AS weighted_total, SUM(quantity) AS total_weight FROM data GROUP BY category",
    { key: 'orderId', outputKey: 'category', ...freshnessConfig, onDrop: handleDrop }
  );

  // Customer aggregation
  interface CustomerSummary { customerId: number; total_spent: number; order_count: number; avg_order: number; total_quantity: number; [key: string]: unknown; }
  const ordersByCustomer = useFreshnessDBSP<Order, CustomerSummary>(
    "SELECT customerId, SUM(amount) AS total_spent, COUNT(*) AS order_count, AVG(amount) AS avg_order, SUM(quantity) AS total_quantity FROM data GROUP BY customerId",
    { key: 'orderId', outputKey: 'customerId', ...freshnessConfig, onDrop: handleDrop }
  );

  // High-value customers (filter on aggregation result - simulates HAVING)
  // MEMORY FIX: Memoize to avoid creating 3 new arrays on every render
  const topCustomers = useMemo(() => {
    return ordersByCustomer.results
      .filter(c => (c.total_spent as number) > 1000)
      .sort((a, b) => ((b.total_spent as number) || 0) - ((a.total_spent as number) || 0))
      .slice(0, 10);
  }, [ordersByCustomer.results]);

  // ============ CONNECTION STATE ============
  const [stats, setStats] = useState<ConnectionStats>({
    state: 'disconnected',
    snapshotLoaded: false,
    snapshotSize: 0,
    snapshotTimeMs: 0,
    totalDeltas: 0,
    deltasPerSecond: 0,
    lastDeltaBatchMs: 0,
    totalRowsProcessed: 0,
  });

  // ============ CONTROLS ============
  const [deltaRate, setDeltaRate] = useState(30);
  const [batchSize, setBatchSize] = useState(10);
  const [isPaused, setIsPaused] = useState(false);
  const [burstSize, setBurstSize] = useState(1000);
  const [activeView, setActiveView] = useState<'pending' | 'highValue' | 'recent'>('recent');
  
  // ============ BURST STATE ============
  const [isBurstInProgress, setIsBurstInProgress] = useState(false);
  
  // ============ RECENT ROWS TRACKING ============
  const [recentRows, setRecentRows] = useState<Array<Order & { _addedAt: number }>>([]);
  const [, forceUpdate] = useState(0);
  
  // Update age display every 5 seconds (reduced from 1s to minimize re-renders)
  useEffect(() => {
    const interval = setInterval(() => forceUpdate(n => n + 1), 5000);
    return () => clearInterval(interval);
  }, []);

  // ============ REFS ============
  const wsRef = useRef<WebSocket | null>(null);
  const snapshotStartRef = useRef(0);
  const snapshotChunksRef = useRef<Order[]>([]);
  const deltaCountRef = useRef(0);
  const deltaWindowRef = useRef<number[]>([]);
  
  // Throttled stats to reduce re-renders (DOM node fix)
  const pendingStatsRef = useRef<Partial<ConnectionStats>>({});
  const statsUpdateTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const totalRowsProcessedRef = useRef(0); // Track separately to avoid stale closure
  
  // Throttled stats update - batches multiple updates into one render
  const updateStatsThrottled = useCallback((updates: Partial<ConnectionStats>) => {
    // Merge updates into pending
    Object.assign(pendingStatsRef.current, updates);
    
    // Schedule update if not already scheduled
    if (!statsUpdateTimeoutRef.current) {
      statsUpdateTimeoutRef.current = setTimeout(() => {
        setStats(s => ({ ...s, ...pendingStatsRef.current }));
        pendingStatsRef.current = {};
        statsUpdateTimeoutRef.current = null;
      }, 100); // Update at most every 100ms
    }
  }, []);

  // ============ CONNECT ============
  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    setStats(s => ({ ...s, state: 'connecting' }));
    snapshotChunksRef.current = [];
    snapshotStartRef.current = performance.now();
    deltaCountRef.current = 0;

    // Clear existing data in all views
    pendingOrders.clear();
    highValueOrders.clear();
    ordersByRegion.clear();
    ordersByStatus.clear();
    ordersByCategory.clear();
    weightedAvgByCategory.clear();
    ordersByCustomer.clear();
    setRecentRows([]);

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      setStats(s => ({ ...s, state: 'connected' }));
      console.log('[App] Connected to server');
    };

    ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);

      if (msg.type === 'snapshot-chunk') {
        // Accumulate snapshot chunks
        snapshotChunksRef.current.push(...msg.data);

        if (msg.isLast) {
          // All chunks received - load into DBSP
          const snapshotTime = performance.now() - snapshotStartRef.current;
          const orders = snapshotChunksRef.current;

          console.log(`[App] Received ${orders.length.toLocaleString()} orders in ${snapshotTime.toFixed(0)}ms`);

          // Load into all DBSP views
          const loadStart = performance.now();
          
          pendingOrders.push(orders);
          highValueOrders.push(orders);
          ordersByRegion.push(orders);
          ordersByStatus.push(orders);
          ordersByCategory.push(orders);
          weightedAvgByCategory.push(orders);
          ordersByCustomer.push(orders);
          
          const loadTime = performance.now() - loadStart;

          console.log(`[App] Loaded into DBSP in ${loadTime.toFixed(0)}ms`);

          totalRowsProcessedRef.current = orders.length;
          setStats(s => ({
            ...s,
            snapshotLoaded: true,
            snapshotSize: orders.length,
            snapshotTimeMs: snapshotTime + loadTime,
            totalRowsProcessed: totalRowsProcessedRef.current,
          }));
          
          // MEMORY FIX: Clear snapshot chunks after loading - prevents 100K objects leak!
          snapshotChunksRef.current = [];
        }
      } else if (msg.type === 'delta') {
        // Process delta batch
        const processStart = performance.now();
        const deltas = msg.data as DeltaOp[];

        // Collect updates
        const updates: Order[] = [];
        const deleteKeys: number[] = [];

        for (const delta of deltas) {
          if (delta.op === 'insert' || delta.op === 'update') {
            if (delta.row) updates.push(delta.row);
          } else if (delta.op === 'delete') {
            if (delta.orderId !== undefined) deleteKeys.push(delta.orderId);
          }
        }

        // Apply to all views
        if (updates.length > 0) {
          pendingOrders.push(updates);
          highValueOrders.push(updates);
          ordersByRegion.push(updates);
          ordersByStatus.push(updates);
          ordersByCategory.push(updates);
          weightedAvgByCategory.push(updates);
          ordersByCustomer.push(updates);
          
          // Track recent rows (newest first, limit to 20)
          // MEMORY FIX: Create new objects instead of mutating shared row objects
          const now = Date.now();
          setRecentRows(prev => {
            const newRows: Array<Order & { _addedAt: number }> = updates.map(row => ({
              ...row,
              _addedAt: now,
            }));
            const combined = [...newRows, ...prev];
            return combined.length > 20 ? combined.slice(0, 20) : combined;
          });
        }

        for (const key of deleteKeys) {
          pendingOrders.remove(key);
          highValueOrders.remove(key);
          ordersByRegion.remove(key);
          ordersByStatus.remove(key);
          ordersByCategory.remove(key);
          weightedAvgByCategory.remove(key);
          ordersByCustomer.remove(key);
          
          // Remove deleted rows from recent list
          setRecentRows(prev => prev.filter(r => r.orderId !== key));
        }

        const processTime = performance.now() - processStart;
        deltaCountRef.current += deltas.length;

        // Track burst completion
        if (msg.isBurst) {
          const progress = msg.burstProgress || 0;
          const total = msg.burstTotal || 0;
          
          // Burst complete when all rows received
          if (progress >= total) {
            setIsBurstInProgress(false);
            // Clear safety timeout
            if (burstTimeoutRef.current) {
              clearTimeout(burstTimeoutRef.current);
              burstTimeoutRef.current = null;
            }
            console.log(`[App] Burst complete: ${total.toLocaleString()} rows received`);
          }
        }

        // Track deltas per second
        // MEMORY FIX: Mutate in place instead of creating new array
        const now = Date.now();
        deltaWindowRef.current.push(now);
        // Remove old timestamps in place (from start)
        while (deltaWindowRef.current.length > 0 && now - deltaWindowRef.current[0] >= 1000) {
          deltaWindowRef.current.shift();
        }

        // DOM NODE FIX: Use throttled stats update to reduce re-renders
        totalRowsProcessedRef.current += deltas.length;
        updateStatsThrottled({
          totalDeltas: deltaCountRef.current,
          deltasPerSecond: deltaWindowRef.current.length * deltas.length,
          lastDeltaBatchMs: processTime,
          totalRowsProcessed: totalRowsProcessedRef.current,
        });
      }
    };

    ws.onclose = () => {
      setStats(s => ({ ...s, state: 'disconnected' }));
      setIsBurstInProgress(false);
      console.log('[App] Disconnected');
    };

    ws.onerror = (err) => {
      console.error('[App] WebSocket error:', err);
      setStats(s => ({ ...s, state: 'disconnected' }));
      setIsBurstInProgress(false);
    };
  }, [pendingOrders, highValueOrders, ordersByRegion, ordersByStatus, ordersByCategory, weightedAvgByCategory, ordersByCustomer]);

  // ============ DISCONNECT ============
  const disconnect = useCallback(() => {
    wsRef.current?.close();
    wsRef.current = null;
  }, []);

  // ============ CONTROLS ============
  const sendControl = useCallback((msg: object) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(msg));
    }
  }, []);

  useEffect(() => {
    sendControl({ type: 'set-rate', rate: deltaRate });
  }, [deltaRate, sendControl]);

  useEffect(() => {
    sendControl({ type: 'set-batch-size', size: batchSize });
  }, [batchSize, sendControl]);

  const togglePause = useCallback(() => {
    if (isPaused) {
      sendControl({ type: 'resume' });
      setIsPaused(false);
    } else {
      sendControl({ type: 'pause' });
      setIsPaused(true);
    }
  }, [isPaused, sendControl]);

  const burstTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  
  const sendBurst = useCallback(() => {
    if (isBurstInProgress) return;
    setIsBurstInProgress(true);
    sendControl({ type: 'burst', size: burstSize });
    
    // Safety timeout: reset burst state after 30 seconds if not completed
    if (burstTimeoutRef.current) {
      clearTimeout(burstTimeoutRef.current);
    }
    burstTimeoutRef.current = setTimeout(() => {
      setIsBurstInProgress(false);
      console.warn('[App] Burst timed out after 30 seconds');
    }, 30000);
  }, [burstSize, sendControl, isBurstInProgress]);

  // Cleanup
  useEffect(() => {
    return () => {
      wsRef.current?.close();
      // Clear throttled stats timeout to prevent updates after unmount
      if (statsUpdateTimeoutRef.current) {
        clearTimeout(statsUpdateTimeoutRef.current);
      }
    };
  }, []);

  // ============ HELPERS ============
  const fmt = (n: number) => n.toLocaleString();
  const fmtMs = (ms: number) => {
    if (ms < 1) return `${(ms * 1000).toFixed(0)}μs`;
    if (ms < 1000) return `${ms.toFixed(2)}ms`;
    return `${(ms / 1000).toFixed(2)}s`;
  };
  
  // Logarithmic slider helpers (1 to 10,000)
  const logMin = 0;  // log10(1)
  const logMax = 4;  // log10(10000)
  const sliderToBatchSize = (sliderVal: number) => {
    // Slider 0-100 maps to 10^0 to 10^4 (1 to 10000)
    const logValue = logMin + (sliderVal / 100) * (logMax - logMin);
    return Math.round(Math.pow(10, logValue));
  };
  const batchSizeToSlider = (batchSize: number) => {
    // Batch size 1-10000 maps to slider 0-100
    const logValue = Math.log10(Math.max(1, batchSize));
    return ((logValue - logMin) / (logMax - logMin)) * 100;
  };

  // Get active view data
  const getActiveViewData = () => {
    switch (activeView) {
      case 'pending':
        return {
          name: 'Pending Orders',
          query: "SELECT * FROM data WHERE status = 'pending'",
          results: pendingOrders.results,
          count: pendingOrders.count,
          stats: pendingOrders.stats,
          isRecent: false,
        };
      case 'highValue':
        return {
          name: 'High Value Orders (>$300)',
          query: 'SELECT * FROM data WHERE amount > 300',
          results: highValueOrders.results,
          count: highValueOrders.count,
          stats: highValueOrders.stats,
          isRecent: false,
        };
      case 'recent':
        return {
          name: 'Recent Updates',
          query: 'Streaming delta updates (newest first)',
          results: recentRows,
          count: recentRows.length,
          stats: pendingOrders.stats,
          isRecent: true,
        };
    }
  };

  const activeData = getActiveViewData();

  return (
    <div className="stress-app">
      {/* ============ HEADER ============ */}
      <header className="stress-header">
        <div className="header-content">
          <h1 className="stress-title">
            <span className="title-icon">●</span>
            DBSP Real-Time Monitor
          </h1>
          <p className="stress-subtitle">
            100K Snapshot • Delta Stream • 7 SQL Views with Complex Expressions
          </p>
        </div>
        <div className={`connection-badge ${stats.state}`}>
          <span className="connection-dot"></span>
          {stats.state === 'connected' ? 'LIVE' : 
           stats.state === 'connecting' ? 'CONNECTING' : 'OFFLINE'}
        </div>
      </header>

      {/* ============ MAIN GRID ============ */}
      <main className="stress-main">
        {/* LEFT: Controls */}
        <section className="panel control-panel">
          <h2 className="panel-title">Controls</h2>

          {/* Connection */}
          <div className="control-group">
            <label className="control-label">Connection</label>
            <div className="button-row">
              {stats.state !== 'connected' ? (
                <button 
                  className="btn btn-connect"
                  onClick={connect}
                  disabled={stats.state === 'connecting'}
                >
                  {stats.state === 'connecting' ? 'Connecting...' : 'Connect'}
                </button>
              ) : (
                <button className="btn btn-disconnect" onClick={disconnect}>
                  Disconnect
                </button>
              )}
            </div>
          </div>

          {/* Delta Rate */}
          <div className="control-group">
            <label className="control-label">
              Delta Rate: <strong>{deltaRate}/s</strong>
            </label>
            <input
              type="range"
              min="1"
              max="60"
              value={deltaRate}
              onChange={(e) => setDeltaRate(Number(e.target.value))}
              className="slider"
            />
            <div className="slider-labels">
              <span>1/s</span>
              <span>60/s</span>
            </div>
          </div>

          {/* Batch Size (Logarithmic) */}
          <div className="control-group">
            <label className="control-label">
              Batch Size: <strong>{fmt(batchSize)} rows</strong>
            </label>
            <input
              type="range"
              min="0"
              max="100"
              step="1"
              value={batchSizeToSlider(batchSize)}
              onChange={(e) => setBatchSize(sliderToBatchSize(Number(e.target.value)))}
              className="slider"
            />
            <div className="slider-labels">
              <span>1</span>
              <span>10</span>
              <span>100</span>
              <span>1K</span>
              <span>10K</span>
            </div>
          </div>

          {/* Throughput */}
          <div className="throughput-display">
            <span className="throughput-value">{fmt(deltaRate * batchSize)}</span>
            <span className="throughput-label">rows/second</span>
          </div>

          {/* Pause/Resume */}
          <div className="control-group">
            <label className="control-label">Stream Control</label>
            <div className="button-row">
              <button
                className={`btn ${isPaused ? 'btn-resume' : 'btn-pause'}`}
                onClick={togglePause}
                disabled={stats.state !== 'connected'}
              >
                {isPaused ? 'Resume' : 'Pause'}
              </button>
            </div>
          </div>

          {/* Burst */}
          <div className="control-group">
            <label className="control-label">
              Burst: <strong>{fmt(burstSize)} rows</strong>
            </label>
            <input
              type="range"
              min="100"
              max="50000"
              step="100"
              value={burstSize}
              onChange={(e) => setBurstSize(Number(e.target.value))}
              className="slider"
              disabled={isBurstInProgress}
            />
            <div className="slider-labels">
              <span>100</span>
              <span>50K</span>
            </div>
            <button
              className={`btn btn-burst ${isBurstInProgress ? 'btn-burst-active' : ''}`}
              onClick={sendBurst}
              disabled={stats.state !== 'connected' || isBurstInProgress}
            >
              {isBurstInProgress 
                ? `Sending ${fmt(burstSize)} rows...`
                : 'Send Burst'}
            </button>
          </div>
          
          {/* Freshness Stats */}
          <div className="control-group freshness-stats">
            <label className="control-label">Freshness Status</label>
            <div className="freshness-grid">
              <div className={`freshness-item ${pendingOrders.stats.isLagging ? 'lagging' : 'fresh'}`}>
                <span className="freshness-label">Lag</span>
                <span className="freshness-value">{pendingOrders.stats.lagMs.toFixed(0)}ms</span>
              </div>
              <div className="freshness-item">
                <span className="freshness-label">Buffer</span>
                <span className="freshness-value">{(pendingOrders.stats.bufferUtilization * 100).toFixed(0)}%</span>
              </div>
              <div className={`freshness-item ${pendingOrders.stats.totalDropped > 0 ? 'warning' : ''}`}>
                <span className="freshness-label">Dropped</span>
                <span className="freshness-value">{pendingOrders.stats.totalDropped}</span>
              </div>
              <div className="freshness-item">
                <span className="freshness-label">Avg Time</span>
                <span className="freshness-value">{fmtMs(pendingOrders.stats.avgUpdateMs)}</span>
              </div>
            </div>
            {pendingOrders.stats.isLagging && (
              <div className="freshness-warning">
                Processing lag detected - oldest data may be dropped
              </div>
            )}
          </div>
        </section>

        {/* CENTER: Stats */}
        <section className="panel stats-panel">
          <h2 className="panel-title">Live Aggregations</h2>

          {/* Global Totals - Computed from regional rollups */}
          {(() => {
            // Compute global totals from regional aggregations (demonstrates derived metrics)
            const totalRevenue = ordersByRegion.results.reduce((sum, r) => sum + ((r.total_amount as number) || 0), 0);
            const totalOrders = ordersByRegion.results.reduce((sum, r) => sum + ((r.order_count as number) || 0), 0);
            const avgOrder = totalOrders > 0 ? totalRevenue / totalOrders : 0;
            const totalUnits = ordersByCategory.results.reduce((sum, c) => sum + ((c.total_quantity as number) || 0), 0);
            
            return (
              <div className="global-totals">
                <div className="total-card primary">
                  <span className="total-value">${totalRevenue.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                  <span className="total-label">Total Revenue</span>
                </div>
                <div className="total-card">
                  <span className="total-value">{fmt(totalOrders)}</span>
                  <span className="total-label">Total Orders</span>
                </div>
                <div className="total-card">
                  <span className="total-value">${avgOrder.toFixed(2)}</span>
                  <span className="total-label">Avg Order</span>
                </div>
                <div className="total-card">
                  <span className="total-value">{fmt(totalUnits)}</span>
                  <span className="total-label">Units Sold</span>
                </div>
              </div>
            );
          })()}

          {/* Status Breakdown - GROUP BY status */}
          <div className="aggregation-section">
            <h3 className="section-title">
              By Status
              <SQLPopover sql={`SELECT status,
       SUM(amount) AS total_amount,
       COUNT(*) AS order_count,
       AVG(amount) AS avg_amount
FROM data
GROUP BY status`} />
            </h3>
            <div className="status-breakdown">
              {ordersByStatus.results.map((s) => (
                <div key={s.status} className={`status-row ${s.status}`}>
                  <span className={`status-indicator ${s.status}`}></span>
                  <span className="status-name">{s.status}</span>
                  <span className="status-count">{fmt((s.order_count as number) || 0)}</span>
                  <span className="status-amount">${((s.total_amount as number) || 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Weighted Average by Category - demonstrates SUM(amount * quantity) */}
          <div className="aggregation-section">
            <h3 className="section-title">
              Weighted Avg by Category
              <SQLPopover sql={`SELECT category,
       SUM(amount * quantity) AS weighted_total,
       SUM(quantity) AS total_weight
FROM data
GROUP BY category

-- Weighted Avg = weighted_total / total_weight`} />
            </h3>
            <div className="weighted-avg-grid">
              {weightedAvgByCategory.results.map((w) => {
                const weightedTotal = (w.weighted_total as number) || 0;
                const totalWeight = (w.total_weight as number) || 0;
                const weightedAvg = totalWeight > 0 ? weightedTotal / totalWeight : 0;
                return (
                  <div key={w.category} className="weighted-card">
                    <span className="weighted-category">{w.category}</span>
                    <span className="weighted-value">${weightedAvg.toFixed(2)}</span>
                    <span className="weighted-meta">{fmt(totalWeight)} units • ${fmt(Math.round(weightedTotal))}</span>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Top Customers - filter on aggregation result */}
          <div className="aggregation-section">
            <h3 className="section-title">
              Top Customers
              <SQLPopover sql={`SELECT customerId,
       SUM(amount) AS total_spent,
       COUNT(*) AS order_count,
       AVG(amount) AS avg_order
FROM data
GROUP BY customerId

-- Then filtered where total_spent > 1000`} />
            </h3>
            <div className="top-spenders">
              {topCustomers.slice(0, 5).map((c) => (
                <div key={c.customerId} className="spender-row">
                  <span className="spender-id">Customer #{c.customerId}</span>
                  <span className="spender-orders">{(c.order_count as number) || 0} orders</span>
                  <span className="spender-total">${((c.total_spent as number) || 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                </div>
              ))}
              {topCustomers.length > 5 && (
                <div className="spender-overflow">+{topCustomers.length - 5} more high spenders</div>
              )}
            </div>
          </div>

          {/* Regional Breakdown - GROUP BY region */}
          <div className="aggregation-section">
            <h3 className="section-title">
              By Region
              <SQLPopover sql={`SELECT region,
       SUM(amount) AS total_amount,
       COUNT(*) AS order_count,
       AVG(amount) AS avg_amount
FROM data
GROUP BY region`} />
            </h3>
            <div className="region-cards">
              {ordersByRegion.results.map((r) => (
                <div key={r.region} className={`region-card ${(r.region || '').toLowerCase()}`}>
                  <span className="region-name">{r.region}</span>
                  <span className="region-total">${((r.total_amount as number) || 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                  <span className="region-avg">Avg: ${((r.avg_amount as number) || 0).toFixed(0)}</span>
                  <span className="region-count">{((r.order_count as number) || 0).toLocaleString()} orders</span>
                </div>
              ))}
            </div>
          </div>

          {/* Performance Stats */}
          <div className="perf-section">
            <div className="perf-stats">
              <div className="perf-stat">
                <span className="perf-stat-value">{fmtMs(stats.lastDeltaBatchMs)}</span>
                <span className="perf-stat-label">Last Batch</span>
              </div>
              <div className="perf-stat">
                <span className="perf-stat-value">{fmt(stats.deltasPerSecond)}</span>
                <span className="perf-stat-label">Rows/sec</span>
              </div>
              <div className="perf-stat">
                <span className="perf-stat-value">{fmtMs(pendingOrders.stats.avgUpdateMs)}</span>
                <span className="perf-stat-label">DBSP Avg</span>
              </div>
            </div>
            <div className="perf-indicator">
              <div className="perf-bar-container">
                <div
                  className="perf-bar"
                  style={{
                    width: `${Math.min(100, (pendingOrders.stats.avgUpdateMs / 10) * 100)}%`,
                    background: pendingOrders.stats.avgUpdateMs < 1 ? 'var(--positive)' :
                                pendingOrders.stats.avgUpdateMs < 5 ? 'var(--warning)' :
                                'var(--negative)',
                  }}
                ></div>
              </div>
              <span className="perf-label">
                {pendingOrders.stats.avgUpdateMs < 1 ? 'Excellent' :
                 pendingOrders.stats.avgUpdateMs < 5 ? 'Good' :
                 'Slow'}
              </span>
            </div>
          </div>
        </section>

        {/* RIGHT: Output View */}
        <section className="panel output-panel">
          <h2 className="panel-title">
            Live View
            <span className="view-count">{fmt(activeData?.count || 0)} rows</span>
          </h2>

          {/* View Selector */}
          <div className="view-selector">
            <button
              className={`view-tab ${activeView === 'recent' ? 'active' : ''}`}
              onClick={() => setActiveView('recent')}
            >
              Recent
            </button>
            <button
              className={`view-tab ${activeView === 'pending' ? 'active' : ''}`}
              onClick={() => setActiveView('pending')}
            >
              Pending
            </button>
            <button
              className={`view-tab ${activeView === 'highValue' ? 'active' : ''}`}
              onClick={() => setActiveView('highValue')}
            >
              High Value
            </button>
          </div>

          <div className="sql-display">
            <code>{activeData?.query || ''}</code>
          </div>

          <div className="output-table-container">
            <table className="output-table">
              <thead>
                <tr>
                  {activeData?.isRecent && <th>Age</th>}
                  <th>Order ID</th>
                  <th>Customer</th>
                  <th>Amount</th>
                  <th>Region</th>
                  <th>Category</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {(activeData?.results || []).slice(0, 20).map((order) => {
                  const addedAt = (order as Order & { _addedAt?: number })._addedAt;
                  const ageMs = addedAt ? Date.now() - addedAt : 0;
                  const isNew = ageMs < 2000;
                  
                  return (
                    <tr key={order.orderId} className={`order-row ${isNew && activeData?.isRecent ? 'new-row' : ''}`}>
                      {activeData?.isRecent && (
                        <td className="age-cell">
                          <span className={`age-badge ${isNew ? 'new' : ''}`}>
                            {ageMs < 1000 ? 'NOW' : ageMs < 60000 ? `${Math.floor(ageMs / 1000)}s` : `${Math.floor(ageMs / 60000)}m`}
                          </span>
                        </td>
                      )}
                      <td className="mono">{order.orderId}</td>
                      <td>{order.customerId}</td>
                      <td className="amount">${order.amount.toFixed(2)}</td>
                      <td><span className={`region-badge ${order.region.toLowerCase()}`}>{order.region}</span></td>
                      <td>{order.category}</td>
                      <td><span className={`status-badge ${order.status}`}>{order.status}</span></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {(activeData?.count || 0) > 20 && (
              <div className="table-overflow">
                ... and {fmt((activeData?.count || 0) - 20)} more rows
              </div>
            )}
          </div>
        </section>
      </main>

      {/* ============ FOOTER ============ */}
      <footer className="stress-footer">
        <div className="theory-cards">
          <div className="theory-card">
            <h3>GROUP BY + Aggregates</h3>
            <p>SUM, COUNT, AVG, MIN, MAX incrementally per group.</p>
            <code>{`SELECT region, SUM(amount), COUNT(*) FROM data GROUP BY region`}</code>
          </div>
          <div className="theory-card">
            <h3>Complex Expressions</h3>
            <p>Arithmetic in aggregates: SUM(a*b), SUM(a+b), etc.</p>
            <code>{`SELECT category, SUM(amount * quantity) AS weighted_total`}</code>
          </div>
          <div className="theory-card">
            <h3>Incremental Processing</h3>
            <p>DBSP only processes deltas, not full data.</p>
            <code>Δ(Q(R)) = Q(ΔR)</code>
          </div>
        </div>
        <p className="footer-links">
          <a href="https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf" target="_blank" rel="noopener">DBSP Paper</a>
          {' • '}
          <a href="https://github.com/feldera/feldera" target="_blank" rel="noopener">Feldera</a>
          {' • '}
          <span className="sql-count">7 Live SQL Views</span>
        </p>
      </footer>
    </div>
  );
}

export default App;
