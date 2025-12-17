import { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import { useFreshnessDBSP } from '../dbsp/react/useFreshnessDBSP';
import { useJoinDBSP } from '../dbsp/react/useJoinDBSP';
import type { JoinMode } from '../dbsp/react/useJoinDBSP';
import { IncrementalSemiJoinState, IncrementalAntiJoinState } from '../dbsp/advanced-joins';
import { SQLPopover, Header, Footer } from '../components';
import type {
  Order,
  Customer,
  DeltaOp,
  CustomerDeltaOp,
  ConnectionStats,
  RegionalSummary,
  StatusSummary,
  CategorySummary,
  WeightedAvgSummary,
  CustomerSummary,
} from '../types';

// ============ CONSTANTS ============
const WS_URL = 'ws://localhost:8765';

export function DashboardPage() {
  // ============ FRESHNESS CONFIG ============
  const freshnessConfig = {
    maxBufferSize: 5000,
    maxMessageAgeMs: 2000,
    maxBatchSize: 500,
    processingIntervalMs: 100,
  };
  
  const handleDrop = useCallback((count: number, reason: 'overflow' | 'stale') => {
    console.log(`[Dashboard] Dropped ${count} messages: ${reason}`);
  }, []);
  
  // ============ DBSP HOOKS ============
  const pendingOrders = useFreshnessDBSP<Order>(
    "SELECT * FROM data WHERE status = 'pending'",
    { key: 'orderId', ...freshnessConfig, onDrop: handleDrop }
  );
  
  const highValueOrders = useFreshnessDBSP<Order>(
    "SELECT * FROM data WHERE amount > 300",
    { key: 'orderId', ...freshnessConfig, onDrop: handleDrop }
  );
  
  const ordersByRegion = useFreshnessDBSP<Order, RegionalSummary>(
    "SELECT region, SUM(amount) AS total_amount, COUNT(*) AS order_count, AVG(amount) AS avg_amount FROM data GROUP BY region",
    { key: 'orderId', outputKey: 'region', ...freshnessConfig, onDrop: handleDrop, debug: false }
  );

  const ordersByStatus = useFreshnessDBSP<Order, StatusSummary>(
    "SELECT status, SUM(amount) AS total_amount, COUNT(*) AS order_count, AVG(amount) AS avg_amount FROM data GROUP BY status",
    { key: 'orderId', outputKey: 'status', ...freshnessConfig, onDrop: handleDrop }
  );

  const ordersByCategory = useFreshnessDBSP<Order, CategorySummary>(
    "SELECT category, SUM(amount) AS total_amount, COUNT(*) AS order_count, AVG(amount) AS avg_amount, SUM(quantity) AS total_quantity FROM data GROUP BY category",
    { key: 'orderId', outputKey: 'category', ...freshnessConfig, onDrop: handleDrop }
  );

  const weightedAvgByCategory = useFreshnessDBSP<Order, WeightedAvgSummary>(
    "SELECT category, SUM(amount * quantity) AS weighted_total, SUM(quantity) AS total_weight FROM data GROUP BY category",
    { key: 'orderId', outputKey: 'category', ...freshnessConfig, onDrop: handleDrop }
  );

  const ordersByCustomer = useFreshnessDBSP<Order, CustomerSummary>(
    "SELECT customerId, SUM(amount) AS total_spent, COUNT(*) AS order_count, AVG(amount) AS avg_order, SUM(quantity) AS total_quantity FROM data GROUP BY customerId",
    { key: 'orderId', outputKey: 'customerId', ...freshnessConfig, onDrop: handleDrop }
  );

  const topCustomers = useMemo(() => {
    return ordersByCustomer.results
      .filter(c => (c.total_spent as number) > 1000)
      .sort((a, b) => ((b.total_spent as number) || 0) - ((a.total_spent as number) || 0))
      .slice(0, 10);
  }, [ordersByCustomer.results]);

  // ============ JOIN OPTIMIZATION MODE ============
  const [joinMode, setJoinMode] = useState<JoinMode>('append-only');
  
  const ordersWithCustomers = useJoinDBSP<Order, Customer>({
    leftKey: 'orderId',
    rightKey: 'customerId',
    leftJoinKey: 'customerId',
    rightJoinKey: 'customerId',
    processingIntervalMs: 100,
    mode: joinMode,
    debug: false,
  });
  
  // ============ SEMI-JOIN & ANTI-JOIN ============
  const semiJoinRef = useRef(new IncrementalSemiJoinState<Order, Customer>(
    (o) => String(o.orderId),
    (o) => String(o.customerId),
    (c) => String(c.customerId)
  ));
  const [validOrderCount, setValidOrderCount] = useState(0);
  
  const antiJoinRef = useRef(new IncrementalAntiJoinState<Order, Customer>(
    (o) => String(o.orderId),
    (o) => String(o.customerId),
    (c) => String(c.customerId)
  ));
  const [orphanedOrderCount, setOrphanedOrderCount] = useState(0);

  const revenueByTier = useMemo(() => {
    const tierMap = new Map<string, { total: number; count: number; orders: number }>();
    
    for (const [order, customer] of ordersWithCustomers.results) {
      const tier = customer.tier;
      const existing = tierMap.get(tier) || { total: 0, count: 0, orders: 0 };
      existing.total += order.amount;
      existing.count += 1;
      existing.orders += 1;
      tierMap.set(tier, existing);
    }
    
    const tierOrder = ['platinum', 'gold', 'silver', 'bronze'];
    return tierOrder
      .filter(tier => tierMap.has(tier))
      .map(tier => ({
        tier,
        ...tierMap.get(tier)!,
        avg: tierMap.get(tier)!.total / tierMap.get(tier)!.count,
      }));
  }, [ordersWithCustomers.results, ordersWithCustomers.count]);

  // ============ CONNECTION STATE ============
  const [stats, setStats] = useState<ConnectionStats>({
    state: 'disconnected',
    snapshotLoaded: false,
    snapshotSize: 0,
    customerSnapshotSize: 0,
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
  const [isBurstInProgress, setIsBurstInProgress] = useState(false);
  const [recentRows, setRecentRows] = useState<Array<Order & { _addedAt: number }>>([]);
  const [, forceUpdate] = useState(0);
  
  useEffect(() => {
    const interval = setInterval(() => forceUpdate(n => n + 1), 5000);
    return () => clearInterval(interval);
  }, []);

  // ============ REFS ============
  const wsRef = useRef<WebSocket | null>(null);
  const snapshotStartRef = useRef(0);
  const snapshotChunksRef = useRef<Order[]>([]);
  const customerSnapshotChunksRef = useRef<Customer[]>([]);
  const deltaCountRef = useRef(0);
  const deltaWindowRef = useRef<number[]>([]);
  const pendingStatsRef = useRef<Partial<ConnectionStats>>({});
  const statsUpdateTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const totalRowsProcessedRef = useRef(0);
  const burstTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  
  const updateStatsThrottled = useCallback((updates: Partial<ConnectionStats>) => {
    Object.assign(pendingStatsRef.current, updates);
    if (!statsUpdateTimeoutRef.current) {
      statsUpdateTimeoutRef.current = setTimeout(() => {
        setStats(s => ({ ...s, ...pendingStatsRef.current }));
        pendingStatsRef.current = {};
        statsUpdateTimeoutRef.current = null;
      }, 100);
    }
  }, []);

  // ============ CONNECT ============
  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    setStats(s => ({ ...s, state: 'connecting' }));
    snapshotChunksRef.current = [];
    customerSnapshotChunksRef.current = [];
    snapshotStartRef.current = performance.now();
    deltaCountRef.current = 0;

    pendingOrders.clear();
    highValueOrders.clear();
    ordersByRegion.clear();
    ordersByStatus.clear();
    ordersByCategory.clear();
    weightedAvgByCategory.clear();
    ordersByCustomer.clear();
    ordersWithCustomers.clear();
    setRecentRows([]);
    
    semiJoinRef.current.clear();
    antiJoinRef.current.clear();
    setValidOrderCount(0);
    setOrphanedOrderCount(0);

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      setStats(s => ({ ...s, state: 'connected' }));
      console.log('[Dashboard] Connected to server');
    };

    ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);

      if (msg.type === 'customer-snapshot-chunk') {
        customerSnapshotChunksRef.current.push(...msg.data);

        if (msg.isLast) {
          const customers = customerSnapshotChunksRef.current;
          console.log(`[Dashboard] Received ${customers.length.toLocaleString()} customers`);
          
          ordersWithCustomers.pushRight(customers);
          
          for (const c of customers) {
            semiJoinRef.current.insertRight(c);
            antiJoinRef.current.insertRight(c);
          }
          
          setStats(s => ({
            ...s,
            customerSnapshotSize: customers.length,
          }));
          
          customerSnapshotChunksRef.current = [];
        }
      }
      else if (msg.type === 'snapshot-chunk') {
        snapshotChunksRef.current.push(...msg.data);

        if (msg.isLast) {
          const snapshotTime = performance.now() - snapshotStartRef.current;
          const orders = snapshotChunksRef.current;

          console.log(`[Dashboard] Received ${orders.length.toLocaleString()} orders in ${snapshotTime.toFixed(0)}ms`);

          const loadStart = performance.now();
          
          pendingOrders.push(orders);
          highValueOrders.push(orders);
          ordersByRegion.push(orders);
          ordersByStatus.push(orders);
          ordersByCategory.push(orders);
          weightedAvgByCategory.push(orders);
          ordersByCustomer.push(orders);
          ordersWithCustomers.pushLeft(orders);
          
          for (const o of orders) {
            semiJoinRef.current.insertLeft(o);
            antiJoinRef.current.insertLeft(o);
          }
          setValidOrderCount(semiJoinRef.current.count);
          setOrphanedOrderCount(antiJoinRef.current.count);
          
          const loadTime = performance.now() - loadStart;

          console.log(`[Dashboard] Loaded into DBSP in ${loadTime.toFixed(0)}ms`);

          totalRowsProcessedRef.current = orders.length;
          setStats(s => ({
            ...s,
            snapshotLoaded: true,
            snapshotSize: orders.length,
            snapshotTimeMs: snapshotTime + loadTime,
            totalRowsProcessed: totalRowsProcessedRef.current,
          }));
          
          snapshotChunksRef.current = [];
        }
      } else if (msg.type === 'delta') {
        const processStart = performance.now();
        const deltas = msg.data as DeltaOp[];
        const customerDeltas = (msg.customerData || []) as CustomerDeltaOp[];

        const updates: Order[] = [];
        const deleteKeys: number[] = [];

        for (const delta of deltas) {
          if (delta.op === 'insert' || delta.op === 'update') {
            if (delta.row) updates.push(delta.row);
          } else if (delta.op === 'delete') {
            if (delta.orderId !== undefined) deleteKeys.push(delta.orderId);
          }
        }

        if (updates.length > 0) {
          pendingOrders.push(updates);
          highValueOrders.push(updates);
          ordersByRegion.push(updates);
          ordersByStatus.push(updates);
          ordersByCategory.push(updates);
          weightedAvgByCategory.push(updates);
          ordersByCustomer.push(updates);
          ordersWithCustomers.pushLeft(updates);
          
          for (const o of updates) {
            semiJoinRef.current.insertLeft(o);
            antiJoinRef.current.insertLeft(o);
          }
          
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
          ordersWithCustomers.removeLeft(String(key));
          semiJoinRef.current.removeLeft(String(key));
          setRecentRows(prev => prev.filter(r => r.orderId !== key));
        }
        
        setValidOrderCount(semiJoinRef.current.count);
        setOrphanedOrderCount(antiJoinRef.current.count);

        const customerUpdates: Customer[] = [];
        const customerDeleteKeys: number[] = [];

        for (const delta of customerDeltas) {
          if (delta.op === 'insert' || delta.op === 'update') {
            if (delta.row) customerUpdates.push(delta.row);
          } else if (delta.op === 'delete') {
            if (delta.customerId !== undefined) customerDeleteKeys.push(delta.customerId);
          }
        }

        if (customerUpdates.length > 0) {
          ordersWithCustomers.pushRight(customerUpdates);
          for (const c of customerUpdates) {
            semiJoinRef.current.insertRight(c);
            antiJoinRef.current.insertRight(c);
          }
        }

        for (const key of customerDeleteKeys) {
          ordersWithCustomers.removeRight(String(key));
          const deletedCustomer = { customerId: key } as Customer;
          semiJoinRef.current.removeRight(deletedCustomer);
          antiJoinRef.current.removeRight(deletedCustomer);
        }
        
        setValidOrderCount(semiJoinRef.current.count);
        setOrphanedOrderCount(antiJoinRef.current.count);

        const processTime = performance.now() - processStart;
        deltaCountRef.current += deltas.length;

        if (msg.isBurst) {
          const progress = msg.burstProgress || 0;
          const total = msg.burstTotal || 0;
          
          if (progress >= total) {
            setIsBurstInProgress(false);
            if (burstTimeoutRef.current) {
              clearTimeout(burstTimeoutRef.current);
              burstTimeoutRef.current = null;
            }
            console.log(`[Dashboard] Burst complete: ${total.toLocaleString()} rows received`);
          }
        }

        const now = Date.now();
        deltaWindowRef.current.push(now);
        while (deltaWindowRef.current.length > 0 && now - deltaWindowRef.current[0] >= 1000) {
          deltaWindowRef.current.shift();
        }

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
      console.log('[Dashboard] Disconnected');
    };

    ws.onerror = (err) => {
      console.error('[Dashboard] WebSocket error:', err);
      setStats(s => ({ ...s, state: 'disconnected' }));
      setIsBurstInProgress(false);
    };
  }, [pendingOrders, highValueOrders, ordersByRegion, ordersByStatus, ordersByCategory, weightedAvgByCategory, ordersByCustomer, ordersWithCustomers, updateStatsThrottled]);

  const disconnect = useCallback(() => {
    wsRef.current?.close();
    wsRef.current = null;
  }, []);

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

  const sendBurst = useCallback(() => {
    if (isBurstInProgress) return;
    setIsBurstInProgress(true);
    sendControl({ type: 'burst', size: burstSize });
    
    if (burstTimeoutRef.current) {
      clearTimeout(burstTimeoutRef.current);
    }
    burstTimeoutRef.current = setTimeout(() => {
      setIsBurstInProgress(false);
      console.warn('[Dashboard] Burst timed out after 30 seconds');
    }, 30000);
  }, [burstSize, sendControl, isBurstInProgress]);

  useEffect(() => {
    return () => {
      wsRef.current?.close();
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
  
  const logMin = 0;
  const logMax = 4;
  const sliderToBatchSize = (sliderVal: number) => {
    const logValue = logMin + (sliderVal / 100) * (logMax - logMin);
    return Math.round(Math.pow(10, logValue));
  };
  const batchSizeToSlider = (batchSize: number) => {
    const logValue = Math.log10(Math.max(1, batchSize));
    return ((logValue - logMin) / (logMax - logMin)) * 100;
  };

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
      <Header connectionState={stats.state} />

      <main className="stress-main">
        {/* LEFT: Controls */}
        <section className="panel control-panel">
          <h2 className="panel-title">Controls</h2>

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

          <div className="throughput-display">
            <span className="throughput-value">{fmt(deltaRate * batchSize)}</span>
            <span className="throughput-label">rows/second</span>
          </div>

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

          {/* Global Totals */}
          {(() => {
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

          {/* Status Breakdown */}
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

          {/* Weighted Average */}
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

          {/* Top Customers */}
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

          {/* JOIN DEMO */}
          <div className="aggregation-section join-demo">
            <h3 className="section-title">
              <span className="join-icon">⋈</span>
              Revenue by Customer Tier (JOIN)
              <SQLPopover sql={`-- Bilinear Join: Δ(a ⋈ b) = Δa ⋈ Δb + a ⋈ Δb + Δa ⋈ b

SELECT c.tier, 
       SUM(o.amount) AS total_revenue,
       COUNT(*) AS order_count,
       AVG(o.amount) AS avg_order
FROM orders o
JOIN customers c ON o.customerId = c.customerId
GROUP BY c.tier

-- When customer tier changes, join result updates automatically!`} />
            </h3>
            
            <div className="join-mode-selector">
              <span className="mode-label">Optimization:</span>
              <div className="mode-buttons">
                <button 
                  className={`mode-btn ${joinMode === 'indexed' ? 'active' : ''}`}
                  onClick={() => {
                    if (joinMode !== 'indexed') {
                      setJoinMode('indexed');
                      disconnect();
                      setTimeout(connect, 100);
                    }
                  }}
                  title="Full incremental with update/delete support (~100x speedup)"
                >
                  Indexed
                </button>
                <button 
                  className={`mode-btn ${joinMode === 'append-only' ? 'active' : ''}`}
                  onClick={() => {
                    if (joinMode !== 'append-only') {
                      setJoinMode('append-only');
                      disconnect();
                      setTimeout(connect, 100);
                    }
                  }}
                  title="Insert-only workloads (~3000x speedup!)"
                >
                  🚀 Append-Only
                </button>
              </div>
              <span className={`mode-speedup ${joinMode === 'append-only' ? 'fast' : ''}`}>
                {joinMode === 'append-only' ? '3000x faster' : '100x faster'}
              </span>
            </div>
            
            <div className="join-stats">
              <div className="join-stat">
                <span className="join-stat-value">{fmt(ordersWithCustomers.leftCount)}</span>
                <span className="join-stat-label">Orders</span>
              </div>
              <div className="join-stat join-symbol">⋈</div>
              <div className="join-stat">
                <span className="join-stat-value">{fmt(ordersWithCustomers.rightCount)}</span>
                <span className="join-stat-label">Customers</span>
              </div>
              <div className="join-stat join-equals">=</div>
              <div className="join-stat highlight">
                <span className="join-stat-value">{fmt(ordersWithCustomers.count)}</span>
                <span className="join-stat-label">Joined</span>
              </div>
            </div>

            <div className="tier-breakdown">
              {revenueByTier.map((tier) => (
                <div key={tier.tier} className={`tier-card ${tier.tier}`}>
                  <div className="tier-header">
                    <span className={`tier-badge ${tier.tier}`}>{tier.tier.toUpperCase()}</span>
                    <span className="tier-orders">{fmt(tier.orders)} orders</span>
                  </div>
                  <div className="tier-revenue">
                    ${tier.total.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                  </div>
                  <div className="tier-avg">
                    Avg: ${tier.avg.toFixed(2)}
                  </div>
                </div>
              ))}
              {revenueByTier.length === 0 && (
                <div className="tier-empty">Loading join results...</div>
              )}
            </div>
            
            <div className="join-perf">
              <span className="join-perf-label">Join Update:</span>
              <span className="join-perf-value">{fmtMs(ordersWithCustomers.stats.avgUpdateMs)}</span>
              <span className="join-perf-mode">Mode: {ordersWithCustomers.mode}</span>
            </div>
          </div>
          
          {/* Advanced Joins */}
          <div className="aggregation-section advanced-joins">
            <h3 className="section-title">
              Advanced Join Variants
              <SQLPopover sql={`-- SEMI-JOIN: Orders with valid customers
SELECT o.* FROM orders o
WHERE EXISTS (
  SELECT 1 FROM customers c 
  WHERE c.customerId = o.customerId
)

-- ANTI-JOIN: Orphaned orders (no customer)
SELECT o.* FROM orders o
WHERE NOT EXISTS (
  SELECT 1 FROM customers c 
  WHERE c.customerId = o.customerId
)`} />
            </h3>
            
            <div className="advanced-join-cards">
              <div className="advanced-join-card semi">
                <div className="adv-join-icon">∃</div>
                <div className="adv-join-content">
                  <span className="adv-join-name">Semi-Join</span>
                  <span className="adv-join-desc">Orders with valid customers</span>
                  <span className="adv-join-value">{fmt(validOrderCount)}</span>
                  <span className="adv-join-sql">WHERE EXISTS (...)</span>
                </div>
              </div>
              
              <div className={`advanced-join-card anti ${orphanedOrderCount > 0 ? 'has-orphans' : ''}`}>
                <div className="adv-join-icon">∄</div>
                <div className="adv-join-content">
                  <span className="adv-join-name">Anti-Join</span>
                  <span className="adv-join-desc">Orphaned orders</span>
                  <span className={`adv-join-value ${orphanedOrderCount > 0 ? 'warning' : ''}`}>
                    {fmt(orphanedOrderCount)}
                  </span>
                  <span className="adv-join-sql">WHERE NOT EXISTS (...)</span>
                </div>
              </div>
            </div>
            
            {orphanedOrderCount > 0 && (
              <div className="orphan-warning">
                ⚠️ {fmt(orphanedOrderCount)} orders reference non-existent customers
              </div>
            )}
          </div>

          {/* Regional Breakdown */}
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

      <Footer />
    </div>
  );
}

