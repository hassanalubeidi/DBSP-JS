/**
 * Credit Trading Page
 * 
 * Real-time credit trading dashboard with DBSP-powered incremental computations.
 * Demonstrates complex multi-table joins and aggregations for credit trading.
 */

import { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import { useFreshnessDBSP } from '../dbsp/react/useFreshnessDBSP';
import { useJoinDBSP } from '../dbsp/react/useJoinDBSP';
import type { JoinMode } from '../dbsp/react/useJoinDBSP';
import { SQLPopover, StreamFlowVisualization } from '../components';
import type {
  RFQ, Position, FXRate, Benchmark, Signal,
  CreditDeltas, CreditConnectionStats,
  SectorPnL, TraderPnL, DeskPnL,
  RatingExposure, TenorExposure, CounterpartyFlow, SignalPerformance
} from '../types/credit';

const WS_URL = 'ws://localhost:8766';

// ============ FORMAT HELPERS ============

const formatCurrency = (value: number, ccy = 'USD') => {
  const prefix = ccy === 'USD' ? '$' : ccy === 'EUR' ? '€' : ccy === 'GBP' ? '£' : '';
  const abs = Math.abs(value);
  if (abs >= 1e9) return `${prefix}${(value / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${prefix}${(value / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${prefix}${(value / 1e3).toFixed(1)}K`;
  return `${prefix}${value.toFixed(0)}`;
};

const formatPnL = (value: number) => {
  const formatted = formatCurrency(Math.abs(value));
  const sign = value >= 0 ? '+' : '-';
  return `${sign}${formatted}`;
};

const formatPercent = (value: number) => `${(value * 100).toFixed(1)}%`;
const formatBps = (value: number) => `${value.toFixed(1)}bp`;

// Format milliseconds with appropriate units
const formatMs = (ms: number) => {
  if (ms < 0.001) return '<1μs';
  if (ms < 1) return `${(ms * 1000).toFixed(0)}μs`;
  if (ms < 1000) return `${ms.toFixed(2)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
};

// Get performance color based on latency
const getPerfColor = (ms: number) => {
  if (ms < 0.5) return '#00d4aa'; // Excellent - green
  if (ms < 2) return '#00a8ff';   // Good - blue
  if (ms < 5) return '#ffd000';   // Warning - yellow
  return '#ff6b6b';               // Slow - red
};

const getPerfLabel = (ms: number) => {
  if (ms < 0.5) return 'FAST';
  if (ms < 2) return 'GOOD';
  if (ms < 5) return 'SLOW';
  return 'BOTTLENECK';
};

// Rating color scheme
const getRatingColor = (rating: string) => {
  if (rating.startsWith('AAA') || rating.startsWith('AA')) return '#00d4aa';
  if (rating.startsWith('A')) return '#00a8ff';
  if (rating.startsWith('BBB')) return '#ffd000';
  return '#ff6b6b';
};

// ============ FRESHNESS CONFIG ============
const freshnessConfig = {
  maxBufferSize: 50000,
  maxBatchSize: 500,
  processingIntervalMs: 16,
  debug: true, // Enable debug to see what's happening
};

// ============ MAIN COMPONENT ============

function CreditTradingPage() {
  const [stats, setStats] = useState<CreditConnectionStats>({
    connected: false,
    rfqCount: 0,
    positionCount: 0,
    fxUpdateCount: 0,
    benchmarkUpdateCount: 0,
    signalCount: 0,
    deltaRate: 20,
    lastUpdate: 0,
  });

  const [deltaRate, setDeltaRate] = useState(20);
  const [isPaused, setIsPaused] = useState(false);
  const [joinMode, setJoinMode] = useState<JoinMode>('indexed');
  const [showFlowView, setShowFlowView] = useState(false); // Start with dashboard view

  const wsRef = useRef<WebSocket | null>(null);
  const [fxRates, setFxRates] = useState<Map<string, number>>(new Map());
  const [benchmarks, setBenchmarks] = useState<Map<string, Benchmark>>(new Map());

  // ============ DBSP HOOKS WITH SQL QUERIES ============
  // Note: Using simple queries that work with the SQL compiler
  // Adding all aggregated fields needed by the UI

  // All RFQs - basic select
  const allRFQs = useFreshnessDBSP<RFQ>(
    "SELECT * FROM data",
    { key: 'rfqId', ...freshnessConfig }
  );

  // RFQ by counterparty - with spread capture aggregation
  const rfqByCounterparty = useFreshnessDBSP<RFQ, CounterpartyFlow>(
    "SELECT counterparty, COUNT(*) AS rfqCount, SUM(notional) AS totalNotional, SUM(spreadCapture) AS avgSpreadCapture FROM data GROUP BY counterparty",
    { key: 'rfqId', outputKey: 'counterparty', ...freshnessConfig }
  );

  // Position by Sector - with all PnL fields
  const positionBySector = useFreshnessDBSP<Position, SectorPnL>(
    "SELECT sector, SUM(notional) AS notional, SUM(unrealizedPnL) AS unrealizedPnL, SUM(realizedPnL) AS realizedPnL, SUM(dv01) AS dv01, COUNT(*) AS positionCount FROM data GROUP BY sector",
    { key: 'positionId', outputKey: 'sector', ...freshnessConfig }
  );

  // Position by Trader - with desk field (need to include desk in GROUP BY to get it)
  const positionByTrader = useFreshnessDBSP<Position, TraderPnL>(
    "SELECT trader, desk, SUM(notional) AS notional, SUM(unrealizedPnL) AS unrealizedPnL, SUM(realizedPnL) AS realizedPnL, COUNT(*) AS tradeCount FROM data GROUP BY trader, desk",
    { key: 'positionId', outputKey: 'trader', ...freshnessConfig }
  );

  // Position by Desk - with PnL fields  
  const positionByDesk = useFreshnessDBSP<Position, DeskPnL>(
    "SELECT desk, SUM(notional) AS notional, SUM(unrealizedPnL) AS unrealizedPnL, SUM(realizedPnL) AS realizedPnL, COUNT(*) AS tradeCount FROM data GROUP BY desk",
    { key: 'positionId', outputKey: 'desk', ...freshnessConfig }
  );

  // Position by Rating - with DV01
  const positionByRating = useFreshnessDBSP<Position, RatingExposure>(
    "SELECT rating, SUM(notional) AS notional, SUM(dv01) AS dv01, COUNT(*) AS count FROM data GROUP BY rating",
    { key: 'positionId', outputKey: 'rating', ...freshnessConfig }
  );

  // Position by Tenor - with DV01
  const positionByTenor = useFreshnessDBSP<Position, TenorExposure>(
    "SELECT tenor, SUM(notional) AS notional, SUM(dv01) AS dv01, COUNT(*) AS count FROM data GROUP BY tenor",
    { key: 'positionId', outputKey: 'tenor', ...freshnessConfig }
  );

  // Signal by Model - with avgSignal and avgConfidence
  const signalByModel = useFreshnessDBSP<Signal, SignalPerformance>(
    "SELECT model, COUNT(*) AS signalCount, AVG(signalValue) AS avgSignal, AVG(confidence) AS avgConfidence FROM data GROUP BY model",
    { key: 'signalId', outputKey: 'model', ...freshnessConfig }
  );
  
  // All signals
  const allSignals = useFreshnessDBSP<Signal>(
    "SELECT * FROM data",
    { key: 'signalId', ...freshnessConfig }
  );

  // Join: RFQs with Signals (find RFQs that match current model signals)
  const rfqsWithSignals = useJoinDBSP<RFQ, Signal>({
    leftKey: 'rfqId',
    rightKey: 'signalId',
    leftJoinKey: 'issuer',
    rightJoinKey: 'issuer',
    mode: joinMode,
    processingIntervalMs: 100,
    debug: false,
  });

  // Join: Positions with Signals (track signal effectiveness)
  const positionsWithSignals = useJoinDBSP<Position, Signal>({
    leftKey: 'positionId',
    rightKey: 'signalId',
    leftJoinKey: 'issuer',
    rightJoinKey: 'issuer',
    mode: joinMode,
    processingIntervalMs: 100,
    debug: false,
  });

  // Join: Positions with RFQs (match trades with RFQ flow)
  const positionsWithRFQs = useJoinDBSP<Position, RFQ>({
    leftKey: 'positionId',
    rightKey: 'rfqId',
    leftJoinKey: 'bondId',
    rightJoinKey: 'bondId',
    mode: joinMode,
    processingIntervalMs: 100,
    debug: false,
  });

  // ============ COMPUTED VALUES ============

  // Total PnL - compute from unrealized + realized
  const totalPnL = useMemo(() => {
    return positionBySector.results.reduce((sum, s) => 
      sum + (s.unrealizedPnL || 0) + (s.realizedPnL || 0), 0);
  }, [positionBySector.results]);

  // Total DV01
  const totalDV01 = useMemo(() => {
    return positionBySector.results.reduce((sum, s) => sum + Math.abs(s.dv01 || 0), 0);
  }, [positionBySector.results]);

  // Total Notional
  const totalNotional = useMemo(() => {
    return positionBySector.results.reduce((sum, s) => sum + Math.abs(s.notional || 0), 0);
  }, [positionBySector.results]);

  // RFQ Hit Rate - count filled client-side
  const rfqStats = useMemo(() => {
    const total = allRFQs.count;
    const filled = allRFQs.results.filter(r => r.status === 'FILLED').length;
    const cpflows = rfqByCounterparty.results;
    const notional = cpflows.reduce((s, c) => s + (c.totalNotional || 0), 0);
    const spreadCapture = cpflows.reduce((s, c) => s + (c.avgSpreadCapture || 0), 0);
    
    return {
      totalRFQs: total,
      filledRFQs: filled,
      rejectedRFQs: total - filled,
      hitRate: total > 0 ? filled / total : 0,
      totalNotional: notional,
      avgSpreadCapture: filled > 0 ? spreadCapture / filled : 0,
    };
  }, [allRFQs.results, allRFQs.count, rfqByCounterparty.results]);

  // Signal-aligned trades (from join)
  const signalAlignedTrades = useMemo(() => {
    return rfqsWithSignals.results.filter(([rfq, signal]) => {
      const rfqDirection = rfq.side === 'BID' ? 'LONG' : 'SHORT';
      return rfqDirection === signal.direction && signal.confidence > 0.6;
    });
  }, [rfqsWithSignals.results, rfqsWithSignals.count]);

  // Sort tenor results by duration
  const sortedTenors = useMemo(() => {
    const order = ['2Y', '3Y', '5Y', '7Y', '10Y', '15Y', '20Y', '30Y'];
    return [...positionByTenor.results].sort((a, b) => 
      order.indexOf(a.tenor) - order.indexOf(b.tenor)
    );
  }, [positionByTenor.results]);

  // Counterparty stats with filled counts (computed client-side since SQL can't do CASE WHEN)
  const counterpartyStats = useMemo(() => {
    const cpMap = new Map<string, { rfqCount: number; filledCount: number; totalNotional: number; spreadCapture: number }>();
    
    for (const rfq of allRFQs.results) {
      const existing = cpMap.get(rfq.counterparty) || { rfqCount: 0, filledCount: 0, totalNotional: 0, spreadCapture: 0 };
      existing.rfqCount++;
      if (rfq.status === 'FILLED') {
        existing.filledCount++;
        existing.spreadCapture += rfq.spreadCapture || 0;
      }
      existing.totalNotional += rfq.notional || 0;
      cpMap.set(rfq.counterparty, existing);
    }
    
    return Array.from(cpMap.entries()).map(([counterparty, data]) => ({
      counterparty,
      ...data,
      hitRate: data.rfqCount > 0 ? data.filledCount / data.rfqCount : 0,
      avgSpreadCapture: data.filledCount > 0 ? data.spreadCapture / data.filledCount : 0,
    })).sort((a, b) => b.totalNotional - a.totalNotional);
  }, [allRFQs.results, allRFQs.count]);

  // Signal model stats with strong/long/short counts (computed client-side)
  const signalModelStats = useMemo(() => {
    const modelMap = new Map<string, { 
      signalCount: number; 
      strongCount: number; 
      longCount: number; 
      shortCount: number;
      totalConfidence: number;
      totalSignal: number;
    }>();
    
    for (const sig of allSignals.results) {
      const existing = modelMap.get(sig.model) || { 
        signalCount: 0, strongCount: 0, longCount: 0, shortCount: 0, totalConfidence: 0, totalSignal: 0 
      };
      existing.signalCount++;
      if (sig.strength === 'STRONG') existing.strongCount++;
      if (sig.direction === 'LONG') existing.longCount++;
      else existing.shortCount++;
      existing.totalConfidence += sig.confidence || 0;
      existing.totalSignal += sig.signalValue || 0;
      modelMap.set(sig.model, existing);
    }
    
    return Array.from(modelMap.entries()).map(([model, data]) => ({
      model,
      ...data,
      avgConfidence: data.signalCount > 0 ? data.totalConfidence / data.signalCount : 0,
      avgSignal: data.signalCount > 0 ? data.totalSignal / data.signalCount : 0,
    })).sort((a, b) => b.signalCount - a.signalCount);
  }, [allSignals.results, allSignals.count]);

  // ============ COMPLEX AGGREGATIONS ============

  // Risk-Adjusted Metrics: P&L per unit of DV01 (like Sharpe for fixed income)
  const riskAdjustedMetrics = useMemo(() => {
    const pnlPerDV01 = totalDV01 > 0 ? totalPnL / totalDV01 : 0;
    const pnlPerNotional = totalNotional > 0 ? (totalPnL / totalNotional) * 10000 : 0; // in bps
    
    // By sector
    const bySector = positionBySector.results.map(s => {
      const pnl = (s.unrealizedPnL || 0) + (s.realizedPnL || 0);
      const dv01 = Math.abs(s.dv01 || 0);
      return {
        sector: s.sector,
        pnl,
        dv01,
        pnlPerDV01: dv01 > 0 ? pnl / dv01 : 0,
        notional: s.notional || 0,
      };
    }).sort((a, b) => b.pnlPerDV01 - a.pnlPerDV01);

    return { pnlPerDV01, pnlPerNotional, bySector };
  }, [totalPnL, totalDV01, totalNotional, positionBySector.results]);


  // Signal Effectiveness: Track P&L of positions that had matching signals
  const signalEffectiveness = useMemo(() => {
    const byModel = new Map<string, { 
      alignedPnL: number; 
      misalignedPnL: number; 
      alignedCount: number;
      misalignedCount: number;
      totalConfidence: number;
    }>();

    for (const [position, signal] of positionsWithSignals.results) {
      const pnl = (position.unrealizedPnL || 0) + (position.realizedPnL || 0);
      const posDirection = position.notional >= 0 ? 'LONG' : 'SHORT';
      const isAligned = posDirection === signal.direction;
      
      const existing = byModel.get(signal.model) || { 
        alignedPnL: 0, misalignedPnL: 0, alignedCount: 0, misalignedCount: 0, totalConfidence: 0 
      };
      
      if (isAligned) {
        existing.alignedPnL += pnl;
        existing.alignedCount++;
      } else {
        existing.misalignedPnL += pnl;
        existing.misalignedCount++;
      }
      existing.totalConfidence += signal.confidence || 0;
      byModel.set(signal.model, existing);
    }

    const results = Array.from(byModel.entries()).map(([model, data]) => ({
      model,
      ...data,
      totalPnL: data.alignedPnL + data.misalignedPnL,
      alignmentRate: (data.alignedCount + data.misalignedCount) > 0 
        ? data.alignedCount / (data.alignedCount + data.misalignedCount) : 0,
      avgConfidence: (data.alignedCount + data.misalignedCount) > 0
        ? data.totalConfidence / (data.alignedCount + data.misalignedCount) : 0,
      signalValue: data.alignedCount > 0 ? data.alignedPnL / data.alignedCount : 0, // P&L per aligned signal
    }));

    const totalAlignedPnL = results.reduce((s, r) => s + r.alignedPnL, 0);
    const totalMisalignedPnL = results.reduce((s, r) => s + r.misalignedPnL, 0);

    return { byModel: results.sort((a, b) => b.totalPnL - a.totalPnL), totalAlignedPnL, totalMisalignedPnL };
  }, [positionsWithSignals.results, positionsWithSignals.count]);

  // Position ⋈ RFQ: Execution Quality Analysis
  const executionQuality = useMemo(() => {
    const byDesk = new Map<string, {
      trades: number;
      rfqs: number;
      totalSlippage: number;
      totalNotional: number;
      filledRFQs: number;
    }>();

    for (const [position, rfq] of positionsWithRFQs.results) {
      const desk = position.desk;
      const slippage = rfq.fillPrice ? Math.abs(rfq.fillPrice - rfq.price) : 0;
      
      const existing = byDesk.get(desk) || { 
        trades: 0, rfqs: 0, totalSlippage: 0, totalNotional: 0, filledRFQs: 0 
      };
      
      existing.trades++;
      existing.rfqs++;
      existing.totalSlippage += slippage;
      existing.totalNotional += Math.abs(rfq.notional || 0);
      if (rfq.status === 'FILLED') existing.filledRFQs++;
      
      byDesk.set(desk, existing);
    }

    return Array.from(byDesk.entries()).map(([desk, data]) => ({
      desk,
      ...data,
      avgSlippage: data.rfqs > 0 ? data.totalSlippage / data.rfqs : 0,
      executionRate: data.rfqs > 0 ? data.filledRFQs / data.rfqs : 0,
    })).sort((a, b) => b.totalNotional - a.totalNotional);
  }, [positionsWithRFQs.results, positionsWithRFQs.count]);

  // Weighted Aggregations: Notional-weighted spread and DV01-weighted P&L
  const weightedMetrics = useMemo(() => {
    let totalNotionalWeight = 0;
    let weightedSpread = 0;
    let totalDV01Weight = 0;
    let dv01WeightedPnL = 0;

    for (const rfq of allRFQs.results) {
      if (rfq.status === 'FILLED' && rfq.notional) {
        totalNotionalWeight += Math.abs(rfq.notional);
        weightedSpread += (rfq.spread || 0) * Math.abs(rfq.notional);
      }
    }

    for (const sector of positionBySector.results) {
      const dv01 = Math.abs(sector.dv01 || 0);
      const pnl = (sector.unrealizedPnL || 0) + (sector.realizedPnL || 0);
      totalDV01Weight += dv01;
      dv01WeightedPnL += pnl * dv01;
    }

    return {
      notionalWeightedSpread: totalNotionalWeight > 0 ? weightedSpread / totalNotionalWeight : 0,
      dv01WeightedPnL: totalDV01Weight > 0 ? dv01WeightedPnL / totalDV01Weight : 0,
      totalNotionalWeight,
      totalDV01Weight,
    };
  }, [allRFQs.results, allRFQs.count, positionBySector.results]);

  // Concentration Risk: Herfindahl-Hirschman Index by sector
  const concentrationMetrics = useMemo(() => {
    const sectorShares = positionBySector.results.map(s => {
      const share = totalNotional > 0 ? Math.abs(s.notional || 0) / totalNotional : 0;
      return { sector: s.sector, share, squaredShare: share * share };
    });
    
    const hhi = sectorShares.reduce((sum, s) => sum + s.squaredShare, 0) * 10000; // Scale to 0-10000
    const topConcentration = sectorShares.sort((a, b) => b.share - a.share).slice(0, 3);
    
    return { hhi, topConcentration, isConcentrated: hhi > 2500 };
  }, [positionBySector.results, totalNotional]);

  // ============ QUERY PERFORMANCE METRICS ============
  const queryPerformanceMetrics = useMemo(() => {
    const queries = [
      { name: 'All RFQs', query: 'SELECT *', stats: allRFQs.stats, count: allRFQs.count, type: 'filter' },
      { name: 'RFQ by CP', query: 'GROUP BY counterparty', stats: rfqByCounterparty.stats, count: rfqByCounterparty.count, type: 'agg' },
      { name: 'Pos by Sector', query: 'GROUP BY sector', stats: positionBySector.stats, count: positionBySector.count, type: 'agg' },
      { name: 'Pos by Trader', query: 'GROUP BY trader', stats: positionByTrader.stats, count: positionByTrader.count, type: 'agg' },
      { name: 'Pos by Desk', query: 'GROUP BY desk', stats: positionByDesk.stats, count: positionByDesk.count, type: 'agg' },
      { name: 'Pos by Rating', query: 'GROUP BY rating', stats: positionByRating.stats, count: positionByRating.count, type: 'agg' },
      { name: 'Pos by Tenor', query: 'GROUP BY tenor', stats: positionByTenor.stats, count: positionByTenor.count, type: 'agg' },
      { name: 'Signal by Model', query: 'GROUP BY model', stats: signalByModel.stats, count: signalByModel.count, type: 'agg' },
      { name: 'All Signals', query: 'SELECT *', stats: allSignals.stats, count: allSignals.count, type: 'filter' },
    ];

    const joins = [
      { name: 'RFQ ⋈ Signal', left: rfqsWithSignals.leftCount, right: rfqsWithSignals.rightCount, result: rfqsWithSignals.count, stats: rfqsWithSignals.stats },
      { name: 'Pos ⋈ Signal', left: positionsWithSignals.leftCount, right: positionsWithSignals.rightCount, result: positionsWithSignals.count, stats: positionsWithSignals.stats },
      { name: 'Pos ⋈ RFQ', left: positionsWithRFQs.leftCount, right: positionsWithRFQs.rightCount, result: positionsWithRFQs.count, stats: positionsWithRFQs.stats },
    ];

    // Calculate aggregated stats
    const totalAvgMs = queries.reduce((sum, q) => sum + q.stats.avgUpdateMs, 0) / queries.length;
    const maxAvgMs = Math.max(...queries.map(q => q.stats.avgUpdateMs));
    const totalDropped = queries.reduce((sum, q) => sum + q.stats.totalDropped, 0);
    const maxLag = Math.max(...queries.map(q => q.stats.lagMs));
    const avgBufferUtil = queries.reduce((sum, q) => sum + q.stats.bufferUtilization, 0) / queries.length;
    const anyLagging = queries.some(q => q.stats.isLagging);

    // Find bottlenecks (queries taking > 2x average)
    const bottlenecks = queries.filter(q => q.stats.avgUpdateMs > totalAvgMs * 2);
    
    // Sort by slowest
    const sortedBySpeed = [...queries].sort((a, b) => b.stats.avgUpdateMs - a.stats.avgUpdateMs);

    return {
      queries,
      joins,
      totalAvgMs,
      maxAvgMs,
      totalDropped,
      maxLag,
      avgBufferUtil,
      anyLagging,
      bottlenecks,
      sortedBySpeed,
    };
  }, [
    allRFQs.stats, allRFQs.count,
    rfqByCounterparty.stats, rfqByCounterparty.count,
    positionBySector.stats, positionBySector.count,
    positionByTrader.stats, positionByTrader.count,
    positionByDesk.stats, positionByDesk.count,
    positionByRating.stats, positionByRating.count,
    positionByTenor.stats, positionByTenor.count,
    signalByModel.stats, signalByModel.count,
    allSignals.stats, allSignals.count,
    rfqsWithSignals.leftCount, rfqsWithSignals.rightCount, rfqsWithSignals.count, rfqsWithSignals.stats,
    positionsWithSignals.leftCount, positionsWithSignals.rightCount, positionsWithSignals.count, positionsWithSignals.stats,
    positionsWithRFQs.leftCount, positionsWithRFQs.rightCount, positionsWithRFQs.count, positionsWithRFQs.stats,
  ]);

  // ============ FLOW VISUALIZATION PROPS ============
  const flowProps = useMemo(() => ({
    // Source streams
    rfqMetrics: {
      count: allRFQs.count,
      rate: deltaRate,
      avgLatencyMs: allRFQs.stats.avgUpdateMs,
      bufferUtil: allRFQs.stats.bufferUtilization,
      isLagging: allRFQs.stats.isLagging,
    },
    positionMetrics: {
      count: positionBySector.results.reduce((sum, s) => sum + (s.positionCount || 0), 0),
      rate: deltaRate,
      avgLatencyMs: positionBySector.stats.avgUpdateMs,
      bufferUtil: positionBySector.stats.bufferUtilization,
      isLagging: positionBySector.stats.isLagging,
    },
    signalMetrics: {
      count: allSignals.count,
      rate: deltaRate,
      avgLatencyMs: allSignals.stats.avgUpdateMs,
      bufferUtil: allSignals.stats.bufferUtilization,
      isLagging: allSignals.stats.isLagging,
    },
    
    // Join metrics
    rfqSignalJoin: {
      leftCount: rfqsWithSignals.leftCount,
      rightCount: rfqsWithSignals.rightCount,
      resultCount: rfqsWithSignals.count,
      avgLatencyMs: rfqsWithSignals.stats.avgUpdateMs,
      selectivity: rfqsWithSignals.leftCount > 0 && rfqsWithSignals.rightCount > 0 
        ? rfqsWithSignals.count / (rfqsWithSignals.leftCount * rfqsWithSignals.rightCount)
        : 0,
    },
    posSignalJoin: {
      leftCount: positionsWithSignals.leftCount,
      rightCount: positionsWithSignals.rightCount,
      resultCount: positionsWithSignals.count,
      avgLatencyMs: positionsWithSignals.stats.avgUpdateMs,
      selectivity: positionsWithSignals.leftCount > 0 && positionsWithSignals.rightCount > 0 
        ? positionsWithSignals.count / (positionsWithSignals.leftCount * positionsWithSignals.rightCount)
        : 0,
    },
    posRfqJoin: {
      leftCount: positionsWithRFQs.leftCount,
      rightCount: positionsWithRFQs.rightCount,
      resultCount: positionsWithRFQs.count,
      avgLatencyMs: positionsWithRFQs.stats.avgUpdateMs,
      selectivity: positionsWithRFQs.leftCount > 0 && positionsWithRFQs.rightCount > 0 
        ? positionsWithRFQs.count / (positionsWithRFQs.leftCount * positionsWithRFQs.rightCount)
        : 0,
    },
    
    // Aggregation metrics
    sectorAgg: {
      inputCount: positionBySector.results.reduce((sum, s) => sum + (s.positionCount || 0), 0),
      outputCount: positionBySector.count,
      avgLatencyMs: positionBySector.stats.avgUpdateMs,
      groups: positionBySector.count,
    },
    deskAgg: {
      inputCount: positionByDesk.results.reduce((sum, s) => sum + (s.tradeCount || 0), 0),
      outputCount: positionByDesk.count,
      avgLatencyMs: positionByDesk.stats.avgUpdateMs,
      groups: positionByDesk.count,
    },
    traderAgg: {
      inputCount: positionByTrader.results.reduce((sum, s) => sum + (s.tradeCount || 0), 0),
      outputCount: positionByTrader.count,
      avgLatencyMs: positionByTrader.stats.avgUpdateMs,
      groups: positionByTrader.count,
    },
    ratingAgg: {
      inputCount: positionByRating.results.reduce((sum, s) => sum + (s.count || 0), 0),
      outputCount: positionByRating.count,
      avgLatencyMs: positionByRating.stats.avgUpdateMs,
      groups: positionByRating.count,
    },
    tenorAgg: {
      inputCount: positionByTenor.results.reduce((sum, s) => sum + (s.count || 0), 0),
      outputCount: positionByTenor.count,
      avgLatencyMs: positionByTenor.stats.avgUpdateMs,
      groups: positionByTenor.count,
    },
    counterpartyAgg: {
      inputCount: rfqByCounterparty.results.reduce((sum, s) => sum + (s.rfqCount || 0), 0),
      outputCount: rfqByCounterparty.count,
      avgLatencyMs: rfqByCounterparty.stats.avgUpdateMs,
      groups: rfqByCounterparty.count,
    },
    modelAgg: {
      inputCount: signalByModel.results.reduce((sum, s) => sum + (s.signalCount || 0), 0),
      outputCount: signalByModel.count,
      avgLatencyMs: signalByModel.stats.avgUpdateMs,
      groups: signalByModel.count,
    },
    
    // Computed outputs
    totalPnL,
    hitRate: rfqStats.hitRate,
    signalAlignedCount: signalAlignedTrades.length,
    
    // Risk metrics
    totalDV01,
    concentrationHHI: concentrationMetrics.hhi,
  }), [
    allRFQs.count, allRFQs.stats, deltaRate,
    positionBySector.results, positionBySector.count, positionBySector.stats,
    allSignals.count, allSignals.stats,
    rfqsWithSignals.leftCount, rfqsWithSignals.rightCount, rfqsWithSignals.count, rfqsWithSignals.stats,
    positionsWithSignals.leftCount, positionsWithSignals.rightCount, positionsWithSignals.count, positionsWithSignals.stats,
    positionsWithRFQs.leftCount, positionsWithRFQs.rightCount, positionsWithRFQs.count, positionsWithRFQs.stats,
    positionByDesk.results, positionByDesk.count, positionByDesk.stats,
    positionByTrader.results, positionByTrader.count, positionByTrader.stats,
    positionByRating.results, positionByRating.count, positionByRating.stats,
    positionByTenor.results, positionByTenor.count, positionByTenor.stats,
    rfqByCounterparty.results, rfqByCounterparty.count, rfqByCounterparty.stats,
    signalByModel.results, signalByModel.count, signalByModel.stats,
    totalPnL, totalDV01, rfqStats.hitRate, signalAlignedTrades.length, concentrationMetrics.hhi,
  ]);

  // ============ WEBSOCKET CONNECTION ============

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    console.log('[Credit Trading] Connecting to', WS_URL);
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      console.log('[Credit Trading] Connected');
      setStats(s => ({ ...s, connected: true }));
    };

    ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);

      // Handle snapshots
      if (msg.type === 'rfq-snapshot') {
        console.log(`[Credit Trading] RFQ snapshot: ${msg.data.length} rows`);
        const rfqs: RFQ[] = msg.data;
        // Push all at once for efficiency
        rfqByCounterparty.push(rfqs);
        allRFQs.push(rfqs);
        const filledRfqs = rfqs.filter(r => r.status === 'FILLED');
        if (filledRfqs.length > 0) {
          rfqsWithSignals.pushLeft(filledRfqs);
        }
        positionsWithRFQs.pushRight(rfqs);
        setStats(s => ({ ...s, rfqCount: s.rfqCount + rfqs.length }));
      }
      else if (msg.type === 'position-snapshot') {
        console.log(`[Credit Trading] Position snapshot: ${msg.data.length} rows`);
        const positions: Position[] = msg.data;
        // Push all at once for efficiency
        positionBySector.push(positions);
        positionByTrader.push(positions);
        positionByDesk.push(positions);
        positionByRating.push(positions);
        positionByTenor.push(positions);
        // Push to complex joins
        positionsWithSignals.pushLeft(positions);
        positionsWithRFQs.pushLeft(positions);
        setStats(s => ({ ...s, positionCount: s.positionCount + positions.length }));
      }
      else if (msg.type === 'fx-snapshot') {
        const rates: FXRate[] = msg.data;
        setFxRates(prev => {
          const next = new Map(prev);
          rates.forEach(fx => next.set(fx.currency, fx.rate));
          return next;
        });
        setStats(s => ({ ...s, fxUpdateCount: s.fxUpdateCount + rates.length }));
      }
      else if (msg.type === 'benchmark-snapshot') {
        const benchmarkData: Benchmark[] = msg.data;
        setBenchmarks(prev => {
          const next = new Map(prev);
          benchmarkData.forEach(b => next.set(b.benchmarkId, b));
          return next;
        });
        setStats(s => ({ ...s, benchmarkUpdateCount: s.benchmarkUpdateCount + benchmarkData.length }));
      }
      else if (msg.type === 'signal-snapshot') {
        console.log(`[Credit Trading] Signal snapshot: ${msg.data.length} rows`);
        const signals: Signal[] = msg.data;
        // Push all at once for efficiency
        signalByModel.push(signals);
        allSignals.push(signals);
        rfqsWithSignals.pushRight(signals);
        positionsWithSignals.pushRight(signals);
        setStats(s => ({ ...s, signalCount: s.signalCount + signals.length }));
      }
      // Handle deltas
      else if (msg.type === 'delta') {
        const deltas: CreditDeltas = msg.data;

        // Process RFQ deltas
        if (deltas.rfqs.length > 0) {
          rfqByCounterparty.push(deltas.rfqs);
          allRFQs.push(deltas.rfqs);
          const filledRfqs = deltas.rfqs.filter(r => r.status === 'FILLED');
          if (filledRfqs.length > 0) {
            rfqsWithSignals.pushLeft(filledRfqs);
          }
          positionsWithRFQs.pushRight(deltas.rfqs);
          setStats(s => ({ ...s, rfqCount: s.rfqCount + deltas.rfqs.length }));
        }

        // Process position deltas
        if (deltas.positions.length > 0) {
          const positionRows = deltas.positions.map(d => d.row);
          const updates = deltas.positions.filter(d => d.op === 'update').length;
          const inserts = deltas.positions.filter(d => d.op === 'insert').length;
          if (updates > 0) {
            console.log(`[Credit Trading] Position updates: ${updates} (PnL changes)`);
          }
          positionBySector.push(positionRows);
          positionByTrader.push(positionRows);
          positionByDesk.push(positionRows);
          positionByRating.push(positionRows);
          positionByTenor.push(positionRows);
          if (inserts > 0) {
            setStats(s => ({ ...s, positionCount: s.positionCount + inserts }));
          }
        }

        // Process FX deltas
        if (deltas.fx.length > 0) {
          setFxRates(prev => {
            const next = new Map(prev);
            deltas.fx.forEach(fx => next.set(fx.currency, fx.rate));
            return next;
          });
          setStats(s => ({ ...s, fxUpdateCount: s.fxUpdateCount + deltas.fx.length }));
        }

        // Process benchmark deltas
        if (deltas.benchmarks.length > 0) {
          setBenchmarks(prev => {
            const next = new Map(prev);
            deltas.benchmarks.forEach(b => next.set(b.benchmarkId, b));
            return next;
          });
          setStats(s => ({ ...s, benchmarkUpdateCount: s.benchmarkUpdateCount + deltas.benchmarks.length }));
        }

        // Process signal deltas
        if (deltas.signals.length > 0) {
          signalByModel.push(deltas.signals);
          allSignals.push(deltas.signals);
          rfqsWithSignals.pushRight(deltas.signals);
          positionsWithSignals.pushRight(deltas.signals);
          setStats(s => ({ ...s, signalCount: s.signalCount + deltas.signals.length }));
        }

        setStats(s => ({ ...s, lastUpdate: Date.now() }));
      }
    };

    ws.onclose = () => {
      console.log('[Credit Trading] Disconnected');
      setStats(s => ({ ...s, connected: false }));
    };

    ws.onerror = (err) => {
      console.error('[Credit Trading] WebSocket error:', err);
    };
  }, [rfqByCounterparty, allRFQs, positionBySector, positionByTrader, positionByDesk, 
      positionByRating, positionByTenor, signalByModel, allSignals, rfqsWithSignals,
      positionsWithSignals, positionsWithRFQs]);

  const disconnect = useCallback(() => {
    wsRef.current?.close();
    wsRef.current = null;
    // Clear all hooks
    rfqByCounterparty.clear();
    allRFQs.clear();
    positionBySector.clear();
    positionByTrader.clear();
    positionByDesk.clear();
    positionByRating.clear();
    positionByTenor.clear();
    signalByModel.clear();
    allSignals.clear();
    rfqsWithSignals.clear();
    positionsWithSignals.clear();
    positionsWithRFQs.clear();
    // Clear state
    setFxRates(new Map());
    setBenchmarks(new Map());
    setStats({
      connected: false,
      rfqCount: 0,
      positionCount: 0,
      fxUpdateCount: 0,
      benchmarkUpdateCount: 0,
      signalCount: 0,
      deltaRate: 20,
      lastUpdate: 0,
    });
  }, [rfqByCounterparty, allRFQs, positionBySector, positionByTrader, positionByDesk,
      positionByRating, positionByTenor, signalByModel, allSignals, rfqsWithSignals,
      positionsWithSignals, positionsWithRFQs]);

  // Auto-connect on mount
  useEffect(() => {
    connect();
    return () => disconnect();
  }, []);

  // ============ CONTROLS ============

  const sendBurst = useCallback((size: number) => {
    wsRef.current?.send(JSON.stringify({ type: 'burst', size }));
  }, []);

  const togglePause = useCallback(() => {
    wsRef.current?.send(JSON.stringify({ type: isPaused ? 'resume' : 'pause' }));
    setIsPaused(!isPaused);
  }, [isPaused]);

  const updateDeltaRate = useCallback((rate: number) => {
    wsRef.current?.send(JSON.stringify({ type: 'set-rate', rate }));
    setDeltaRate(rate);
  }, []);

  // ============ RENDER ============

  return (
    <div className="credit-trading-page dense-layout">
      {/* Compact Header */}
      <header className="credit-header dense">
        <div className="credit-header-left">
          <h1 className="credit-title">
            <span className="credit-logo">◈</span>
            Credit Trading
          </h1>
          <div className={`credit-status ${stats.connected ? 'connected' : 'disconnected'}`}>
            <span className="status-dot"></span>
            {stats.connected ? 'LIVE' : 'OFFLINE'}
          </div>
        </div>
        
        {/* Top KPIs in header */}
        <div className="credit-header-kpis">
          <div className="header-kpi large">
            <span className={`kpi-value ${totalPnL >= 0 ? 'positive' : 'negative'}`}>
              {formatPnL(totalPnL)}
            </span>
            <span className="kpi-label">P&L</span>
          </div>
          <div className="kpi-divider"></div>
          <div className="header-kpi">
            <span className="kpi-value">{formatCurrency(totalDV01)}</span>
            <span className="kpi-label">DV01</span>
          </div>
          <div className="header-kpi">
            <span className="kpi-value">{formatCurrency(totalNotional)}</span>
            <span className="kpi-label">Notional</span>
          </div>
          <div className="kpi-divider"></div>
          <div className="header-kpi">
            <span className="kpi-value cyan">{formatPercent(rfqStats.hitRate)}</span>
            <span className="kpi-label">Hit Rate</span>
          </div>
          <div className="header-kpi">
            <span className="kpi-value gold">{rfqStats.filledRFQs.toLocaleString()}</span>
            <span className="kpi-label">Filled</span>
          </div>
          <div className="header-kpi">
            <span className="kpi-value">{formatCurrency(rfqStats.avgSpreadCapture)}</span>
            <span className="kpi-label">Spread Capture</span>
          </div>
        </div>

        {/* Compact Controls */}
        <div className="credit-header-controls">
          <button 
            onClick={() => stats.connected ? disconnect() : connect()}
            className={`credit-btn compact ${stats.connected ? 'danger' : 'primary'}`}
          >
            {stats.connected ? '⏹' : '▶'}
          </button>
          <button 
            onClick={togglePause}
            disabled={!stats.connected}
            className="credit-btn compact secondary"
          >
            {isPaused ? '▶' : '⏸'}
          </button>
          <div className="rate-control compact">
            <input
              type="range"
              min="1"
              max="60"
              value={deltaRate}
              onChange={(e) => updateDeltaRate(parseInt(e.target.value))}
              disabled={!stats.connected}
            />
            <span className="rate-value">{deltaRate}/s</span>
          </div>
          <button onClick={() => sendBurst(1000)} disabled={!stats.connected} className="credit-btn compact accent">
            +1K
          </button>
          <div className="join-mode-toggle">
            <button
              onClick={() => {
                if (joinMode !== 'indexed') {
                  setJoinMode('indexed');
                  disconnect();
                  setTimeout(connect, 100);
                }
              }}
              className={`mode-btn ${joinMode === 'indexed' ? 'active' : ''}`}
            >
              IDX
            </button>
            <button
              onClick={() => {
                if (joinMode !== 'append-only') {
                  setJoinMode('append-only');
                  disconnect();
                  setTimeout(connect, 100);
                }
              }}
              className={`mode-btn ${joinMode === 'append-only' ? 'active' : ''}`}
            >
              APP
            </button>
          </div>
          <button 
            onClick={() => setShowFlowView(!showFlowView)}
            className={`credit-btn compact ${showFlowView ? 'accent' : 'secondary'}`}
            title="Toggle Pipeline View"
          >
            {showFlowView ? '🔀' : '📊'}
          </button>
        </div>
      </header>

      {/* Dense Main Content - All visible at once */}
      <main className="credit-main dense">
        {/* Dashboard View (Rows 1-6) - hidden when flow view is active */}
        {!showFlowView && (<>
        {/* Row 1: Core Metrics */}
        <div className="credit-row">
          {/* PnL by Sector - Compact */}
          <div className="credit-card dense">
            <div className="card-header dense">
              <h3>
                Sector P&L
                <SQLPopover sql={`SELECT sector,
       SUM(notional) AS notional,
       SUM(unrealizedPnL) AS unrealizedPnL,
       SUM(realizedPnL) AS realizedPnL,
       SUM(dv01) AS dv01,
       COUNT(*) AS positionCount
FROM positions
GROUP BY sector

-- Incremental: Δ(SUM) = SUM(Δ)`} />
              </h3>
            </div>
            <div className="card-body dense">
              <table className="credit-table dense">
                <thead>
                  <tr>
                    <th>Sector</th>
                    <th>Notional</th>
                    <th>P&L</th>
                    <th>DV01</th>
                  </tr>
                </thead>
                <tbody>
                  {positionBySector.results.map((row, i) => {
                    const pnl = (row.unrealizedPnL || 0) + (row.realizedPnL || 0);
                    return (
                      <tr key={row.sector || i}>
                        <td className="sector-cell">{row.sector}</td>
                        <td>{formatCurrency(row.notional || 0)}</td>
                        <td className={pnl >= 0 ? 'positive' : 'negative'}>{formatPnL(pnl)}</td>
                        <td className="muted">{formatCurrency(row.dv01 || 0)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {/* Desk P&L - Mini bars */}
          <div className="credit-card dense narrow">
            <div className="card-header dense">
              <h3>
                Desk P&L
                <SQLPopover sql={`SELECT desk,
       SUM(notional) AS notional,
       SUM(unrealizedPnL) AS unrealizedPnL,
       SUM(realizedPnL) AS realizedPnL,
       COUNT(*) AS tradeCount
FROM positions
GROUP BY desk`} />
              </h3>
            </div>
            <div className="card-body dense">
              {positionByDesk.results.slice(0, 6).map((row, i) => {
                const pnl = (row.unrealizedPnL || 0) + (row.realizedPnL || 0);
                const maxPnL = Math.max(...positionByDesk.results.map(r => Math.abs((r.unrealizedPnL || 0) + (r.realizedPnL || 0))), 1);
                const width = (Math.abs(pnl) / maxPnL) * 100;
                return (
                  <div key={row.desk || i} className="mini-bar-row">
                    <span className="mini-bar-label">{row.desk}</span>
                    <div className="mini-bar-track">
                      <div className={`mini-bar-fill ${pnl >= 0 ? 'positive' : 'negative'}`} style={{ width: `${width}%` }}></div>
                    </div>
                    <span className={`mini-bar-value ${pnl >= 0 ? 'positive' : 'negative'}`}>{formatPnL(pnl)}</span>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Trader Performance - Compact */}
          <div className="credit-card dense narrow">
            <div className="card-header dense">
              <h3>
                Traders
                <SQLPopover sql={`SELECT trader, desk,
       SUM(notional) AS notional,
       SUM(unrealizedPnL) AS unrealizedPnL,
       SUM(realizedPnL) AS realizedPnL,
       COUNT(*) AS tradeCount
FROM positions
GROUP BY trader, desk`} />
              </h3>
            </div>
            <div className="card-body dense">
              <div className="trader-list">
                {positionByTrader.results.slice(0, 6).map((row, i) => {
                  const pnl = (row.unrealizedPnL || 0) + (row.realizedPnL || 0);
                  return (
                    <div key={`${row.trader}-${row.desk}` || i} className="trader-row">
                      <span className="trader-name">{row.trader}</span>
                      <span className="trader-desk">{row.desk}</span>
                      <span className={`trader-pnl ${pnl >= 0 ? 'positive' : 'negative'}`}>{formatPnL(pnl)}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </div>

        {/* Row 2: RFQ & Counterparty */}
        <div className="credit-row">
          {/* RFQ Stats - Super compact */}
          <div className="credit-card dense stats-strip">
            <div className="stats-strip-content">
              <div className="stat-mini">
                <span className="stat-mini-value">{stats.rfqCount.toLocaleString()}</span>
                <span className="stat-mini-label">RFQs</span>
              </div>
              <div className="stat-mini">
                <span className="stat-mini-value positive">{rfqStats.filledRFQs.toLocaleString()}</span>
                <span className="stat-mini-label">Filled</span>
              </div>
              <div className="stat-mini">
                <span className="stat-mini-value negative">{rfqStats.rejectedRFQs.toLocaleString()}</span>
                <span className="stat-mini-label">Rejected</span>
              </div>
              <div className="stat-mini">
                <span className="stat-mini-value cyan">{formatPercent(rfqStats.hitRate)}</span>
                <span className="stat-mini-label">Hit Rate</span>
              </div>
              <div className="stat-mini">
                <span className="stat-mini-value gold">{formatCurrency(rfqStats.totalNotional)}</span>
                <span className="stat-mini-label">Notional</span>
              </div>
              <div className="stat-mini">
                <SQLPopover sql={`SELECT counterparty,
       COUNT(*) AS rfqCount,
       SUM(notional) AS totalNotional,
       SUM(spreadCapture) AS avgSpreadCapture
FROM rfqs
GROUP BY counterparty

-- RFQ Stream Query`} />
              </div>
            </div>
          </div>

          {/* Counterparty Flow - Compact table */}
          <div className="credit-card dense wide">
            <div className="card-header dense">
              <h3>
                Counterparty Flow
                <SQLPopover sql={`-- Computed incrementally from RFQ stream
-- Hit rate = filled / total per counterparty

SELECT counterparty,
       COUNT(*) AS rfqCount,
       SUM(CASE WHEN status='FILLED' THEN 1 ELSE 0 END) AS filled,
       SUM(notional) AS totalNotional,
       AVG(spreadCapture) AS avgSpread
FROM rfqs
GROUP BY counterparty
ORDER BY totalNotional DESC`} />
              </h3>
            </div>
            <div className="card-body dense">
              <table className="credit-table dense">
                <thead>
                  <tr>
                    <th>Counterparty</th>
                    <th>RFQs</th>
                    <th>Hit%</th>
                    <th>Notional</th>
                    <th>Spread</th>
                  </tr>
                </thead>
                <tbody>
                  {counterpartyStats.slice(0, 8).map((row, i) => (
                    <tr key={row.counterparty || i}>
                      <td className="cp-cell">{row.counterparty}</td>
                      <td>{row.rfqCount}</td>
                      <td className={row.hitRate > 0.5 ? 'positive' : row.hitRate > 0.3 ? '' : 'negative'}>{formatPercent(row.hitRate)}</td>
                      <td>{formatCurrency(row.totalNotional)}</td>
                      <td className="positive">{formatCurrency(row.avgSpreadCapture)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        {/* Row 3: Risk & Signals */}
        <div className="credit-row">
          {/* Rating Exposure - Horizontal */}
          <div className="credit-card dense">
            <div className="card-header dense">
              <h3>
                Rating Exposure
                <SQLPopover sql={`SELECT rating,
       SUM(notional) AS notional,
       SUM(dv01) AS dv01,
       COUNT(*) AS count
FROM positions
GROUP BY rating

-- Credit quality distribution`} />
              </h3>
            </div>
            <div className="card-body dense">
              <div className="rating-strip">
                {positionByRating.results.map((row, i) => (
                  <div key={row.rating || i} className="rating-chip" style={{ borderColor: getRatingColor(row.rating) }}>
                    <span className="rating-grade" style={{ color: getRatingColor(row.rating) }}>{row.rating}</span>
                    <span className="rating-amount">{formatCurrency(row.notional || 0)}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Tenor DV01 - Mini bars */}
          <div className="credit-card dense narrow">
            <div className="card-header dense">
              <h3>
                Tenor DV01
                <SQLPopover sql={`SELECT tenor,
       SUM(notional) AS notional,
       SUM(dv01) AS dv01,
       COUNT(*) AS count
FROM positions
GROUP BY tenor

-- Duration risk by maturity bucket`} />
              </h3>
            </div>
            <div className="card-body dense">
              <div className="tenor-mini-chart">
                {sortedTenors.map((row, i) => {
                  const maxDv01 = Math.max(...sortedTenors.map(r => Math.abs(r.dv01 || 0)), 1);
                  const height = (Math.abs(row.dv01 || 0) / maxDv01) * 100;
                  return (
                    <div key={row.tenor || i} className="tenor-mini-bar">
                      <div className="tenor-mini-fill" style={{ height: `${height}%` }}></div>
                      <span className="tenor-mini-label">{row.tenor}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>

          {/* Signal Models - Compact */}
          <div className="credit-card dense">
            <div className="card-header dense">
              <h3>
                Signal Models
                <SQLPopover sql={`SELECT model,
       COUNT(*) AS signalCount,
       AVG(signalValue) AS avgSignal,
       AVG(confidence) AS avgConfidence
FROM signals
GROUP BY model

-- Quantitative model performance`} />
              </h3>
            </div>
            <div className="card-body dense">
              <div className="signal-grid">
                {signalModelStats.map((row, i) => (
                  <div key={row.model || i} className="signal-card">
                    <span className="signal-model-name">{row.model}</span>
                    <div className="signal-stats">
                      <span className="signal-count">{row.signalCount}</span>
                      <span className="signal-conf">{formatPercent(row.avgConfidence)}</span>
                    </div>
                    <div className="signal-direction">
                      <span className="positive">↑{row.longCount}</span>
                      <span className="negative">↓{row.shortCount}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Row 4: Joins & Market Data */}
        <div className="credit-row">
          {/* Signal-Aligned Trades (JOIN) */}
          <div className="credit-card dense">
            <div className="card-header dense">
              <h3>
                <span className="join-icon-inline">⋈</span> Signal-Aligned Trades
                <SQLPopover sql={`-- Bilinear Join: Δ(RFQ ⋈ Signal) = ΔRFQ ⋈ ΔSig + RFQ ⋈ ΔSig + ΔRFQ ⋈ Sig

SELECT r.*, s.direction, s.confidence, s.model
FROM rfqs r
INNER JOIN signals s ON r.issuer = s.issuer
WHERE s.confidence > 0.6
  AND r.side = CASE s.direction 
    WHEN 'LONG' THEN 'BID' 
    ELSE 'ASK' 
  END

-- Mode: ${joinMode} (${joinMode === 'append-only' ? '3000x' : '100x'} faster)`} />
              </h3>
            </div>
            <div className="card-body dense">
              <div className="join-inline-stats">
                <span className="join-inline-stat">{rfqsWithSignals.leftCount.toLocaleString()} RFQs</span>
                <span className="join-inline-op">⋈</span>
                <span className="join-inline-stat">{rfqsWithSignals.rightCount.toLocaleString()} Signals</span>
                <span className="join-inline-op">=</span>
                <span className="join-inline-result">{rfqsWithSignals.count.toLocaleString()}</span>
              </div>
              <div className="aligned-trades-compact">
                <div className="aligned-big-number">
                  <span className="aligned-number">{signalAlignedTrades.length}</span>
                  <span className="aligned-desc">aligned trades</span>
                </div>
                {signalAlignedTrades.slice(0, 4).map(([rfq, signal]) => (
                  <div key={`${rfq.rfqId}-${signal.signalId}`} className="aligned-trade-mini">
                    <span className="trade-issuer">{rfq.issuer}</span>
                    <span className={`trade-dir ${signal.direction.toLowerCase()}`}>{signal.direction}</span>
                    <span className="trade-model">{signal.model}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Market Data - Benchmarks & FX */}
          <div className="credit-card dense">
            <div className="card-header dense">
              <h3>Market Data</h3>
            </div>
            <div className="card-body dense market-data-grid">
              <div className="market-section">
                <span className="market-section-title">Benchmarks</span>
                {Array.from(benchmarks.values()).slice(0, 4).map((b) => (
                  <div key={b.benchmarkId} className="market-item">
                    <span className="market-name">{b.benchmarkId}</span>
                    <span className="market-value">{b.isSpread ? formatBps(b.level) : `${b.level.toFixed(2)}%`}</span>
                    <span className={`market-change ${b.change >= 0 ? 'positive' : 'negative'}`}>
                      {b.change >= 0 ? '↑' : '↓'}{Math.abs(b.change).toFixed(1)}
                    </span>
                  </div>
                ))}
              </div>
              <div className="market-section">
                <span className="market-section-title">FX</span>
                {Array.from(fxRates.entries()).slice(0, 4).map(([ccy, rate]) => (
                  <div key={ccy} className="market-item">
                    <span className="market-name">{ccy}</span>
                    <span className="market-value">{rate.toFixed(4)}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Performance Stats */}
          <div className="credit-card dense narrow">
            <div className="card-header dense">
              <h3>Stream Stats</h3>
            </div>
            <div className="card-body dense">
              <div className="perf-stats-vertical">
                <div className="perf-stat-row">
                  <span className="perf-stat-label">RFQs</span>
                  <span className="perf-stat-value cyan">{stats.rfqCount.toLocaleString()}</span>
                </div>
                <div className="perf-stat-row">
                  <span className="perf-stat-label">Positions</span>
                  <span className="perf-stat-value gold">{stats.positionCount.toLocaleString()}</span>
                </div>
                <div className="perf-stat-row">
                  <span className="perf-stat-label">Signals</span>
                  <span className="perf-stat-value purple">{stats.signalCount.toLocaleString()}</span>
                </div>
                <div className="perf-stat-row">
                  <span className="perf-stat-label">FX Updates</span>
                  <span className="perf-stat-value">{stats.fxUpdateCount}</span>
                </div>
                <div className="perf-stat-row">
                  <span className="perf-stat-label">Join Mode</span>
                  <span className="perf-stat-value accent">{joinMode}</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Row 5: Complex Aggregations & Advanced Joins */}
        <div className="credit-row">
          {/* Risk-Adjusted Metrics */}
          <div className="credit-card dense">
            <div className="card-header dense">
              <h3>
                📐 Risk-Adjusted Metrics
                <SQLPopover sql={`-- Risk-Adjusted P&L (like Sharpe for fixed income)
-- P&L per unit of DV01 risk taken

SELECT 
  SUM(unrealizedPnL + realizedPnL) / 
    NULLIF(SUM(ABS(dv01)), 0) AS pnl_per_dv01,
  SUM(unrealizedPnL + realizedPnL) / 
    NULLIF(SUM(ABS(notional)), 0) * 10000 AS pnl_bps
FROM positions

-- Formula: Risk-Adjusted Return = P&L / DV01
-- Higher = better risk-adjusted performance`} />
              </h3>
            </div>
            <div className="card-body dense">
              <div className="risk-metrics-grid">
                <div className="risk-metric-card primary">
                  <span className="risk-metric-label">P&L / DV01</span>
                  <span className={`risk-metric-value ${riskAdjustedMetrics.pnlPerDV01 >= 0 ? 'positive' : 'negative'}`}>
                    {riskAdjustedMetrics.pnlPerDV01.toFixed(2)}
                  </span>
                </div>
                <div className="risk-metric-card">
                  <span className="risk-metric-label">P&L (bps)</span>
                  <span className={`risk-metric-value ${riskAdjustedMetrics.pnlPerNotional >= 0 ? 'positive' : 'negative'}`}>
                    {riskAdjustedMetrics.pnlPerNotional.toFixed(1)}bp
                  </span>
                </div>
              </div>
              <div className="risk-by-sector">
                {riskAdjustedMetrics.bySector.slice(0, 4).map((s, i) => (
                  <div key={s.sector || i} className="sector-risk-row">
                    <span className="sector-name">{s.sector}</span>
                    <span className={`sector-risk ${s.pnlPerDV01 >= 0 ? 'positive' : 'negative'}`}>
                      {s.pnlPerDV01.toFixed(2)}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Signal Effectiveness (Position ⋈ Signal) */}
          <div className="credit-card dense">
            <div className="card-header dense">
              <h3>
                <span className="join-icon-inline">⋈</span> Signal Effectiveness
                <SQLPopover sql={`-- Position ⋈ Signal JOIN
-- Track P&L of positions that had matching model signals

SELECT 
  s.model,
  SUM(CASE WHEN pos_direction = s.direction 
      THEN p.pnl ELSE 0 END) AS aligned_pnl,
  SUM(CASE WHEN pos_direction != s.direction 
      THEN p.pnl ELSE 0 END) AS misaligned_pnl,
  COUNT(*) AS total_signals
FROM positions p
INNER JOIN signals s ON p.issuer = s.issuer
GROUP BY s.model

-- Δ(Position ⋈ Signal) computed incrementally
-- Measures: Does following the signal make money?`} />
              </h3>
            </div>
            <div className="card-body dense">
              <div className="signal-effectiveness-summary">
                <div className="eff-stat">
                  <span className="eff-label">Aligned P&L</span>
                  <span className={`eff-value ${signalEffectiveness.totalAlignedPnL >= 0 ? 'positive' : 'negative'}`}>
                    {formatPnL(signalEffectiveness.totalAlignedPnL)}
                  </span>
                </div>
                <div className="eff-stat">
                  <span className="eff-label">Misaligned P&L</span>
                  <span className={`eff-value ${signalEffectiveness.totalMisalignedPnL >= 0 ? 'positive' : 'negative'}`}>
                    {formatPnL(signalEffectiveness.totalMisalignedPnL)}
                  </span>
                </div>
              </div>
              <div className="signal-eff-by-model">
                {signalEffectiveness.byModel.slice(0, 4).map((m, i) => (
                  <div key={m.model || i} className="eff-model-row">
                    <span className="eff-model-name">{m.model}</span>
                    <span className={`eff-model-pnl ${m.totalPnL >= 0 ? 'positive' : 'negative'}`}>
                      {formatPnL(m.totalPnL)}
                    </span>
                    <span className="eff-model-rate">{formatPercent(m.alignmentRate)}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Weighted Aggregations */}
          <div className="credit-card dense narrow">
            <div className="card-header dense">
              <h3>
                ⚖️ Weighted Metrics
                <SQLPopover sql={`-- Notional-Weighted Aggregations
-- More accurate than simple averages

-- Notional-weighted spread:
SELECT SUM(spread * notional) / 
       NULLIF(SUM(notional), 0) AS weighted_spread
FROM rfqs WHERE status = 'FILLED'

-- DV01-weighted P&L:
SELECT SUM(pnl * ABS(dv01)) / 
       NULLIF(SUM(ABS(dv01)), 0) AS dv01_weighted_pnl
FROM positions

-- Formula: Σ(value × weight) / Σ(weight)`} />
              </h3>
            </div>
            <div className="card-body dense">
              <div className="weighted-metrics-list">
                <div className="weighted-metric">
                  <span className="weighted-label">Wtd Spread</span>
                  <span className="weighted-value">{formatBps(weightedMetrics.notionalWeightedSpread)}</span>
                </div>
                <div className="weighted-metric">
                  <span className="weighted-label">Wtd P&L</span>
                  <span className={`weighted-value ${weightedMetrics.dv01WeightedPnL >= 0 ? 'positive' : 'negative'}`}>
                    {formatCurrency(weightedMetrics.dv01WeightedPnL)}
                  </span>
                </div>
                <div className="weighted-metric">
                  <span className="weighted-label">Notional Wt</span>
                  <span className="weighted-value muted">{formatCurrency(weightedMetrics.totalNotionalWeight)}</span>
                </div>
                <div className="weighted-metric">
                  <span className="weighted-label">DV01 Wt</span>
                  <span className="weighted-value muted">{formatCurrency(weightedMetrics.totalDV01Weight)}</span>
                </div>
              </div>
            </div>
          </div>

          {/* Concentration Risk (HHI) */}
          <div className="credit-card dense narrow">
            <div className="card-header dense">
              <h3>
                📊 Concentration
                <SQLPopover sql={`-- Herfindahl-Hirschman Index (HHI)
-- Measures portfolio concentration

SELECT 
  SUM(POWER(sector_share, 2)) * 10000 AS hhi
FROM (
  SELECT sector, 
    ABS(notional) / SUM(ABS(notional)) OVER() AS sector_share
  FROM positions
  GROUP BY sector
)

-- HHI < 1500: Unconcentrated
-- HHI 1500-2500: Moderate
-- HHI > 2500: Concentrated`} />
              </h3>
            </div>
            <div className="card-body dense">
              <div className="hhi-display">
                <span className={`hhi-value ${concentrationMetrics.isConcentrated ? 'warning' : 'good'}`}>
                  {concentrationMetrics.hhi.toFixed(0)}
                </span>
                <span className="hhi-label">HHI</span>
                <span className={`hhi-status ${concentrationMetrics.isConcentrated ? 'warning' : 'good'}`}>
                  {concentrationMetrics.isConcentrated ? 'CONCENTRATED' : 'DIVERSIFIED'}
                </span>
              </div>
              <div className="top-concentrations">
                {concentrationMetrics.topConcentration.slice(0, 3).map((c, i) => (
                  <div key={c.sector || i} className="concentration-row">
                    <span className="conc-sector">{c.sector}</span>
                    <span className="conc-share">{formatPercent(c.share)}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Row 6: Execution Quality & More Joins */}
        <div className="credit-row">
          {/* Execution Quality (Position ⋈ RFQ) */}
          <div className="credit-card dense wide">
            <div className="card-header dense">
              <h3>
                <span className="join-icon-inline">⋈</span> Execution Quality
                <SQLPopover sql={`-- Position ⋈ RFQ JOIN
-- Match trades with their originating RFQ flow

SELECT 
  p.desk,
  COUNT(*) AS trades,
  AVG(ABS(r.fillPrice - r.price)) AS avg_slippage,
  SUM(r.notional) AS total_notional,
  SUM(CASE WHEN r.status='FILLED' THEN 1 ELSE 0 END) / 
    COUNT(*) AS execution_rate
FROM positions p
INNER JOIN rfqs r ON p.bondId = r.bondId
GROUP BY p.desk

-- Δ(Position ⋈ RFQ) = ΔPos ⋈ RFQ + Pos ⋈ ΔRFQ + ΔPos ⋈ ΔRFQ`} />
              </h3>
            </div>
            <div className="card-body dense">
              <div className="exec-join-stats">
                <span>{positionsWithRFQs.leftCount.toLocaleString()} Positions</span>
                <span className="join-op">⋈</span>
                <span>{positionsWithRFQs.rightCount.toLocaleString()} RFQs</span>
                <span className="join-op">=</span>
                <span className="join-result">{positionsWithRFQs.count.toLocaleString()} matched</span>
              </div>
              <table className="credit-table dense">
                <thead>
                  <tr>
                    <th>Desk</th>
                    <th>Trades</th>
                    <th>Exec Rate</th>
                    <th>Slippage</th>
                    <th>Notional</th>
                  </tr>
                </thead>
                <tbody>
                  {executionQuality.slice(0, 5).map((row, i) => (
                    <tr key={row.desk || i}>
                      <td className="desk-cell">{row.desk}</td>
                      <td>{row.trades}</td>
                      <td className={row.executionRate > 0.5 ? 'positive' : 'negative'}>
                        {formatPercent(row.executionRate)}
                      </td>
                      <td className="muted">{row.avgSlippage.toFixed(4)}</td>
                      <td>{formatCurrency(row.totalNotional)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Position × Signal Join Summary */}
          <div className="credit-card dense">
            <div className="card-header dense">
              <h3>
                <span className="join-icon-inline">⋈</span> Position × Signal
                <SQLPopover sql={`-- Full JOIN analysis
-- Every position matched with relevant signals

SELECT 
  COUNT(DISTINCT p.positionId) AS positions_with_signals,
  COUNT(DISTINCT s.signalId) AS signals_with_positions,
  COUNT(*) AS total_join_rows,
  AVG(s.confidence) AS avg_signal_confidence
FROM positions p
INNER JOIN signals s ON p.issuer = s.issuer

-- Join cardinality: |Position ⋈ Signal|`} />
              </h3>
            </div>
            <div className="card-body dense">
              <div className="join-summary-grid">
                <div className="join-summary-stat">
                  <span className="join-summary-value">{positionsWithSignals.leftCount.toLocaleString()}</span>
                  <span className="join-summary-label">Positions</span>
                </div>
                <div className="join-summary-op">⋈</div>
                <div className="join-summary-stat">
                  <span className="join-summary-value">{positionsWithSignals.rightCount.toLocaleString()}</span>
                  <span className="join-summary-label">Signals</span>
                </div>
                <div className="join-summary-op">=</div>
                <div className="join-summary-stat highlight">
                  <span className="join-summary-value">{positionsWithSignals.count.toLocaleString()}</span>
                  <span className="join-summary-label">Joined Rows</span>
                </div>
              </div>
              <div className="join-formula-display">
                <code>Δ(L ⋈ R) = ΔL ⋈ R ∪ L ⋈ ΔR ∪ ΔL ⋈ ΔR</code>
              </div>
            </div>
          </div>
        </div>
        </>)}

        {/* Row 7: Real-time Stream Analytics Pipeline */}
        {showFlowView && (
          <div className="flow-section">
            <div className="flow-header">
              <h3>
                🔀 Real-time Stream Analytics Pipeline
                <SQLPopover sql={`-- DBSP Incremental Computation Pipeline
-- This visualization shows data flow through:

-- 1. SOURCE STREAMS (left)
--    RFQ Stream: Incoming quote requests
--    Position Stream: Current holdings
--    Signal Stream: ML model predictions

-- 2. JOIN OPERATIONS (center)
--    Bilinear joins with O(|ΔL| + |ΔR|) complexity
--    • RFQ ⋈ Signal: Match RFQs with model signals
--    • Position ⋈ Signal: Track signal effectiveness
--    • Position ⋈ RFQ: Execution quality analysis

-- 3. AGGREGATIONS (right)
--    GROUP BY sector/desk/trader/rating/tenor
--    Incremental: Δ(SUM) = SUM(Δ)

-- 4. OUTPUT SINKS (far right)
--    Real-time metrics: P&L, Hit Rate, etc.

-- Key: Only deltas (Δ) flow through the system
-- Full recomputation NOT needed!`} />
              </h3>
              <div className="flow-stats">
                <div className="flow-stat">
                  <span className="flow-stat-value">{(allRFQs.count + positionBySector.results.reduce((s, r) => s + (r.positionCount || 0), 0) + allSignals.count).toLocaleString()}</span>
                  <span className="flow-stat-label">Total Events</span>
                </div>
                <div className="flow-stat">
                  <span className="flow-stat-value">{(rfqsWithSignals.count + positionsWithSignals.count + positionsWithRFQs.count).toLocaleString()}</span>
                  <span className="flow-stat-label">Join Results</span>
                </div>
                <div className="flow-stat">
                  <span className="flow-stat-value" style={{ color: getPerfColor(queryPerformanceMetrics.totalAvgMs) }}>
                    {formatMs(queryPerformanceMetrics.totalAvgMs)}
                  </span>
                  <span className="flow-stat-label">Avg Latency</span>
                </div>
              </div>
            </div>
            <StreamFlowVisualization {...flowProps} />
          </div>
        )}

      </main>

      {/* Performance Footer */}
      <footer className="credit-footer perf-footer">
        {/* Performance Header Bar */}
        <div className="perf-footer-header">
          <h3>
            ⚡ Query Performance
            <SQLPopover sql={`-- DBSP Query Performance Monitoring
-- Each query is processed incrementally

-- Key Metrics:
-- • Avg Update Time: Time to process a delta batch
-- • Lag: How far behind real-time
-- • Buffer Utilization: Memory pressure
-- • Dropped: Messages lost due to backpressure

-- Performance targets:
-- • Excellent: < 0.5ms per update
-- • Good: < 2ms per update  
-- • Warning: < 5ms per update
-- • Bottleneck: > 5ms per update`} />
          </h3>
          <div className="perf-header-stats">
            <div className={`perf-header-stat ${queryPerformanceMetrics.anyLagging ? 'warning' : 'good'}`}>
              <span className="stat-value">{formatMs(queryPerformanceMetrics.totalAvgMs)}</span>
              <span className="stat-label">Avg</span>
            </div>
            <div className="perf-header-stat">
              <span className="stat-value" style={{ color: getPerfColor(queryPerformanceMetrics.maxAvgMs) }}>
                {formatMs(queryPerformanceMetrics.maxAvgMs)}
              </span>
              <span className="stat-label">Max</span>
            </div>
            <div className={`perf-header-stat ${queryPerformanceMetrics.maxLag > 100 ? 'warning' : ''}`}>
              <span className="stat-value">{queryPerformanceMetrics.maxLag.toFixed(0)}ms</span>
              <span className="stat-label">Lag</span>
            </div>
            <div className={`perf-header-stat ${queryPerformanceMetrics.totalDropped > 0 ? 'error' : ''}`}>
              <span className="stat-value">{queryPerformanceMetrics.totalDropped}</span>
              <span className="stat-label">Drop</span>
            </div>
            <div className={`perf-header-stat ${queryPerformanceMetrics.avgBufferUtil > 0.8 ? 'warning' : ''}`}>
              <span className="stat-value">{(queryPerformanceMetrics.avgBufferUtil * 100).toFixed(0)}%</span>
              <span className="stat-label">Buf</span>
            </div>
          </div>
          {queryPerformanceMetrics.bottlenecks.length > 0 && (
            <div className="perf-bottleneck-badge">
              ⚠️ {queryPerformanceMetrics.bottlenecks.map(b => b.name).join(', ')}
            </div>
          )}
        </div>

        {/* Performance Details Grid */}
        <div className="perf-footer-grid">
          {/* Query Latencies */}
          <div className="perf-footer-section queries-section">
            <h4>📊 Query Latencies</h4>
            <div className="perf-query-list">
              {queryPerformanceMetrics.sortedBySpeed.map((q) => (
                <div key={q.name} className={`perf-query-row ${q.stats.avgUpdateMs > 5 ? 'bottleneck' : ''}`}>
                  <span className="pq-name">{q.name}</span>
                  <span className={`pq-type ${q.type}`}>{q.type}</span>
                  <span className="pq-rows">{q.count.toLocaleString()}</span>
                  <span className="pq-latency" style={{ color: getPerfColor(q.stats.avgUpdateMs) }}>
                    {formatMs(q.stats.avgUpdateMs)}
                  </span>
                  <span className="pq-status" style={{ background: getPerfColor(q.stats.avgUpdateMs) }}>
                    {getPerfLabel(q.stats.avgUpdateMs)}
                  </span>
                </div>
              ))}
            </div>
          </div>

          {/* Join Performance */}
          <div className="perf-footer-section joins-section">
            <h4>
              ⋈ Join Performance
              <SQLPopover sql={`-- Incremental Join Complexity
-- Bilinear: O(|ΔL| + |ΔR|) per update

-- Join Cardinality affects performance:
-- • Low selectivity = faster
-- • High cardinality = more work

-- Mode comparison:
-- • Indexed: O(log n) lookups, handles updates
-- • Append-Only: O(1) amortized, insert-only`} />
            </h4>
            <div className="perf-join-list">
              {queryPerformanceMetrics.joins.map((j) => (
                <div key={j.name} className="perf-join-row">
                  <div className="pj-header">
                    <span className="pj-name">{j.name}</span>
                    <span className="pj-latency" style={{ color: getPerfColor(j.stats.avgUpdateMs) }}>
                      {formatMs(j.stats.avgUpdateMs)}
                    </span>
                  </div>
                  <div className="pj-details">
                    <span className="pj-cardinality">{j.left.toLocaleString()} × {j.right.toLocaleString()} → {j.result.toLocaleString()}</span>
                    <span className="pj-selectivity">
                      {j.left > 0 && j.right > 0 ? `${((j.result / (j.left * j.right)) * 100).toFixed(4)}%` : '-'}
                    </span>
                  </div>
                  <div className="pj-bar">
                    <div className="pj-bar-fill" style={{ width: `${Math.min(100, j.stats.avgUpdateMs * 20)}%`, background: getPerfColor(j.stats.avgUpdateMs) }}></div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Buffer Status */}
          <div className="perf-footer-section buffers-section">
            <h4>🔄 Buffer Status</h4>
            <div className="perf-buffer-list">
              {queryPerformanceMetrics.queries.map((q) => (
                <div key={q.name} className="perf-buffer-row">
                  <span className="pb-name">{q.name}</span>
                  <div className="pb-bar">
                    <div 
                      className={`pb-bar-fill ${q.stats.bufferUtilization > 0.8 ? 'high' : q.stats.bufferUtilization > 0.5 ? 'medium' : 'low'}`}
                      style={{ width: `${q.stats.bufferUtilization * 100}%` }}
                    ></div>
                  </div>
                  <span className="pb-percent">{(q.stats.bufferUtilization * 100).toFixed(0)}%</span>
                  <span className={`pb-status ${q.stats.isLagging ? 'lagging' : ''}`}>
                    {q.stats.isLagging ? '⚠️' : '✓'}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Footer bottom bar */}
        <div className="perf-footer-bottom">
          <div className="footer-formulas">
            <code>Δ(RFQ ⋈ Signal) = ΔRFQ ⋈ ΔSig + RFQ ⋈ ΔSig + ΔRFQ ⋈ Sig</code>
            <code>Δ(SUM(x)) = SUM(Δx) — Linear Time</code>
          </div>
          <a href="/" className="nav-link" onClick={(e) => { e.preventDefault(); }}>← Orders</a>
        </div>
      </footer>
    </div>
  );
}

export default CreditTradingPage;
