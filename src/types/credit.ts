/**
 * Credit Trading Types
 *
 * Type definitions for systematic credit trading simulation
 */

// ============ RFQ & TRADE ============

export interface RFQ {
  rfqId: number;
  timestamp: number;
  bondId: string;
  issuer: string;
  issuerName: string;
  sector: string;
  rating: string;
  country: string;
  tenor: string;
  coupon: number;
  currency: string;
  side: 'BID' | 'OFFER';
  notional: number;
  counterparty: string;
  spread: number;
  price: number;
  status: 'PENDING' | 'FILLED' | 'REJECTED';
  trader: string;
  desk: string;
  tradeId?: number;
  fillPrice?: number;
  fillTime?: number;
  spreadCapture?: number;
  dv01?: number;
}

// ============ POSITION ============

export interface Position {
  positionId: number;
  bondId: string;
  issuer: string;
  issuerName: string;
  sector: string;
  rating: string;
  tenor: string;
  currency: string;
  notional: number;
  avgPrice: number;
  currentPrice: number;
  unrealizedPnL: number;
  realizedPnL: number;
  dv01: number;
  trader: string;
  desk: string;
  timestamp: number;
}

// ============ FX ============

export interface FXRate {
  timestamp: number;
  currency: string;
  rate: number;
  change24h: number;
}

// ============ BENCHMARK ============

export interface Benchmark {
  timestamp: number;
  benchmarkId: string;
  name: string;
  region: string;
  level: number;
  change: number;
  isSpread: boolean;
}

// ============ SIGNAL ============

export interface Signal {
  signalId: number;
  timestamp: number;
  model: string;
  issuer: string;
  issuerName: string;
  sector: string;
  signalValue: number; // Renamed from 'signal' to avoid SQL reserved word conflict
  direction: 'LONG' | 'SHORT';
  confidence: number;
  strength: 'WEAK' | 'MODERATE' | 'STRONG';
}

// ============ DELTA OPERATIONS ============

export interface PositionDelta {
  op: 'insert' | 'update' | 'delete';
  row: Position;
}

export interface CreditDeltas {
  rfqs: RFQ[];
  positions: PositionDelta[];
  fx: FXRate[];
  benchmarks: Benchmark[];
  signals: Signal[];
}

// ============ AGGREGATIONS ============

export interface SectorPnL {
  sector: string;
  notional: number;
  unrealizedPnL: number;
  realizedPnL: number;
  totalPnL: number;
  dv01: number;
  positionCount: number;
}

export interface TraderPnL {
  trader: string;
  desk: string;
  notional: number;
  unrealizedPnL: number;
  realizedPnL: number;
  totalPnL: number;
  tradeCount: number;
  hitRate: number;
}

export interface DeskPnL {
  desk: string;
  notional: number;
  unrealizedPnL: number;
  realizedPnL: number;
  totalPnL: number;
  tradeCount: number;
  spreadCapture: number;
}

export interface RFQStats {
  totalRFQs: number;
  filledRFQs: number;
  rejectedRFQs: number;
  hitRate: number;
  totalNotional: number;
  avgSpreadCapture: number;
  bySide: {
    BID: { count: number; notional: number };
    OFFER: { count: number; notional: number };
  };
}

export interface RatingExposure {
  rating: string;
  notional: number;
  dv01: number;
  pnl: number;
  count: number;
}

export interface TenorExposure {
  tenor: string;
  notional: number;
  dv01: number;
  pnl: number;
  count: number;
}

export interface CounterpartyFlow {
  counterparty: string;
  rfqCount: number;
  filledCount: number;
  hitRate: number;
  totalNotional: number;
  avgSpreadCapture: number;
}

export interface SignalPerformance {
  model: string;
  signalCount: number;
  avgSignal: number;
  strongCount: number;
  longCount: number;
  shortCount: number;
  avgConfidence: number;
}

// ============ CONNECTION ============

export interface CreditConnectionStats {
  connected: boolean;
  rfqCount: number;
  positionCount: number;
  fxUpdateCount: number;
  benchmarkUpdateCount: number;
  signalCount: number;
  deltaRate: number;
  lastUpdate: number;
}

