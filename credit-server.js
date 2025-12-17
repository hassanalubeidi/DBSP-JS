/**
 * Credit Trading Real-Time Server
 * 
 * Simulates a systematic credit trading environment with:
 * - RFQs (Request for Quotes) - Buy/sell requests for corporate bonds
 * - PnL (Profit & Loss) - Real-time position tracking
 * - FX Rates - Currency conversion for multi-currency positions
 * - Benchmarks - Credit indices, treasury yields
 * - Signals - Quantitative model outputs
 */

import { WebSocketServer } from 'ws';

const PORT = 8766;

// ============ REFERENCE DATA ============

const ISSUERS = [
  { ticker: 'AAPL', name: 'Apple Inc', sector: 'Technology', rating: 'AA+', country: 'US' },
  { ticker: 'MSFT', name: 'Microsoft Corp', sector: 'Technology', rating: 'AAA', country: 'US' },
  { ticker: 'GOOGL', name: 'Alphabet Inc', sector: 'Technology', rating: 'AA+', country: 'US' },
  { ticker: 'AMZN', name: 'Amazon.com Inc', sector: 'Consumer', rating: 'AA', country: 'US' },
  { ticker: 'JPM', name: 'JPMorgan Chase', sector: 'Financials', rating: 'A+', country: 'US' },
  { ticker: 'GS', name: 'Goldman Sachs', sector: 'Financials', rating: 'A+', country: 'US' },
  { ticker: 'MS', name: 'Morgan Stanley', sector: 'Financials', rating: 'A', country: 'US' },
  { ticker: 'BAC', name: 'Bank of America', sector: 'Financials', rating: 'A', country: 'US' },
  { ticker: 'C', name: 'Citigroup', sector: 'Financials', rating: 'A-', country: 'US' },
  { ticker: 'WFC', name: 'Wells Fargo', sector: 'Financials', rating: 'A-', country: 'US' },
  { ticker: 'XOM', name: 'Exxon Mobil', sector: 'Energy', rating: 'AA-', country: 'US' },
  { ticker: 'CVX', name: 'Chevron Corp', sector: 'Energy', rating: 'AA', country: 'US' },
  { ticker: 'BP', name: 'BP plc', sector: 'Energy', rating: 'A', country: 'UK' },
  { ticker: 'SHEL', name: 'Shell plc', sector: 'Energy', rating: 'A+', country: 'UK' },
  { ticker: 'T', name: 'AT&T Inc', sector: 'Telecom', rating: 'BBB', country: 'US' },
  { ticker: 'VZ', name: 'Verizon', sector: 'Telecom', rating: 'BBB+', country: 'US' },
  { ticker: 'PFE', name: 'Pfizer Inc', sector: 'Healthcare', rating: 'A+', country: 'US' },
  { ticker: 'JNJ', name: 'Johnson & Johnson', sector: 'Healthcare', rating: 'AAA', country: 'US' },
  { ticker: 'UNH', name: 'UnitedHealth', sector: 'Healthcare', rating: 'A+', country: 'US' },
  { ticker: 'PG', name: 'Procter & Gamble', sector: 'Consumer', rating: 'AA-', country: 'US' },
  { ticker: 'KO', name: 'Coca-Cola Co', sector: 'Consumer', rating: 'A+', country: 'US' },
  { ticker: 'MCD', name: 'McDonalds Corp', sector: 'Consumer', rating: 'BBB+', country: 'US' },
  { ticker: 'BMW', name: 'BMW AG', sector: 'Autos', rating: 'A', country: 'DE' },
  { ticker: 'VOW', name: 'Volkswagen AG', sector: 'Autos', rating: 'A-', country: 'DE' },
  { ticker: 'TM', name: 'Toyota Motor', sector: 'Autos', rating: 'A+', country: 'JP' },
  { ticker: 'LVMH', name: 'LVMH', sector: 'Consumer', rating: 'A+', country: 'FR' },
  { ticker: 'NESN', name: 'Nestle SA', sector: 'Consumer', rating: 'AA', country: 'CH' },
  { ticker: 'ROG', name: 'Roche Holding', sector: 'Healthcare', rating: 'AA', country: 'CH' },
  { ticker: 'HSBA', name: 'HSBC Holdings', sector: 'Financials', rating: 'A', country: 'UK' },
  { ticker: 'BCS', name: 'Barclays plc', sector: 'Financials', rating: 'A-', country: 'UK' },
];

const TENORS = ['2Y', '3Y', '5Y', '7Y', '10Y', '15Y', '20Y', '30Y'];
const CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CHF'];
const TRADERS = ['Alice Chen', 'Bob Smith', 'Carol Davis', 'David Kim', 'Eva Mueller', 'Frank Brown'];
const DESKS = ['IG Flow', 'HY Trading', 'EM Credit', 'Structured', 'Index'];
const COUNTERPARTIES = ['Citadel', 'Millennium', 'Two Sigma', 'DE Shaw', 'Bridgewater', 'Point72', 'AQR', 'Renaissance', 'BlackRock', 'Vanguard', 'Fidelity', 'PIMCO'];
const SIGNAL_MODELS = ['MeanReversion', 'Momentum', 'ValueCarry', 'TechFlow', 'SentimentNLP', 'VolRegime'];

// Credit indices
const BENCHMARKS = [
  { id: 'CDX.NA.IG', name: 'CDX NA Investment Grade', region: 'NA' },
  { id: 'CDX.NA.HY', name: 'CDX NA High Yield', region: 'NA' },
  { id: 'ITRX.EUR.IG', name: 'iTraxx Europe', region: 'EU' },
  { id: 'ITRX.EUR.XO', name: 'iTraxx Crossover', region: 'EU' },
  { id: 'UST.2Y', name: 'US Treasury 2Y', region: 'NA' },
  { id: 'UST.5Y', name: 'US Treasury 5Y', region: 'NA' },
  { id: 'UST.10Y', name: 'US Treasury 10Y', region: 'NA' },
  { id: 'UST.30Y', name: 'US Treasury 30Y', region: 'NA' },
];

// ============ HELPERS ============

const randomChoice = (arr) => arr[Math.floor(Math.random() * arr.length)];
const randomBetween = (min, max) => Math.random() * (max - min) + min;
const randomInt = (min, max) => Math.floor(randomBetween(min, max + 1));
const gaussian = () => {
  let u = 0, v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  return Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
};

// Generate CUSIP-like bond identifier
const generateBondId = (issuer, tenor) => {
  const hash = (issuer.ticker + tenor).split('').reduce((a, b) => a + b.charCodeAt(0), 0);
  return `${issuer.ticker}${tenor.replace('Y', '')}${(hash % 900 + 100)}`;
};

// ============ DATA GENERATORS ============

// Base spread by rating (bps)
const RATING_SPREADS = {
  'AAA': 30, 'AA+': 45, 'AA': 55, 'AA-': 70,
  'A+': 85, 'A': 100, 'A-': 120,
  'BBB+': 150, 'BBB': 180, 'BBB-': 220,
  'BB+': 280, 'BB': 350, 'BB-': 450,
};

// Tenor multiplier for spread
const TENOR_MULT = {
  '2Y': 0.7, '3Y': 0.85, '5Y': 1.0, '7Y': 1.1,
  '10Y': 1.2, '15Y': 1.3, '20Y': 1.35, '30Y': 1.4,
};

let rfqId = 1;
let tradeId = 1;
let positionId = 1;
let signalId = 1;

function generateRFQ() {
  const issuer = randomChoice(ISSUERS);
  const tenor = randomChoice(TENORS);
  const bondId = generateBondId(issuer, tenor);
  const side = Math.random() < 0.5 ? 'BID' : 'OFFER';
  const notional = randomChoice([1, 2, 5, 10, 15, 20, 25, 50]) * 1_000_000;
  const baseSpread = RATING_SPREADS[issuer.rating] || 150;
  const tenorMult = TENOR_MULT[tenor] || 1.0;
  const marketSpread = baseSpread * tenorMult + gaussian() * 10;
  
  // Price in 100ths (par = 100)
  const coupon = Math.round((baseSpread / 100 + 2) * 4) / 4; // Rounded to 0.25%
  const price = 100 + gaussian() * 3 + (side === 'BID' ? -0.25 : 0.25);
  
  return {
    rfqId: rfqId++,
    timestamp: Date.now(),
    bondId,
    issuer: issuer.ticker,
    issuerName: issuer.name,
    sector: issuer.sector,
    rating: issuer.rating,
    country: issuer.country,
    tenor,
    coupon,
    currency: issuer.country === 'US' ? 'USD' : 
              issuer.country === 'UK' ? 'GBP' : 
              issuer.country === 'DE' || issuer.country === 'FR' ? 'EUR' :
              issuer.country === 'JP' ? 'JPY' : 
              issuer.country === 'CH' ? 'CHF' : 'USD',
    side,
    notional,
    counterparty: randomChoice(COUNTERPARTIES),
    spread: Math.round(marketSpread * 100) / 100,
    price: Math.round(price * 1000) / 1000,
    status: 'PENDING',
    trader: randomChoice(TRADERS),
    desk: randomChoice(DESKS),
  };
}

function generateTrade(rfq) {
  // 60% of RFQs get filled
  if (Math.random() > 0.6) {
    return { ...rfq, status: 'REJECTED' };
  }
  
  const fillPrice = rfq.price + (rfq.side === 'BID' ? -randomBetween(0.05, 0.25) : randomBetween(0.05, 0.25));
  const spreadCapture = rfq.side === 'BID' 
    ? (rfq.price - fillPrice) * rfq.notional / 100 
    : (fillPrice - rfq.price) * rfq.notional / 100;
  
  return {
    ...rfq,
    tradeId: tradeId++,
    status: 'FILLED',
    fillPrice: Math.round(fillPrice * 1000) / 1000,
    fillTime: Date.now(),
    spreadCapture: Math.round(spreadCapture),
    dv01: Math.round(rfq.notional * parseInt(rfq.tenor) * 0.0001), // Simplified DV01
  };
}

function generatePosition(trade) {
  const direction = trade.side === 'BID' ? 1 : -1;
  return {
    positionId: positionId++,
    bondId: trade.bondId,
    issuer: trade.issuer,
    issuerName: trade.issuerName,
    sector: trade.sector,
    rating: trade.rating,
    tenor: trade.tenor,
    currency: trade.currency,
    notional: trade.notional * direction,
    avgPrice: trade.fillPrice,
    currentPrice: trade.fillPrice,
    unrealizedPnL: 0,
    realizedPnL: trade.spreadCapture,
    dv01: trade.dv01 * direction,
    trader: trade.trader,
    desk: trade.desk,
    timestamp: Date.now(),
  };
}

// Initial FX rates
const fxRates = {
  'USD': 1.0,
  'EUR': 1.08,
  'GBP': 1.26,
  'JPY': 0.0067,
  'CHF': 1.12,
};

function generateFXUpdate() {
  const ccy = randomChoice(CURRENCIES.filter(c => c !== 'USD'));
  const change = gaussian() * 0.001; // Small FX moves
  fxRates[ccy] = Math.round((fxRates[ccy] * (1 + change)) * 10000) / 10000;
  
  return {
    timestamp: Date.now(),
    currency: ccy,
    rate: fxRates[ccy],
    change24h: Math.round(change * 10000) / 100, // bps
  };
}

// Benchmark levels (spreads in bps, yields in %)
const benchmarkLevels = {
  'CDX.NA.IG': 55,
  'CDX.NA.HY': 380,
  'ITRX.EUR.IG': 62,
  'ITRX.EUR.XO': 320,
  'UST.2Y': 4.25,
  'UST.5Y': 4.10,
  'UST.10Y': 4.05,
  'UST.30Y': 4.20,
};

function generateBenchmarkUpdate() {
  const benchmark = randomChoice(BENCHMARKS);
  const isSpread = benchmark.id.startsWith('CDX') || benchmark.id.startsWith('ITRX');
  const change = gaussian() * (isSpread ? 0.5 : 0.01);
  benchmarkLevels[benchmark.id] = Math.round((benchmarkLevels[benchmark.id] + change) * 100) / 100;
  
  return {
    timestamp: Date.now(),
    benchmarkId: benchmark.id,
    name: benchmark.name,
    region: benchmark.region,
    level: benchmarkLevels[benchmark.id],
    change: Math.round(change * 100) / 100,
    isSpread,
  };
}

function generateSignal() {
  const issuer = randomChoice(ISSUERS);
  const model = randomChoice(SIGNAL_MODELS);
  const signal = gaussian(); // Z-score
  const confidence = Math.min(0.99, Math.abs(signal) / 3 + 0.3);
  
  return {
    signalId: signalId++,
    timestamp: Date.now(),
    model,
    issuer: issuer.ticker,
    issuerName: issuer.name,
    sector: issuer.sector,
    signalValue: Math.round(signal * 100) / 100,
    direction: signal > 0 ? 'LONG' : 'SHORT',
    confidence: Math.round(confidence * 100) / 100,
    strength: Math.abs(signal) > 2 ? 'STRONG' : Math.abs(signal) > 1 ? 'MODERATE' : 'WEAK',
  };
}

// Track positions for PnL updates
const positions = new Map();

function updatePositionPnL(position) {
  // Simulate price movement
  const priceMove = gaussian() * 0.1;
  position.currentPrice = Math.round((position.currentPrice + priceMove) * 1000) / 1000;
  position.unrealizedPnL = Math.round((position.currentPrice - position.avgPrice) * position.notional / 100);
  
  // DV01 changes with interest rate movements (duration sensitivity)
  // As prices rise (yields fall), duration typically increases slightly
  // This simulates the convexity effect in fixed income
  const tenor = parseInt(position.tenor) || 5;
  const durationShift = priceMove * 0.02 * tenor; // Proportional to tenor
  const baseDV01 = Math.abs(position.notional) * tenor * 0.0001;
  position.dv01 = Math.round((baseDV01 + baseDV01 * durationShift) * (position.notional >= 0 ? 1 : -1));
  
  position.timestamp = Date.now();
  return position;
}

// ============ SNAPSHOT GENERATORS ============

function generateRFQSnapshot(count = 500) {
  console.log(`[Credit Server] Generating ${count} historical RFQs...`);
  const rfqs = [];
  for (let i = 0; i < count; i++) {
    const rfq = generateRFQ();
    const trade = generateTrade(rfq);
    rfqs.push(trade);
    
    // Build position book from filled trades
    if (trade.status === 'FILLED') {
      const existing = positions.get(trade.bondId);
      if (existing) {
        existing.notional += (trade.side === 'BID' ? 1 : -1) * trade.notional;
        existing.realizedPnL += trade.spreadCapture;
        // Recalculate DV01 based on new notional
        existing.dv01 = Math.round(existing.notional * parseInt(existing.tenor) * 0.0001);
        existing.timestamp = Date.now();
      } else {
        positions.set(trade.bondId, generatePosition(trade));
      }
    }
  }
  return rfqs;
}

function generatePositionSnapshot() {
  return Array.from(positions.values());
}

function generateFXSnapshot() {
  return CURRENCIES.map(ccy => ({
    timestamp: Date.now(),
    currency: ccy,
    rate: fxRates[ccy],
    change24h: 0,
  }));
}

function generateBenchmarkSnapshot() {
  return BENCHMARKS.map(b => ({
    timestamp: Date.now(),
    benchmarkId: b.id,
    name: b.name,
    region: b.region,
    level: benchmarkLevels[b.id],
    change: 0,
    isSpread: b.id.startsWith('CDX') || b.id.startsWith('ITRX'),
  }));
}

function generateSignalSnapshot(count = 50) {
  const signals = [];
  for (let i = 0; i < count; i++) {
    signals.push(generateSignal());
  }
  return signals;
}

// ============ WEBSOCKET SERVER ============

const wss = new WebSocketServer({ port: PORT });

console.log(`
╔════════════════════════════════════════════════════════════════╗
║          CREDIT TRADING Real-Time Server                       ║
╠════════════════════════════════════════════════════════════════╣
║  WebSocket: ws://localhost:${PORT}                               ║
║  Streams:   RFQs, Positions, FX, Benchmarks, Signals           ║
║  Issuers:   ${ISSUERS.length} companies across ${[...new Set(ISSUERS.map(i => i.sector))].length} sectors                     ║
║  Features:  Hit rate, Spread capture, DV01, PnL attribution    ║
╚════════════════════════════════════════════════════════════════╝
`);

wss.on('connection', (ws, req) => {
  const clientAddr = req.socket.remoteAddress;
  console.log(`\n[Credit Server] Client connected from ${clientAddr}`);
  
  let deltaInterval = null;
  let currentDeltaRate = 20; // deltas per second
  let isPaused = false;
  
  // Generate snapshots
  console.log('[Credit Server] Generating snapshots...');
  const rfqSnapshot = generateRFQSnapshot(500);
  const positionSnapshot = generatePositionSnapshot();
  const fxSnapshot = generateFXSnapshot();
  const benchmarkSnapshot = generateBenchmarkSnapshot();
  const signalSnapshot = generateSignalSnapshot(50);
  
  console.log(`[Credit Server] Snapshots ready: ${rfqSnapshot.length} RFQs, ${positionSnapshot.length} positions`);
  
  // Send snapshots
  ws.send(JSON.stringify({ type: 'rfq-snapshot', data: rfqSnapshot }));
  ws.send(JSON.stringify({ type: 'position-snapshot', data: positionSnapshot }));
  ws.send(JSON.stringify({ type: 'fx-snapshot', data: fxSnapshot }));
  ws.send(JSON.stringify({ type: 'benchmark-snapshot', data: benchmarkSnapshot }));
  ws.send(JSON.stringify({ type: 'signal-snapshot', data: signalSnapshot }));
  
  console.log('[Credit Server] Snapshots sent, starting delta stream...');
  
  // Delta stream
  const startDeltaStream = () => {
    if (deltaInterval) clearInterval(deltaInterval);
    
    deltaInterval = setInterval(() => {
      if (isPaused) return;
      
      const deltas = {
        rfqs: [],
        positions: [],
        fx: [],
        benchmarks: [],
        signals: [],
      };
      
      // Generate mix of updates
      const roll = Math.random();
      
      // 40% RFQ
      if (roll < 0.4) {
        const rfq = generateRFQ();
        const trade = generateTrade(rfq);
        deltas.rfqs.push(trade);
        
        if (trade.status === 'FILLED') {
          const existing = positions.get(trade.bondId);
          if (existing) {
            existing.notional += (trade.side === 'BID' ? 1 : -1) * trade.notional;
            existing.realizedPnL += trade.spreadCapture;
            // Recalculate DV01 based on new notional
            existing.dv01 = Math.round(existing.notional * parseInt(existing.tenor) * 0.0001);
            existing.timestamp = Date.now();
            deltas.positions.push({ op: 'update', row: { ...existing } });
          } else {
            const pos = generatePosition(trade);
            positions.set(trade.bondId, pos);
            deltas.positions.push({ op: 'insert', row: pos });
          }
        }
      }
      
      // 35% Position PnL update (more frequent to show live ticking)
      if (roll >= 0.4 && roll < 0.75 && positions.size > 0) {
        const posArr = Array.from(positions.values());
        // Update 3 random positions for more visible PnL changes
        for (let i = 0; i < Math.min(3, posArr.length); i++) {
          const pos = randomChoice(posArr);
          updatePositionPnL(pos);
          deltas.positions.push({ op: 'update', row: { ...pos } });
        }
      }
      
      // 10% FX update
      if (roll >= 0.75 && roll < 0.85) {
        deltas.fx.push(generateFXUpdate());
      }
      
      // 10% Benchmark update
      if (roll >= 0.85 && roll < 0.95) {
        deltas.benchmarks.push(generateBenchmarkUpdate());
      }
      
      // 5% Signal update
      if (roll >= 0.95) {
        deltas.signals.push(generateSignal());
      }
      
      // Send non-empty deltas
      if (deltas.rfqs.length > 0 || deltas.positions.length > 0 || 
          deltas.fx.length > 0 || deltas.benchmarks.length > 0 || deltas.signals.length > 0) {
        ws.send(JSON.stringify({ type: 'delta', data: deltas }));
      }
    }, 1000 / currentDeltaRate);
  };
  
  startDeltaStream();
  
  // Handle messages
  ws.on('message', (message) => {
    try {
      const msg = JSON.parse(message);
      
      if (msg.type === 'set-rate') {
        currentDeltaRate = Math.max(1, Math.min(60, msg.rate));
        startDeltaStream();
        console.log(`[Credit Server] Delta rate set to ${currentDeltaRate}/s`);
      }
      else if (msg.type === 'pause') {
        isPaused = true;
        console.log('[Credit Server] Stream paused');
      }
      else if (msg.type === 'resume') {
        isPaused = false;
        console.log('[Credit Server] Stream resumed');
      }
      else if (msg.type === 'burst') {
        // Generate burst of RFQs
        const burstSize = msg.size || 100;
        console.log(`[Credit Server] Sending burst of ${burstSize} RFQs...`);
        
        const burstRfqs = [];
        const burstPositions = [];
        
        for (let i = 0; i < burstSize; i++) {
          const rfq = generateRFQ();
          const trade = generateTrade(rfq);
          burstRfqs.push(trade);
          
          if (trade.status === 'FILLED') {
            const existing = positions.get(trade.bondId);
            if (existing) {
              existing.notional += (trade.side === 'BID' ? 1 : -1) * trade.notional;
              existing.realizedPnL += trade.spreadCapture;
              // Recalculate DV01 based on new notional
              existing.dv01 = Math.round(existing.notional * parseInt(existing.tenor) * 0.0001);
              existing.timestamp = Date.now();
              burstPositions.push({ op: 'update', row: { ...existing } });
            } else {
              const pos = generatePosition(trade);
              positions.set(trade.bondId, pos);
              burstPositions.push({ op: 'insert', row: pos });
            }
          }
        }
        
        ws.send(JSON.stringify({
          type: 'delta',
          data: { rfqs: burstRfqs, positions: burstPositions, fx: [], benchmarks: [], signals: [] },
          isBurst: true,
          burstTotal: burstSize,
        }));
      }
    } catch (err) {
      console.error('[Credit Server] Message parse error:', err);
    }
  });
  
  ws.on('close', () => {
    if (deltaInterval) clearInterval(deltaInterval);
    console.log('[Credit Server] Client disconnected');
  });
  
  ws.on('error', (err) => {
    console.error('[Credit Server] WebSocket error:', err);
  });
});

console.log(`[Credit Server] Listening on port ${PORT}...`);

