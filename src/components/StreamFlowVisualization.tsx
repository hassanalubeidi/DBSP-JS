/**
 * Enhanced Real-time Stream Analytics Visualization
 * 
 * Visualizes DBSP incremental computation pipeline using React Flow.
 * Features:
 * - Hierarchical auto-layout with clear stage columns
 * - SQL query display on each node
 * - Live data preview showing actual flowing records
 * - Edge throughput metrics
 * - Interactive node details panel
 */

import { useMemo, useEffect, useState, useCallback, memo, useRef } from 'react';
import {
  ReactFlow,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  useReactFlow,
  useUpdateNodeInternals,
  ReactFlowProvider,
  Position,
  Handle,
  type Node,
  type Edge,
  type NodeTypes,
  MarkerType,
  Panel,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import Dagre from 'dagre';

// ============ AUTO-LAYOUT ============

const getLayoutedElements = (
  nodes: Node[],
  edges: Edge[],
  direction: 'TB' | 'LR' = 'LR'
) => {
  const dagreGraph = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));
  
  dagreGraph.setGraph({ 
    rankdir: direction,
    nodesep: 80,
    ranksep: 150,
    marginx: 40,
    marginy: 40,
  });

  // Add nodes to dagre
  nodes.forEach((node) => {
    // Estimate node dimensions based on type
    const width = node.type === 'join' ? 280 : node.type === 'source' ? 260 : 200;
    const height = node.type === 'join' ? 280 : node.type === 'source' ? 240 : 180;
    dagreGraph.setNode(node.id, { width, height });
  });

  // Add edges to dagre
  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  // Run the layout algorithm
  Dagre.layout(dagreGraph);

  // Apply the calculated positions back to nodes
  const layoutedNodes = nodes.map((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    const width = node.type === 'join' ? 280 : node.type === 'source' ? 260 : 200;
    const height = node.type === 'join' ? 280 : node.type === 'source' ? 240 : 180;
    
    return {
      ...node,
      position: {
        x: nodeWithPosition.x - width / 2,
        y: nodeWithPosition.y - height / 2,
      },
    };
  });

  return { nodes: layoutedNodes, edges };
};

// ============ TYPES ============

interface StreamMetrics {
  count: number;
  rate: number;
  avgLatencyMs: number;
  bufferUtil: number;
  isLagging: boolean;
}

interface JoinMetrics {
  leftCount: number;
  rightCount: number;
  resultCount: number;
  avgLatencyMs: number;
  selectivity: number;
}

interface AggMetrics {
  inputCount: number;
  outputCount: number;
  avgLatencyMs: number;
  groups: number;
}

// Sample data record for preview
interface DataSample {
  id: string;
  fields: Record<string, string | number>;
  timestamp?: number;
}

export interface StreamFlowProps {
  // Source streams
  rfqMetrics: StreamMetrics;
  positionMetrics: StreamMetrics;
  signalMetrics: StreamMetrics;
  
  // Join metrics
  rfqSignalJoin: JoinMetrics;
  posSignalJoin: JoinMetrics;
  posRfqJoin: JoinMetrics;
  
  // Aggregation metrics
  sectorAgg: AggMetrics;
  deskAgg: AggMetrics;
  traderAgg: AggMetrics;
  ratingAgg: AggMetrics;
  tenorAgg: AggMetrics;
  counterpartyAgg: AggMetrics;
  modelAgg: AggMetrics;
  
  // Computed outputs
  totalPnL: number;
  hitRate: number;
  signalAlignedCount: number;
  
  // Risk metrics
  totalDV01?: number;
  concentrationHHI?: number;

  // Optional: sample data for preview
  rfqSamples?: DataSample[];
  positionSamples?: DataSample[];
  signalSamples?: DataSample[];
}

// ============ LAYOUT CONSTANTS ============
// Hierarchical column-based layout for clear data flow visualization

const LAYOUT = {
  STAGE_GAP: 300,        // Horizontal gap between stages
  NODE_GAP_V: 180,       // Vertical gap between nodes
  PADDING_X: 40,
  PADDING_Y: 30,
  
  // Stage X positions (left to right)
  SOURCES_X: 0,
  JOINS_X: 340,
  AGGS_X: 680,
  OUTPUTS_X: 1020,
  
  // Node widths for alignment
  SOURCE_WIDTH: 280,
  JOIN_WIDTH: 300,
  AGG_WIDTH: 240,
  OUTPUT_WIDTH: 220,
};

// ============ FORMAT HELPERS ============

const formatNumber = (n: number) => {
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return n.toFixed(0);
};

const formatMs = (ms: number) => {
  if (ms < 0.001) return '<1μs';
  if (ms < 1) return `${(ms * 1000).toFixed(0)}μs`;
  return `${ms.toFixed(2)}ms`;
};

const formatCurrency = (value: number) => {
  const sign = value >= 0 ? '+' : '-';
  const abs = Math.abs(value);
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(1)}K`;
  return `${sign}$${abs.toFixed(0)}`;
};

const getLatencyColor = (ms: number) => {
  if (ms < 0.5) return '#00d4aa';
  if (ms < 2) return '#00a8ff';
  if (ms < 5) return '#ffd000';
  return '#ff6b6b';
};

const getLatencyGrade = (ms: number) => {
  if (ms < 0.5) return 'A+';
  if (ms < 1) return 'A';
  if (ms < 2) return 'B';
  if (ms < 5) return 'C';
  return 'D';
};


// ============ SQL QUERIES FOR EACH NODE ============

const SQL_QUERIES = {
  'src-rfq': `-- RFQ Input Stream
SELECT rfqId, counterparty, bondId, 
       issuer, side, notional, 
       spread, status, timestamp
FROM rfqs
-- Δ = incoming quote requests`,

  'src-position': `-- Position Input Stream
SELECT positionId, bondId, issuer,
       trader, desk, sector, rating,
       tenor, notional, dv01,
       unrealizedPnL, realizedPnL
FROM positions
-- Δ = position updates`,

  'src-signal': `-- Signal Input Stream
SELECT signalId, issuer, model,
       direction, strength, 
       signalValue, confidence
FROM signals
-- Δ = ML model predictions`,

  'join-rfq-signal': `-- Bilinear Join: RFQ × Signal
SELECT r.*, s.direction, s.confidence
FROM rfqs r
INNER JOIN signals s 
  ON r.issuer = s.issuer
WHERE s.confidence > 0.6

-- Incremental: 
-- Δ(R⋈S) = ΔR⋈S + R⋈ΔS + ΔR⋈ΔS
-- Complexity: O(|ΔR| + |ΔS|)`,

  'join-pos-signal': `-- Bilinear Join: Position × Signal
SELECT p.*, s.model, s.direction,
       s.confidence
FROM positions p
INNER JOIN signals s 
  ON p.issuer = s.issuer

-- Track signal effectiveness on P&L
-- Incremental join complexity: O(|Δ|)`,

  'join-pos-rfq': `-- Bilinear Join: Position × RFQ  
SELECT p.desk, p.trader, r.status,
       r.fillPrice, r.notional
FROM positions p
INNER JOIN rfqs r 
  ON p.bondId = r.bondId

-- Execution quality analysis
-- Match trades with RFQ flow`,

  'agg-sector': `-- Aggregation: GROUP BY sector
SELECT sector,
       SUM(notional) AS notional,
       SUM(unrealizedPnL + realizedPnL) AS pnl,
       SUM(dv01) AS dv01,
       COUNT(*) AS positionCount
FROM positions
GROUP BY sector

-- Incremental: Δ(SUM) = SUM(Δ)`,

  'agg-desk': `-- Aggregation: GROUP BY desk
SELECT desk,
       SUM(notional) AS notional,
       SUM(unrealizedPnL + realizedPnL) AS pnl,
       COUNT(*) AS tradeCount
FROM positions
GROUP BY desk`,

  'agg-trader': `-- Aggregation: GROUP BY trader, desk
SELECT trader, desk,
       SUM(notional) AS notional,
       SUM(unrealizedPnL + realizedPnL) AS pnl,
       COUNT(*) AS tradeCount
FROM positions
GROUP BY trader, desk`,

  'agg-rating': `-- Aggregation: GROUP BY rating
SELECT rating,
       SUM(notional) AS notional,
       SUM(dv01) AS dv01,
       COUNT(*) AS count
FROM positions
GROUP BY rating`,

  'agg-tenor': `-- Aggregation: GROUP BY tenor
SELECT tenor,
       SUM(notional) AS notional,
       SUM(dv01) AS dv01,
       COUNT(*) AS count
FROM positions
GROUP BY tenor`,

  'agg-cp': `-- Aggregation: GROUP BY counterparty
SELECT counterparty,
       COUNT(*) AS rfqCount,
       SUM(notional) AS totalNotional,
       SUM(spreadCapture) AS spreadCapture
FROM rfqs
GROUP BY counterparty`,

  'agg-model': `-- Aggregation: GROUP BY model
SELECT model,
       COUNT(*) AS signalCount,
       AVG(signalValue) AS avgSignal,
       AVG(confidence) AS avgConfidence
FROM signals
GROUP BY model`,

  'out-pnl': `-- Output: Total P&L
SELECT SUM(unrealizedPnL + realizedPnL) AS total_pnl
FROM positions

-- Real-time aggregated P&L
-- Updates incrementally with each delta`,

  'out-hitrate': `-- Output: Hit Rate
SELECT 
  COUNT(CASE WHEN status='FILLED' THEN 1 END) / 
  COUNT(*) AS hit_rate
FROM rfqs

-- Execution success metric`,

  'out-aligned': `-- Output: Signal-Aligned Trades
SELECT COUNT(*) AS aligned_count
FROM rfqs r
JOIN signals s ON r.issuer = s.issuer
WHERE s.confidence > 0.6
  AND r.side = CASE s.direction 
    WHEN 'LONG' THEN 'BID' ELSE 'ASK' END`,

  'out-execution': `-- Output: Execution Quality
SELECT COUNT(*) AS matched_count
FROM positions p
JOIN rfqs r ON p.bondId = r.bondId

-- Cross-reference analysis`,

  'risk-dv01': `-- Risk: DV01 Exposure
SELECT SUM(ABS(dv01)) AS total_dv01
FROM positions

-- Interest rate sensitivity
-- Risk limit: $1M DV01`,

  'risk-concentration': `-- Risk: Concentration (HHI)
SELECT SUM(POWER(share, 2)) * 10000 AS hhi
FROM (
  SELECT ABS(notional) / total AS share
  FROM sector_totals
)
-- HHI > 2500 = concentrated`,
};

// ============ SELECTED NODE CONTEXT ============

interface SelectedNodeData {
  id: string;
  type: string;
  label: string;
  sql: string;
  metrics: Record<string, string | number>;
  samples?: DataSample[];
}

// ============ CUSTOM NODES ============

// Source Node - Data ingestion point with SQL display
const SourceNode = memo(({ data, selected }: { data: { 
  label: string; 
  metrics: StreamMetrics; 
  icon: string; 
  color: string;
  description: string;
  sql: string;
  onSelect: (data: SelectedNodeData) => void;
  layoutDirection?: 'LR' | 'TB';
}, selected?: boolean }) => {
  const { label, metrics, icon, color, description, sql, onSelect, layoutDirection = 'LR' } = data;
  const [pulse, setPulse] = useState(false);
  const [showSql, setShowSql] = useState(false);
  
  useEffect(() => {
    if (metrics.rate > 0) {
      setPulse(true);
      const timeout = setTimeout(() => setPulse(false), 200);
      return () => clearTimeout(timeout);
    }
  }, [metrics.count, metrics.rate]);
  
  const throughput = metrics.rate * (1 - metrics.bufferUtil);

  const handleClick = () => {
    onSelect({
      id: data.label,
      type: 'source',
      label: data.label,
      sql,
      metrics: {
        'Records': formatNumber(metrics.count),
        'Rate': `${metrics.rate}/s`,
        'Latency': formatMs(metrics.avgLatencyMs),
        'Buffer': `${(metrics.bufferUtil * 100).toFixed(0)}%`,
        'Status': metrics.isLagging ? 'Lagging' : 'Healthy',
      }
    });
  };
  
  return (
    <div 
      className={`flow-node source-node ${pulse ? 'pulse' : ''} ${metrics.isLagging ? 'lagging' : ''} ${selected ? 'selected' : ''}`}
      onClick={handleClick}
    >
      <Handle type="source" position={layoutDirection === 'TB' ? Position.Bottom : Position.Right} style={{ background: color }} />
      
      <div className="node-header" style={{ borderColor: color }}>
        <span className="node-icon">{icon}</span>
        <div className="node-title">
          <span className="node-label">{label}</span>
          <span className="node-badge source">STREAM</span>
        </div>
        <button 
          className="sql-toggle-btn"
          onClick={(e) => { e.stopPropagation(); setShowSql(!showSql); }}
          title="Toggle SQL"
        >
          {showSql ? '▼' : '▶'} SQL
        </button>
      </div>
      
      {showSql && (
        <div className="node-sql-preview">
          <pre>{sql.slice(0, 150)}...</pre>
        </div>
      )}
      
      <div className="node-description">{description}</div>
      
      <div className="node-stats">
        <div className="stat-row">
          <span className="stat-label">Records</span>
          <span className="stat-value primary">{formatNumber(metrics.count)}</span>
        </div>
        <div className="stat-row">
          <span className="stat-label">Rate</span>
          <span className="stat-value highlight">{metrics.rate.toFixed(0)}/s</span>
        </div>
        <div className="stat-row">
          <span className="stat-label">Latency</span>
          <span className="stat-value" style={{ color: getLatencyColor(metrics.avgLatencyMs) }}>
            {formatMs(metrics.avgLatencyMs)} <span className="grade">{getLatencyGrade(metrics.avgLatencyMs)}</span>
          </span>
        </div>
      </div>
      
      <div className="throughput-bar">
        <div 
          className="throughput-fill"
          style={{ 
            width: `${Math.min(100, (throughput / 100) * 100)}%`,
            background: `linear-gradient(90deg, ${color}88, ${color})`,
          }}
        />
        <span className="throughput-label">{throughput.toFixed(0)}/s effective</span>
      </div>
      
      {metrics.isLagging && (
        <div className="warning-badge">
          <span className="warning-icon">⚠️</span>
          <span>Backpressure</span>
        </div>
      )}

      <div className="node-data-preview">
        <span className="preview-label">🔄 Live Stream</span>
      </div>
    </div>
  );
});
SourceNode.displayName = 'SourceNode';

// Join Node - Bilinear join operation with SQL
const JoinNode = memo(({ data, selected }: { data: { 
  label: string; 
  metrics: JoinMetrics; 
  leftLabel: string; 
  rightLabel: string;
  joinKey: string;
  traderInsight: string;
  sql: string;
  onSelect: (data: SelectedNodeData) => void;
  layoutDirection?: 'LR' | 'TB';
}, selected?: boolean }) => {
  const { label, metrics, leftLabel, rightLabel, joinKey, traderInsight, sql, onSelect, layoutDirection = 'LR' } = data;
  const [showSql, setShowSql] = useState(false);
  
  const selectivityPct = metrics.leftCount > 0 && metrics.rightCount > 0 
    ? (metrics.resultCount / Math.max(1, metrics.leftCount * metrics.rightCount)) * 100 
    : 0;
  
  const isBottleneck = metrics.avgLatencyMs > 5;
  const cardinality = metrics.leftCount * metrics.rightCount;

  const handleClick = () => {
    onSelect({
      id: label,
      type: 'join',
      label,
      sql,
      metrics: {
        'Left Count': formatNumber(metrics.leftCount),
        'Right Count': formatNumber(metrics.rightCount),
        'Result': formatNumber(metrics.resultCount),
        'Selectivity': `${selectivityPct.toFixed(4)}%`,
        'Cardinality': formatNumber(cardinality),
        'Latency': formatMs(metrics.avgLatencyMs),
      }
    });
  };
  
  return (
    <div 
      className={`flow-node join-node ${isBottleneck ? 'bottleneck' : ''} ${selected ? 'selected' : ''}`}
      onClick={handleClick}
    >
      <Handle 
        type="target" 
        position={layoutDirection === 'TB' ? Position.Top : Position.Left} 
        id="left" 
        style={{ 
          [layoutDirection === 'TB' ? 'left' : 'top']: '30%', 
          background: '#00d4aa' 
        }} 
      />
      <Handle 
        type="target" 
        position={layoutDirection === 'TB' ? Position.Top : Position.Left} 
        id="right" 
        style={{ 
          [layoutDirection === 'TB' ? 'left' : 'top']: '70%', 
          background: '#ffd000' 
        }} 
      />
      <Handle type="source" position={layoutDirection === 'TB' ? Position.Bottom : Position.Right} style={{ background: '#00a8ff' }} />
      
      <div className="node-header" style={{ borderColor: '#ffd000' }}>
        <span className="node-icon join-symbol">⋈</span>
        <div className="node-title">
          <span className="node-label">{label}</span>
          <span className="node-badge join">JOIN</span>
        </div>
        <button 
          className="sql-toggle-btn"
          onClick={(e) => { e.stopPropagation(); setShowSql(!showSql); }}
          title="Toggle SQL"
        >
          {showSql ? '▼' : '▶'} SQL
        </button>
      </div>

      {showSql && (
        <div className="node-sql-preview">
          <pre>{sql.slice(0, 180)}...</pre>
        </div>
      )}
      
      <div className="join-visualization">
        <div className="join-input">
          <span className="join-set">{leftLabel}</span>
          <span className="join-count">{formatNumber(metrics.leftCount)}</span>
        </div>
        <div className="join-operator">
          <span className="join-key">ON {joinKey}</span>
          <span className="join-arrow">⋈</span>
        </div>
        <div className="join-input">
          <span className="join-set">{rightLabel}</span>
          <span className="join-count">{formatNumber(metrics.rightCount)}</span>
        </div>
      </div>
      
      <div className="join-output">
        <span className="output-arrow">→</span>
        <span className="output-count">{formatNumber(metrics.resultCount)}</span>
        <span className="output-label">matched</span>
      </div>
      
      <div className="node-stats compact">
        <div className="stat-row">
          <span className="stat-label">Selectivity</span>
          <span className="stat-value">{selectivityPct.toFixed(4)}%</span>
        </div>
        <div className="stat-row">
          <span className="stat-label">Latency</span>
          <span className="stat-value" style={{ color: getLatencyColor(metrics.avgLatencyMs) }}>
            {formatMs(metrics.avgLatencyMs)} {getLatencyGrade(metrics.avgLatencyMs)}
          </span>
        </div>
      </div>
      
      <div className="trader-insight">
        <span className="insight-icon">💡</span>
        <span>{traderInsight}</span>
      </div>
      
      <div className="incremental-note">
        <code>Δ(L⋈R) = ΔL⋈R + L⋈ΔR</code>
      </div>
    </div>
  );
});
JoinNode.displayName = 'JoinNode';

// Aggregation Node - GROUP BY operation with SQL
const AggNode = memo(({ data, selected }: { data: { 
  label: string; 
  metrics: AggMetrics; 
  groupBy: string; 
  color: string;
  outputMetric: string;
  sql: string;
  onSelect: (data: SelectedNodeData) => void;
  layoutDirection?: 'LR' | 'TB';
}, selected?: boolean }) => {
  const { label, metrics, groupBy, color, outputMetric, sql, onSelect, layoutDirection = 'LR' } = data;
  const [showSql, setShowSql] = useState(false);
  
  const compressionRatio = metrics.inputCount > 0 ? metrics.inputCount / Math.max(metrics.outputCount, 1) : 0;

  const handleClick = () => {
    onSelect({
      id: label,
      type: 'agg',
      label,
      sql,
      metrics: {
        'Input': formatNumber(metrics.inputCount),
        'Groups': metrics.groups,
        'Compression': `${compressionRatio.toFixed(0)}:1`,
        'Output': outputMetric,
        'Latency': formatMs(metrics.avgLatencyMs),
      }
    });
  };
  
  return (
    <div 
      className={`flow-node agg-node ${selected ? 'selected' : ''}`} 
      style={{ '--accent-color': color } as React.CSSProperties}
      onClick={handleClick}
    >
      <Handle type="target" position={layoutDirection === 'TB' ? Position.Top : Position.Left} style={{ background: color }} />
      <Handle type="source" position={layoutDirection === 'TB' ? Position.Bottom : Position.Right} style={{ background: color }} />
      
      <div className="node-header" style={{ borderColor: color }}>
        <span className="node-icon agg-symbol">Σ</span>
        <div className="node-title">
          <span className="node-label">{label}</span>
          <span className="node-badge agg">AGG</span>
        </div>
        <button 
          className="sql-toggle-btn"
          onClick={(e) => { e.stopPropagation(); setShowSql(!showSql); }}
          title="Toggle SQL"
        >
          {showSql ? '▼' : '▶'} SQL
        </button>
      </div>

      {showSql && (
        <div className="node-sql-preview">
          <pre>{sql.slice(0, 150)}...</pre>
        </div>
      )}
      
      <div className="agg-formula">
        <code>GROUP BY {groupBy}</code>
      </div>
      
      <div className="agg-flow">
        <span className="flow-in">{formatNumber(metrics.inputCount)}</span>
        <span className="flow-arrow">→</span>
        <span className="flow-out" style={{ color }}>{metrics.groups}</span>
        <span className="flow-label">groups</span>
      </div>
      
      <div className="node-stats compact">
        <div className="stat-row">
          <span className="stat-label">Compression</span>
          <span className="stat-value">{compressionRatio.toFixed(0)}:1</span>
        </div>
        <div className="stat-row">
          <span className="stat-label">Latency</span>
          <span className="stat-value" style={{ color: getLatencyColor(metrics.avgLatencyMs) }}>
            {formatMs(metrics.avgLatencyMs)}
          </span>
        </div>
      </div>
      
      <div className="incremental-note">
        <code>Δ(SUM) = SUM(Δ)</code>
      </div>
    </div>
  );
});
AggNode.displayName = 'AggNode';

// Output Node - Final metrics sink with SQL
const OutputNode = memo(({ data, selected }: { data: { 
  label: string; 
  value: string; 
  subValue?: string; 
  color: string; 
  icon: string;
  trend?: 'up' | 'down' | 'flat';
  significance?: string;
  sql: string;
  onSelect: (data: SelectedNodeData) => void;
  layoutDirection?: 'LR' | 'TB';
}, selected?: boolean }) => {
  const { label, value, subValue, color, icon, trend, significance, sql, onSelect, layoutDirection = 'LR' } = data;
  const [showSql, setShowSql] = useState(false);

  const handleClick = () => {
    onSelect({
      id: label,
      type: 'output',
      label,
      sql,
      metrics: {
        'Value': value,
        'Description': subValue || '',
        'Significance': significance || '',
      }
    });
  };
  
  return (
    <div 
      className={`flow-node output-node ${selected ? 'selected' : ''}`} 
      style={{ '--accent-color': color } as React.CSSProperties}
      onClick={handleClick}
    >
      <Handle type="target" position={layoutDirection === 'TB' ? Position.Top : Position.Left} style={{ background: color }} />
      
      <div className="node-header" style={{ borderColor: color }}>
        <span className="node-icon">{icon}</span>
        <div className="node-title">
          <span className="node-label">{label}</span>
          <span className="node-badge output">OUTPUT</span>
        </div>
        <button 
          className="sql-toggle-btn"
          onClick={(e) => { e.stopPropagation(); setShowSql(!showSql); }}
          title="Toggle SQL"
        >
          {showSql ? '▼' : '▶'} SQL
        </button>
      </div>

      {showSql && (
        <div className="node-sql-preview">
          <pre>{sql.slice(0, 120)}...</pre>
        </div>
      )}
      
      <div className="output-value" style={{ color }}>
        {trend === 'up' && <span className="trend up">▲</span>}
        {trend === 'down' && <span className="trend down">▼</span>}
        {value}
      </div>
      
      {subValue && <div className="output-subvalue">{subValue}</div>}
      
      {significance && (
        <div className="output-significance">
          <span className="sig-dot" style={{ background: color }} />
          {significance}
        </div>
      )}
    </div>
  );
});
OutputNode.displayName = 'OutputNode';

// Risk Node - Risk metric display with SQL
const RiskNode = memo(({ data, selected }: { data: { 
  label: string; 
  value: string; 
  status: 'safe' | 'caution' | 'danger';
  icon: string;
  details: string;
  sql: string;
  onSelect: (data: SelectedNodeData) => void;
  layoutDirection?: 'LR' | 'TB';
}, selected?: boolean }) => {
  const { label, value, status, icon, details, sql, onSelect, layoutDirection = 'LR' } = data;
  const [showSql, setShowSql] = useState(false);
  const statusColors = {
    safe: '#00d4aa',
    caution: '#ffd000',
    danger: '#ff6b6b',
  };

  const handleClick = () => {
    onSelect({
      id: label,
      type: 'risk',
      label,
      sql,
      metrics: {
        'Value': value,
        'Status': status.toUpperCase(),
        'Details': details,
      }
    });
  };
  
  return (
    <div 
      className={`flow-node risk-node ${status} ${selected ? 'selected' : ''}`}
      onClick={handleClick}
    >
      <Handle type="target" position={layoutDirection === 'TB' ? Position.Top : Position.Left} style={{ background: statusColors[status] }} />
      
      <div className="node-header" style={{ borderColor: statusColors[status] }}>
        <span className="node-icon">{icon}</span>
        <div className="node-title">
          <span className="node-label">{label}</span>
          <span className="node-badge risk" style={{ background: statusColors[status] }}>RISK</span>
        </div>
        <button 
          className="sql-toggle-btn"
          onClick={(e) => { e.stopPropagation(); setShowSql(!showSql); }}
          title="Toggle SQL"
        >
          {showSql ? '▼' : '▶'} SQL
        </button>
      </div>

      {showSql && (
        <div className="node-sql-preview">
          <pre>{sql.slice(0, 120)}...</pre>
        </div>
      )}
      
      <div className="risk-value" style={{ color: statusColors[status] }}>{value}</div>
      <div className="risk-details">{details}</div>
    </div>
  );
});
RiskNode.displayName = 'RiskNode';

// Stage Label Node - Visual separator for pipeline stages
const StageLabelNode = memo(({ data }: { data: { label: string; color: string; count: number } }) => {
  return (
    <div className="stage-label-node" style={{ borderColor: data.color }}>
      <span className="stage-name">{data.label}</span>
      <span className="stage-count">{data.count} nodes</span>
    </div>
  );
});
StageLabelNode.displayName = 'StageLabelNode';

// ============ NODE TYPES ============

const nodeTypes: NodeTypes = {
  source: SourceNode,
  join: JoinNode,
  agg: AggNode,
  output: OutputNode,
  risk: RiskNode,
  stageLabel: StageLabelNode,
};

// ============ DETAILS PANEL ============

const DetailsPanel = memo(({ node, onClose }: { node: SelectedNodeData | null; onClose: () => void }) => {
  if (!node) return null;

  const typeColors: Record<string, string> = {
    source: '#00a8ff',
    join: '#ffd000',
    agg: '#00d4aa',
    output: '#9c6ade',
    risk: '#ff6b6b',
  };

  return (
    <div className="details-panel">
      <div className="details-header" style={{ borderColor: typeColors[node.type] }}>
        <div className="details-title">
          <span className="details-type-badge" style={{ background: typeColors[node.type] }}>
            {node.type.toUpperCase()}
          </span>
          <h3>{node.label}</h3>
        </div>
        <button className="details-close" onClick={onClose}>✕</button>
      </div>

      <div className="details-content">
        <div className="details-section">
          <h4>📊 Metrics</h4>
          <div className="details-metrics">
            {Object.entries(node.metrics).map(([key, value]) => (
              <div key={key} className="detail-metric">
                <span className="metric-key">{key}</span>
                <span className="metric-value">{value}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="details-section sql-section">
          <h4>📝 SQL Query</h4>
          <div className="details-sql">
            <pre>{node.sql}</pre>
          </div>
        </div>

        {node.samples && node.samples.length > 0 && (
          <div className="details-section">
            <h4>🔄 Sample Data</h4>
            <div className="details-samples">
              {node.samples.slice(0, 3).map((sample, i) => (
                <div key={sample.id || i} className="sample-row">
                  <span className="sample-id">{sample.id}</span>
                  <span className="sample-fields">
                    {Object.entries(sample.fields).slice(0, 3).map(([k, v]) => (
                      <span key={k} className="sample-field">{k}: {v}</span>
                    ))}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
});
DetailsPanel.displayName = 'DetailsPanel';

// ============ PIPELINE HEALTH PANEL ============

const PipelineHealthPanel = memo(({ 
  avgLatency, 
  maxLatency, 
  totalEvents, 
  totalJoins, 
  lagging, 
  grade 
}: { 
  avgLatency: number;
  maxLatency: number;
  totalEvents: number;
  totalJoins: number;
  lagging: boolean;
  grade: string;
}) => (
  <div className="pipeline-health-panel enhanced">
    <div className="health-header">
      <span className="health-icon">🔄</span>
      <span className="health-title">Pipeline Health</span>
      <span className={`health-grade grade-${grade.toLowerCase()}`}>
        {grade}
      </span>
    </div>
    <div className="health-stats">
      <div className="health-stat">
        <span className="stat-label">Avg Latency</span>
        <span className="stat-value" style={{ color: getLatencyColor(avgLatency) }}>
          {formatMs(avgLatency)}
        </span>
      </div>
      <div className="health-stat">
        <span className="stat-label">Max Latency</span>
        <span className="stat-value" style={{ color: getLatencyColor(maxLatency) }}>
          {formatMs(maxLatency)}
        </span>
      </div>
      <div className="health-stat">
        <span className="stat-label">Total Events</span>
        <span className="stat-value highlight">{formatNumber(totalEvents)}</span>
      </div>
      <div className="health-stat">
        <span className="stat-label">Join Results</span>
        <span className="stat-value gold">{formatNumber(totalJoins)}</span>
      </div>
      <div className="health-stat">
        <span className="stat-label">Status</span>
        <span className={`stat-value ${lagging ? 'lagging' : 'healthy'}`}>
          {lagging ? '⚠️ Lagging' : '✓ Healthy'}
        </span>
      </div>
    </div>
  </div>
));
PipelineHealthPanel.displayName = 'PipelineHealthPanel';

// ============ DATA FLOW PANEL ============

const DataFlowPanel = memo(({ 
  rfqCount, 
  posCount, 
  signalCount,
  joinCount,
  rate 
}: { 
  rfqCount: number;
  posCount: number;
  signalCount: number;
  joinCount: number;
  rate: number;
}) => {
  return (
    <div className="data-flow-panel">
      <div className="flow-panel-header">
        <span className="flow-panel-title">🌊 Live Data Flow</span>
        <span className="flow-panel-rate">{rate}/s</span>
      </div>
      
      <div className="flow-panel-stats">
        <div className="flow-stat">
          <span className="flow-stat-color" style={{ background: '#00a8ff' }} />
          <span className="flow-stat-label">RFQs</span>
          <span className="flow-stat-value">{formatNumber(rfqCount)}</span>
        </div>
        <div className="flow-stat">
          <span className="flow-stat-color" style={{ background: '#ffd000' }} />
          <span className="flow-stat-label">Positions</span>
          <span className="flow-stat-value">{formatNumber(posCount)}</span>
        </div>
        <div className="flow-stat">
          <span className="flow-stat-color" style={{ background: '#ff6b6b' }} />
          <span className="flow-stat-label">Signals</span>
          <span className="flow-stat-value">{formatNumber(signalCount)}</span>
        </div>
        <div className="flow-stat highlight">
          <span className="flow-stat-color" style={{ background: '#9c6ade' }} />
          <span className="flow-stat-label">Joined</span>
          <span className="flow-stat-value">{formatNumber(joinCount)}</span>
        </div>
      </div>
    </div>
  );
});
DataFlowPanel.displayName = 'DataFlowPanel';

// ============ MAIN COMPONENT ============

// Inner component that uses useReactFlow (must be inside ReactFlowProvider)
function StreamFlowVisualizationInner(props: StreamFlowProps) {
  const {
    rfqMetrics, positionMetrics, signalMetrics,
    rfqSignalJoin, posSignalJoin, posRfqJoin,
    sectorAgg, deskAgg, traderAgg, ratingAgg, tenorAgg, counterpartyAgg, modelAgg,
    totalPnL, hitRate, signalAlignedCount,
    totalDV01 = 0,
    concentrationHHI = 0,
  } = props;

  const [selectedNode, setSelectedNode] = useState<SelectedNodeData | null>(null);
  const [showMinimap, setShowMinimap] = useState(true);
  const [zenMode, setZenMode] = useState(true); // Default to zen mode
  const [layoutDirection, setLayoutDirection] = useState<'LR' | 'TB'>('TB'); // Default to vertical layout
  const { fitView } = useReactFlow();
  const updateNodeInternals = useUpdateNodeInternals();

  // Calculate pipeline health
  const pipelineHealth = useMemo(() => {
    const latencies = [
      rfqMetrics.avgLatencyMs,
      positionMetrics.avgLatencyMs,
      signalMetrics.avgLatencyMs,
      rfqSignalJoin.avgLatencyMs,
      posSignalJoin.avgLatencyMs,
      posRfqJoin.avgLatencyMs,
      sectorAgg.avgLatencyMs,
    ];
    const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length;
    const maxLatency = Math.max(...latencies);
    const lagging = [rfqMetrics, positionMetrics, signalMetrics].some(m => m.isLagging);
    
    return {
      avgLatency,
      maxLatency,
      lagging,
      grade: avgLatency < 1 ? 'A' : avgLatency < 3 ? 'B' : avgLatency < 5 ? 'C' : 'D',
      totalEvents: rfqMetrics.count + positionMetrics.count + signalMetrics.count,
      totalJoins: rfqSignalJoin.resultCount + posSignalJoin.resultCount + posRfqJoin.resultCount,
    };
  }, [rfqMetrics, positionMetrics, signalMetrics, rfqSignalJoin, posSignalJoin, posRfqJoin, sectorAgg]);

  const handleNodeSelect = useCallback((data: SelectedNodeData) => {
    setSelectedNode(data);
  }, []);

  // Static node configuration - positions and structure that never change
  // This is separate from dynamic data to preserve user-dragged positions
  const staticNodeConfig = useMemo(() => {
    const sourceY = LAYOUT.PADDING_Y;
    const joinY = sourceY + 20;
    const aggY = sourceY;
    const aggGap = LAYOUT.NODE_GAP_V * 0.85;
    const outputY = sourceY + 20;
    const outputGap = LAYOUT.NODE_GAP_V * 0.95;

    return {
      'src-rfq': { type: 'source', position: { x: LAYOUT.SOURCES_X, y: sourceY } },
      'src-position': { type: 'source', position: { x: LAYOUT.SOURCES_X, y: sourceY + LAYOUT.NODE_GAP_V * 1.2 } },
      'src-signal': { type: 'source', position: { x: LAYOUT.SOURCES_X, y: sourceY + LAYOUT.NODE_GAP_V * 2.4 } },
      'join-rfq-signal': { type: 'join', position: { x: LAYOUT.JOINS_X, y: joinY } },
      'join-pos-signal': { type: 'join', position: { x: LAYOUT.JOINS_X, y: joinY + LAYOUT.NODE_GAP_V * 1.3 } },
      'join-pos-rfq': { type: 'join', position: { x: LAYOUT.JOINS_X, y: joinY + LAYOUT.NODE_GAP_V * 2.6 } },
      'agg-sector': { type: 'agg', position: { x: LAYOUT.AGGS_X, y: aggY } },
      'agg-desk': { type: 'agg', position: { x: LAYOUT.AGGS_X, y: aggY + aggGap } },
      'agg-trader': { type: 'agg', position: { x: LAYOUT.AGGS_X, y: aggY + aggGap * 2 } },
      'agg-rating': { type: 'agg', position: { x: LAYOUT.AGGS_X, y: aggY + aggGap * 3 } },
      'agg-tenor': { type: 'agg', position: { x: LAYOUT.AGGS_X, y: aggY + aggGap * 4 } },
      'agg-cp': { type: 'agg', position: { x: LAYOUT.AGGS_X, y: aggY + aggGap * 5 } },
      'agg-model': { type: 'agg', position: { x: LAYOUT.AGGS_X, y: aggY + aggGap * 6 } },
      'out-pnl': { type: 'output', position: { x: LAYOUT.OUTPUTS_X, y: outputY } },
      'out-hitrate': { type: 'output', position: { x: LAYOUT.OUTPUTS_X, y: outputY + outputGap } },
      'out-aligned': { type: 'output', position: { x: LAYOUT.OUTPUTS_X, y: outputY + outputGap * 2 } },
      'out-execution': { type: 'output', position: { x: LAYOUT.OUTPUTS_X, y: outputY + outputGap * 3 } },
      'risk-dv01': { type: 'risk', position: { x: LAYOUT.OUTPUTS_X, y: outputY + outputGap * 4.2 } },
      'risk-concentration': { type: 'risk', position: { x: LAYOUT.OUTPUTS_X, y: outputY + outputGap * 5.2 } },
    } as const;
  }, []); // Empty deps - positions never change

  // Dynamic node data - this changes with props but doesn't include positions
  const nodeDataMap = useMemo(() => ({
    'src-rfq': { 
      label: 'RFQ Stream', 
      metrics: rfqMetrics, 
      icon: '📊',
      color: '#00a8ff',
      description: 'Quote requests from counterparties',
      sql: SQL_QUERIES['src-rfq'],
      onSelect: handleNodeSelect,
    },
    'src-position': { 
      label: 'Position Stream', 
      metrics: positionMetrics, 
      icon: '💼',
      color: '#ffd000',
      description: 'Real-time position & P&L updates',
      sql: SQL_QUERIES['src-position'],
      onSelect: handleNodeSelect,
    },
    'src-signal': { 
      label: 'Signal Stream', 
      metrics: signalMetrics, 
      icon: '📡',
      color: '#ff6b6b',
      description: 'ML model predictions & signals',
      sql: SQL_QUERIES['src-signal'],
      onSelect: handleNodeSelect,
    },
    'join-rfq-signal': {
      label: 'RFQ × Signal',
      metrics: rfqSignalJoin,
      leftLabel: 'RFQ',
      rightLabel: 'Signal',
      joinKey: 'issuer',
      traderInsight: 'Match RFQs with model signals',
      sql: SQL_QUERIES['join-rfq-signal'],
      onSelect: handleNodeSelect,
    },
    'join-pos-signal': {
      label: 'Position × Signal',
      metrics: posSignalJoin,
      leftLabel: 'Position',
      rightLabel: 'Signal',
      joinKey: 'issuer',
      traderInsight: 'Track signal effectiveness',
      sql: SQL_QUERIES['join-pos-signal'],
      onSelect: handleNodeSelect,
    },
    'join-pos-rfq': {
      label: 'Position × RFQ',
      metrics: posRfqJoin,
      leftLabel: 'Position',
      rightLabel: 'RFQ',
      joinKey: 'bondId',
      traderInsight: 'Execution quality analysis',
      sql: SQL_QUERIES['join-pos-rfq'],
      onSelect: handleNodeSelect,
    },
    'agg-sector': {
      label: 'By Sector',
      metrics: sectorAgg,
      groupBy: 'sector',
      color: '#00d4aa',
      outputMetric: 'P&L, DV01',
      sql: SQL_QUERIES['agg-sector'],
      onSelect: handleNodeSelect,
    },
    'agg-desk': {
      label: 'By Desk',
      metrics: deskAgg,
      groupBy: 'desk',
      color: '#00a8ff',
      outputMetric: 'Position, Trades',
      sql: SQL_QUERIES['agg-desk'],
      onSelect: handleNodeSelect,
    },
    'agg-trader': {
      label: 'By Trader',
      metrics: traderAgg,
      groupBy: 'trader, desk',
      color: '#ffd000',
      outputMetric: 'P&L Attribution',
      sql: SQL_QUERIES['agg-trader'],
      onSelect: handleNodeSelect,
    },
    'agg-rating': {
      label: 'By Rating',
      metrics: ratingAgg,
      groupBy: 'rating',
      color: '#9c6ade',
      outputMetric: 'Credit Risk',
      sql: SQL_QUERIES['agg-rating'],
      onSelect: handleNodeSelect,
    },
    'agg-tenor': {
      label: 'By Tenor',
      metrics: tenorAgg,
      groupBy: 'tenor',
      color: '#ff8800',
      outputMetric: 'Duration Bucket',
      sql: SQL_QUERIES['agg-tenor'],
      onSelect: handleNodeSelect,
    },
    'agg-cp': {
      label: 'By Counterparty',
      metrics: counterpartyAgg,
      groupBy: 'counterparty',
      color: '#00a8ff',
      outputMetric: 'RFQ Flow',
      sql: SQL_QUERIES['agg-cp'],
      onSelect: handleNodeSelect,
    },
    'agg-model': {
      label: 'By Model',
      metrics: modelAgg,
      groupBy: 'model',
      color: '#ff6b6b',
      outputMetric: 'Signal Stats',
      sql: SQL_QUERIES['agg-model'],
      onSelect: handleNodeSelect,
    },
    'out-pnl': {
      label: 'Total P&L',
      value: formatCurrency(totalPnL),
      subValue: `${sectorAgg.groups} sectors contributing`,
      color: totalPnL >= 0 ? '#00d4aa' : '#ff6b6b',
      icon: '💰',
      trend: totalPnL > 0 ? 'up' : totalPnL < 0 ? 'down' : 'flat',
      significance: totalPnL > 1e6 ? 'Material P&L impact' : 'Within normal range',
      sql: SQL_QUERIES['out-pnl'],
      onSelect: handleNodeSelect,
    },
    'out-hitrate': {
      label: 'Hit Rate',
      value: `${(hitRate * 100).toFixed(1)}%`,
      subValue: `${formatNumber(rfqMetrics.count)} total RFQs`,
      color: hitRate > 0.5 ? '#00d4aa' : hitRate > 0.3 ? '#ffd000' : '#ff6b6b',
      icon: '🎯',
      significance: hitRate > 0.5 ? 'Strong execution' : 'Review pricing',
      sql: SQL_QUERIES['out-hitrate'],
      onSelect: handleNodeSelect,
    },
    'out-aligned': {
      label: 'Signal Aligned',
      value: formatNumber(signalAlignedCount),
      subValue: `of ${formatNumber(rfqSignalJoin.resultCount)} matched`,
      color: '#00a8ff',
      icon: '📈',
      significance: 'Trades matching model direction',
      sql: SQL_QUERIES['out-aligned'],
      onSelect: handleNodeSelect,
    },
    'out-execution': {
      label: 'Execution Quality',
      value: formatNumber(posRfqJoin.resultCount),
      subValue: 'position-RFQ matches',
      color: '#9c6ade',
      icon: '⚡',
      significance: 'Cross-reference analysis',
      sql: SQL_QUERIES['out-execution'],
      onSelect: handleNodeSelect,
    },
    'risk-dv01': {
      label: 'DV01 Exposure',
      value: `$${formatNumber(totalDV01)}`,
      status: totalDV01 > 1e6 ? 'danger' : totalDV01 > 5e5 ? 'caution' : 'safe' as const,
      icon: '📐',
      details: 'Interest rate sensitivity',
      sql: SQL_QUERIES['risk-dv01'],
      onSelect: handleNodeSelect,
    },
    'risk-concentration': {
      label: 'Concentration',
      value: `HHI ${concentrationHHI.toFixed(0)}`,
      status: concentrationHHI > 2500 ? 'danger' : concentrationHHI > 1500 ? 'caution' : 'safe' as const,
      icon: '🎲',
      details: concentrationHHI > 2500 ? 'Highly concentrated' : 'Diversified',
      sql: SQL_QUERIES['risk-concentration'],
      onSelect: handleNodeSelect,
    },
  }), [
    rfqMetrics, positionMetrics, signalMetrics,
    rfqSignalJoin, posSignalJoin, posRfqJoin,
    sectorAgg, deskAgg, traderAgg, ratingAgg, tenorAgg, counterpartyAgg, modelAgg,
    totalPnL, hitRate, signalAlignedCount, totalDV01, concentrationHHI,
    handleNodeSelect,
  ]);

  // Initial nodes - only computed once on mount
  // We use a ref to capture the initial nodeDataMap and never recompute
  const initialNodeDataRef = useRef(nodeDataMap);
  const initialNodes: Node[] = useMemo(() => {
    return Object.entries(staticNodeConfig).map(([id, config]) => ({
      id,
      type: config.type,
      position: config.position,
      data: { ...initialNodeDataRef.current[id as keyof typeof initialNodeDataRef.current], layoutDirection: 'TB' as const },
    }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [staticNodeConfig]); // Intentionally exclude nodeDataMap - we only want initial values

  // ============ SOURCE COLORS (unique per root source) ============
  const SOURCE_COLORS = {
    rfq: '#00a8ff',      // Blue - RFQ stream
    signal: '#ff6b6b',   // Coral - Signal stream
    position: '#ffd000', // Gold - Position stream
  };

  // Node lineage tracking - which sources feed into each node
  const NODE_LINEAGE: Record<string, string[]> = {
    'src-rfq': ['rfq'],
    'src-signal': ['signal'],
    'src-position': ['position'],
    'join-rfq-signal': ['rfq', 'signal'],
    'join-pos-signal': ['position', 'signal'],
    'join-pos-rfq': ['position', 'rfq'],
    'agg-sector': ['position'],
    'agg-desk': ['position'],
    'agg-trader': ['position'],
    'agg-rating': ['position'],
    'agg-tenor': ['position'],
    'agg-cp': ['rfq'],
    'agg-model': ['signal'],
    'out-pnl': ['position'],
    'out-hitrate': ['rfq'],
    'out-aligned': ['rfq', 'signal'],
    'out-execution': ['position', 'rfq'],
    'risk-dv01': ['position'],
    'risk-concentration': ['position'],
  };

  // Get edge color based on source node's lineage
  const getEdgeColor = (sourceNode: string): string => {
    const lineage = NODE_LINEAGE[sourceNode] || [];
    if (lineage.length === 1) {
      return SOURCE_COLORS[lineage[0] as keyof typeof SOURCE_COLORS] || '#4a5568';
    }
    // For multi-source (joins), return gradient ID reference
    return `url(#gradient-${lineage.sort().join('-')})`;
  };

  // Get marker color (solid color for arrow, use first lineage color for gradients)
  const getMarkerColor = (sourceNode: string): string => {
    const lineage = NODE_LINEAGE[sourceNode] || [];
    if (lineage.length >= 1) {
      // Use the first source color for the marker
      return SOURCE_COLORS[lineage[0] as keyof typeof SOURCE_COLORS] || '#4a5568';
    }
    return '#4a5568';
  };

  // Build edges with animation and throughput labels
  const initialEdges: Edge[] = useMemo(() => {
    const createEdge = (
      id: string, 
      source: string, 
      target: string, 
      targetHandle?: string,
      label?: string,
      throughput?: number,
    ): Edge => {
      const edgeColor = getEdgeColor(source);
      const markerColor = getMarkerColor(source);
      const lineage = NODE_LINEAGE[source] || [];
      const isMultiSource = lineage.length > 1;
      
      return {
        id,
        source,
        target,
        targetHandle,
        animated: true,
        label: throughput ? `${formatNumber(throughput)}/s` : label,
        labelStyle: { 
          fill: '#8b9cb8', 
          fontSize: 9, 
          fontFamily: 'JetBrains Mono',
          fontWeight: 500,
        },
        labelBgStyle: { fill: '#1a1f2e', fillOpacity: 0.9 },
        labelBgPadding: [4, 2] as [number, number],
        style: { 
          stroke: edgeColor, 
          strokeWidth: throughput ? Math.min(4, 1.5 + throughput / 50) : isMultiSource ? 3 : 2,
        },
        markerEnd: { type: MarkerType.ArrowClosed, color: markerColor, width: 12, height: 12 },
        className: isMultiSource ? 'multi-source-edge' : 'single-source-edge',
      };
    };

    return [
      // Sources → Joins (each source brings its own color)
      createEdge('e1', 'src-rfq', 'join-rfq-signal', 'left', 'RFQ Δ', rfqMetrics.rate),
      createEdge('e2', 'src-signal', 'join-rfq-signal', 'right', 'Signal Δ', signalMetrics.rate),
      createEdge('e3', 'src-position', 'join-pos-signal', 'left', 'Pos Δ', positionMetrics.rate),
      createEdge('e4', 'src-signal', 'join-pos-signal', 'right', 'Signal Δ'),
      createEdge('e5', 'src-position', 'join-pos-rfq', 'left', 'Pos Δ'),
      createEdge('e6', 'src-rfq', 'join-pos-rfq', 'right', 'RFQ Δ'),
      
      // Sources → Aggregations (inherit source color)
      createEdge('e7', 'src-position', 'agg-sector'),
      createEdge('e8', 'src-position', 'agg-desk'),
      createEdge('e9', 'src-position', 'agg-trader'),
      createEdge('e10', 'src-position', 'agg-rating'),
      createEdge('e11', 'src-position', 'agg-tenor'),
      createEdge('e12', 'src-rfq', 'agg-cp'),
      createEdge('e13', 'src-signal', 'agg-model'),
      
      // Aggregations → Outputs (inherit lineage color)
      createEdge('e14', 'agg-sector', 'out-pnl'),
      createEdge('e15', 'agg-cp', 'out-hitrate'),
      createEdge('e16', 'join-rfq-signal', 'out-aligned'),  // Gradient: rfq + signal
      createEdge('e17', 'join-pos-rfq', 'out-execution'),   // Gradient: position + rfq
      
      // To Risk nodes (inherit lineage)
      createEdge('e18', 'agg-tenor', 'risk-dv01'),
      createEdge('e19', 'agg-sector', 'risk-concentration'),
    ];
    // Only depend on rates for edge label updates, not constantly
  }, [rfqMetrics.rate, positionMetrics.rate, signalMetrics.rate]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // Apply auto-layout on initial mount with default 'TB' direction
  const hasAppliedInitialLayout = useRef(false);
  useEffect(() => {
    if (!hasAppliedInitialLayout.current && nodes.length > 0) {
      hasAppliedInitialLayout.current = true;
      // Apply vertical auto-layout after a small delay for the DOM to be ready
      setTimeout(() => {
        const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
          nodes,
          edges,
          'TB'
        );
        const updatedNodes = layoutedNodes.map(node => ({
          ...node,
          data: { ...node.data, layoutDirection: 'TB' as const }
        }));
        setNodes(updatedNodes);
        setEdges([...layoutedEdges]);
        setTimeout(() => {
          updatedNodes.forEach(node => updateNodeInternals(node.id));
          fitView({ padding: 0.1, duration: 300 });
        }, 50);
      }, 100);
    }
  }, [nodes.length]); // eslint-disable-line react-hooks/exhaustive-deps

  // Track render count to skip initial render updates
  const renderCountRef = useRef(0);

  // Update only node DATA when props change - preserves positions!
  // This is the key fix: we update data in-place instead of replacing nodes
  useEffect(() => {
    // Skip the first 2 renders (initial mount + first effect cycle)
    renderCountRef.current++;
    if (renderCountRef.current <= 2) {
      return;
    }

    // Update only the data property of each node, preserving position
    setNodes(currentNodes => 
      currentNodes.map(node => {
        const newData = nodeDataMap[node.id as keyof typeof nodeDataMap];
        if (newData) {
          return { ...node, data: { ...newData, layoutDirection } };
        }
        return node;
      })
    );
  }, [nodeDataMap, setNodes, layoutDirection]);

  // Edges: only update labels based on rate changes
  // Track previous rates to avoid unnecessary updates
  const prevRatesRef = useRef({ rfq: 0, pos: 0, sig: 0 });
  
  useEffect(() => {
    const { rfq, pos, sig } = prevRatesRef.current;
    const ratesChanged = 
      rfqMetrics.rate !== rfq || 
      positionMetrics.rate !== pos || 
      signalMetrics.rate !== sig;
    
    if (!ratesChanged) return;
    
    prevRatesRef.current = { 
      rfq: rfqMetrics.rate, 
      pos: positionMetrics.rate, 
      sig: signalMetrics.rate 
    };
    
    setEdges(currentEdges => 
      currentEdges.map(edge => {
        const initialEdge = initialEdges.find(e => e.id === edge.id);
        if (initialEdge) {
          return { 
            ...edge, 
            label: initialEdge.label,
            style: initialEdge.style,
          };
        }
        return edge;
      })
    );
  }, [initialEdges, setEdges, rfqMetrics.rate, positionMetrics.rate, signalMetrics.rate]);

  // Node click is handled by the node's internal onClick handler
  // This callback is just for ReactFlow's API - we don't need to do anything here
  const onNodeClick = useCallback((_: React.MouseEvent, _node: Node) => {
    // Handled by node's onClick
  }, []);

  // Auto-layout handler - applies dagre layout algorithm
  const onAutoLayout = useCallback((direction: 'TB' | 'LR' = 'LR') => {
    const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
      nodes,
      edges,
      direction
    );
    setLayoutDirection(direction);
    // Update nodes with new positions AND new layoutDirection in data
    const updatedNodes = layoutedNodes.map(node => ({
      ...node,
      data: { ...node.data, layoutDirection: direction }
    }));
    setNodes(updatedNodes);
    setEdges([...layoutedEdges]);
    
    // Force React Flow to update node internals (handle positions) after state updates
    setTimeout(() => {
      updatedNodes.forEach(node => updateNodeInternals(node.id));
      fitView({ padding: 0.1, duration: 300 });
    }, 50);
  }, [nodes, edges, setNodes, setEdges, fitView, updateNodeInternals]);

  // Reset to original layout
  const onResetLayout = useCallback(() => {
    setLayoutDirection('LR'); // Reset to default horizontal layout
    setNodes(currentNodes => {
      const updatedNodes = currentNodes.map(node => {
        const config = staticNodeConfig[node.id as keyof typeof staticNodeConfig];
        if (config) {
          return { 
            ...node, 
            position: config.position,
            data: { ...node.data, layoutDirection: 'LR' as const }
          };
        }
        return { ...node, data: { ...node.data, layoutDirection: 'LR' as const } };
      });
      // Force React Flow to update node internals (handle positions) after state updates
      setTimeout(() => {
        updatedNodes.forEach(node => updateNodeInternals(node.id));
        fitView({ padding: 0.1, duration: 300 });
      }, 50);
      return updatedNodes;
    });
  }, [setNodes, staticNodeConfig, fitView, updateNodeInternals]);

  // Memoize minimap color function to prevent re-renders
  const minimapNodeColor = useCallback((node: Node) => {
    switch (node.type) {
      case 'source': return '#00a8ff';
      case 'join': return '#ffd000';
      case 'agg': return '#00d4aa';
      case 'output': return '#9c6ade';
      case 'risk': return '#ff6b6b';
      default: return '#4a5568';
    }
  }, []);

  return (
    <div className={`stream-flow-container enhanced ${zenMode ? 'zen-mode' : ''}`}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={onNodeClick}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.1, maxZoom: 0.9 }}
        minZoom={0.15}
        maxZoom={1.5}
        defaultViewport={{ x: 0, y: 0, zoom: 0.55 }}
        proOptions={{ hideAttribution: true }}
      >
        {/* SVG Gradient Definitions for multi-source edges */}
        <svg style={{ position: 'absolute', width: 0, height: 0 }}>
          <defs>
            {/* Position + RFQ gradient (gold → blue) */}
            <linearGradient id="gradient-position-rfq" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor={SOURCE_COLORS.position} />
              <stop offset="35%" stopColor={SOURCE_COLORS.position} />
              <stop offset="50%" stopColor="#88c4e0" />
              <stop offset="65%" stopColor={SOURCE_COLORS.rfq} />
              <stop offset="100%" stopColor={SOURCE_COLORS.rfq} />
            </linearGradient>
            {/* RFQ + Signal gradient (blue → coral) */}
            <linearGradient id="gradient-rfq-signal" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor={SOURCE_COLORS.rfq} />
              <stop offset="35%" stopColor={SOURCE_COLORS.rfq} />
              <stop offset="50%" stopColor="#b089d8" />
              <stop offset="65%" stopColor={SOURCE_COLORS.signal} />
              <stop offset="100%" stopColor={SOURCE_COLORS.signal} />
            </linearGradient>
            {/* Position + Signal gradient (gold → coral) */}
            <linearGradient id="gradient-position-signal" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor={SOURCE_COLORS.position} />
              <stop offset="35%" stopColor={SOURCE_COLORS.position} />
              <stop offset="50%" stopColor="#ffb366" />
              <stop offset="65%" stopColor={SOURCE_COLORS.signal} />
              <stop offset="100%" stopColor={SOURCE_COLORS.signal} />
            </linearGradient>
            {/* Animated flowing gradient - used for edge animations */}
            <linearGradient id="flow-gradient" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor="rgba(255,255,255,0)" />
              <stop offset="40%" stopColor="rgba(255,255,255,0.3)" />
              <stop offset="50%" stopColor="rgba(255,255,255,0.6)" />
              <stop offset="60%" stopColor="rgba(255,255,255,0.3)" />
              <stop offset="100%" stopColor="rgba(255,255,255,0)" />
            </linearGradient>
          </defs>
        </svg>
        
        <Background 
          color="#2d3748" 
          gap={24} 
          size={1}
          variant={BackgroundVariant.Dots}
        />
        <Controls 
          showInteractive={false}
          position="bottom-left"
        />
        
        {showMinimap && (
          <MiniMap 
            position="bottom-right"
            nodeColor={minimapNodeColor}
            maskColor="rgba(10, 14, 20, 0.8)"
            style={{ background: '#141a22' }}
          />
        )}
        
        {/* Pipeline Health Panel - hidden in zen mode */}
        {!zenMode && (
          <Panel position="top-left">
            <PipelineHealthPanel 
              avgLatency={pipelineHealth.avgLatency}
              maxLatency={pipelineHealth.maxLatency}
              totalEvents={pipelineHealth.totalEvents}
              totalJoins={pipelineHealth.totalJoins}
              lagging={pipelineHealth.lagging}
              grade={pipelineHealth.grade}
            />
          </Panel>
        )}

        {/* Data Flow Panel - hidden in zen mode */}
        {!zenMode && (
          <Panel position="top-right" className="data-flow-panel-container">
            <DataFlowPanel 
              rfqCount={rfqMetrics.count}
              posCount={positionMetrics.count}
              signalCount={signalMetrics.count}
              joinCount={rfqSignalJoin.resultCount + posSignalJoin.resultCount + posRfqJoin.resultCount}
              rate={rfqMetrics.rate}
            />
          </Panel>
        )}
        
        {/* Control Panel - always visible but simplified in zen mode */}
        <Panel position="bottom-left" className={`legend-panel enhanced ${zenMode ? 'zen-mode' : ''}`} style={{ marginBottom: '60px', marginLeft: '50px' }}>
          <div className="legend-header">
            <span className="legend-title">{zenMode ? '🧘 Zen' : 'Pipeline Legend'}</span>
            <div className="legend-controls">
              <button 
                className={`zen-toggle ${zenMode ? 'active' : ''}`}
                onClick={() => setZenMode(!zenMode)}
                title={zenMode ? 'Exit zen mode' : 'Enter zen mode'}
              >
                {zenMode ? '🔍 Detail' : '🧘 Zen'}
              </button>
              {!zenMode && (
                <>
                  <button 
                    className="layout-btn"
                    onClick={() => onAutoLayout('LR')}
                    title="Auto-layout horizontal"
                  >
                    ⇢ Auto
                  </button>
                  <button 
                    className="layout-btn"
                    onClick={() => onAutoLayout('TB')}
                    title="Auto-layout vertical"
                  >
                    ⇣ Auto
                  </button>
                  <button 
                    className="layout-btn"
                    onClick={onResetLayout}
                    title="Reset to default layout"
                  >
                    ↺ Reset
                  </button>
                  <button 
                    className="minimap-toggle"
                    onClick={() => setShowMinimap(!showMinimap)}
                  >
                    {showMinimap ? '🗺️' : '🗺️'}
                  </button>
                </>
              )}
            </div>
          </div>
          {!zenMode && (
            <>
              <div className="legend-section">
                <span className="legend-section-title">Data Sources</span>
                <div className="legend-items flow-colors">
                  <div className="legend-item">
                    <span className="legend-line" style={{ background: SOURCE_COLORS.rfq }} />
                    <span>RFQ Stream</span>
                  </div>
                  <div className="legend-item">
                    <span className="legend-line" style={{ background: SOURCE_COLORS.position }} />
                    <span>Position Stream</span>
                  </div>
                  <div className="legend-item">
                    <span className="legend-line" style={{ background: SOURCE_COLORS.signal }} />
                    <span>Signal Stream</span>
                  </div>
                  <div className="legend-item">
                    <span className="legend-line gradient" style={{ background: `linear-gradient(90deg, ${SOURCE_COLORS.rfq}, ${SOURCE_COLORS.signal})` }} />
                    <span>Joined (multi-source)</span>
                  </div>
                </div>
              </div>
              <div className="legend-section">
                <span className="legend-section-title">Node Types</span>
                <div className="legend-items">
                  <div className="legend-item">
                    <span className="legend-dot" style={{ background: '#00a8ff' }} />
                    <span>Source</span>
                  </div>
                  <div className="legend-item">
                    <span className="legend-dot" style={{ background: '#ffd000' }} />
                    <span>Join (⋈)</span>
                  </div>
                  <div className="legend-item">
                    <span className="legend-dot" style={{ background: '#00d4aa' }} />
                    <span>Aggregate (Σ)</span>
                  </div>
                  <div className="legend-item">
                    <span className="legend-dot" style={{ background: '#9c6ade' }} />
                    <span>Output</span>
                  </div>
                  <div className="legend-item">
                    <span className="legend-dot" style={{ background: '#ff6b6b' }} />
                    <span>Risk</span>
                  </div>
                </div>
              </div>
              <div className="legend-formulas">
                <code>Δ(L⋈R) = ΔL⋈R + L⋈ΔR</code>
                <code>Δ(SUM) = SUM(Δ)</code>
              </div>
              <div className="legend-tip">
                💡 Edge colors trace data lineage from sources
              </div>
            </>
          )}
        </Panel>
      </ReactFlow>

      {/* Details Panel */}
      {selectedNode && (
        <DetailsPanel 
          node={selectedNode} 
          onClose={() => setSelectedNode(null)} 
        />
      )}
    </div>
  );
}

// Wrapper component that provides ReactFlowProvider context
export function StreamFlowVisualization(props: StreamFlowProps) {
  return (
    <ReactFlowProvider>
      <StreamFlowVisualizationInner {...props} />
    </ReactFlowProvider>
  );
}

export default StreamFlowVisualization;
