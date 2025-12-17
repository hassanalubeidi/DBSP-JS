// ============ SHARED TYPES ============

export interface Order {
  orderId: number;
  customerId: number;
  productId: number;
  amount: number;
  quantity: number;
  status: string;
  region: string;
  category: string;
  [key: string]: unknown;
}

export interface Customer {
  customerId: number;
  name: string;
  tier: 'bronze' | 'silver' | 'gold' | 'platinum';
  country: string;
  memberSince: number;
  [key: string]: unknown;
}

export interface DeltaOp {
  op: 'insert' | 'update' | 'delete';
  row?: Order;
  orderId?: number;
}

export interface CustomerDeltaOp {
  op: 'insert' | 'update' | 'delete';
  row?: Customer;
  customerId?: number;
}

export interface ConnectionStats {
  state: 'disconnected' | 'connecting' | 'connected';
  snapshotLoaded: boolean;
  snapshotSize: number;
  customerSnapshotSize: number;
  snapshotTimeMs: number;
  totalDeltas: number;
  deltasPerSecond: number;
  lastDeltaBatchMs: number;
  totalRowsProcessed: number;
}

// Aggregation result types
export interface RegionalSummary {
  region: string;
  total_amount: number;
  order_count: number;
  avg_amount: number;
  [key: string]: unknown;
}

export interface StatusSummary {
  status: string;
  total_amount: number;
  order_count: number;
  avg_amount: number;
  [key: string]: unknown;
}

export interface CategorySummary {
  category: string;
  total_amount: number;
  order_count: number;
  avg_amount: number;
  total_quantity: number;
  [key: string]: unknown;
}

export interface WeightedAvgSummary {
  category: string;
  weighted_total: number;
  total_weight: number;
  [key: string]: unknown;
}

export interface CustomerSummary {
  customerId: number;
  total_spent: number;
  order_count: number;
  avg_order: number;
  total_quantity: number;
  [key: string]: unknown;
}

export interface TierRevenue {
  tier: string;
  total: number;
  count: number;
  orders: number;
  avg: number;
}

// Re-export credit types
export * from './credit';

