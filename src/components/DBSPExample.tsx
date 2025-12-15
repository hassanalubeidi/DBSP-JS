/**
 * DBSP React Hook Example Component
 * 
 * Demonstrates real-time filtering and analytics with useDBSP
 */

import React, { useState } from 'react';
import { useDBSP } from '../dbsp/useDBSP';

// ============ TYPES ============

interface Order {
  orderId: number;
  customerId: number;
  product: string;
  amount: number;
  status: 'pending' | 'processing' | 'shipped' | 'delivered' | 'cancelled';
  region: 'NA' | 'EU' | 'APAC' | 'LATAM';
}

// ============ SAMPLE DATA ============

const initialOrders: Order[] = [
  { orderId: 1, customerId: 100, product: 'Laptop', amount: 1200, status: 'pending', region: 'NA' },
  { orderId: 2, customerId: 101, product: 'Mouse', amount: 25, status: 'shipped', region: 'EU' },
  { orderId: 3, customerId: 100, product: 'Keyboard', amount: 75, status: 'pending', region: 'NA' },
  { orderId: 4, customerId: 102, product: 'Monitor', amount: 400, status: 'delivered', region: 'APAC' },
  { orderId: 5, customerId: 103, product: 'Webcam', amount: 80, status: 'processing', region: 'EU' },
  { orderId: 6, customerId: 101, product: 'Headphones', amount: 150, status: 'pending', region: 'LATAM' },
  { orderId: 7, customerId: 104, product: 'USB Hub', amount: 35, status: 'cancelled', region: 'NA' },
  { orderId: 8, customerId: 100, product: 'SSD', amount: 200, status: 'shipped', region: 'APAC' },
];

// ============ COMPONENT ============

export function DBSPExample() {
  // Filter state
  const [statusFilter, setStatusFilter] = useState<string>('all');
  const [minAmount, setMinAmount] = useState<number>(0);
  
  // Build SQL dynamically
  const sql = React.useMemo(() => {
    const conditions: string[] = [];
    
    if (statusFilter !== 'all') {
      conditions.push(`status = '${statusFilter}'`);
    }
    if (minAmount > 0) {
      conditions.push(`amount >= ${minAmount}`);
    }
    
    const where = conditions.length > 0 ? ` WHERE ${conditions.join(' AND ')}` : '';
    return `SELECT * FROM orders${where}`;
  }, [statusFilter, minAmount]);
  
  // DBSP hook with SQL transformation
  const {
    data: filteredOrders,
    rawData: allOrders,
    count,
    insert,
    upsert,
    remove,
    removeWhere,
    update,
    stats,
  } = useDBSP({
    tableName: 'orders',
    initialData: initialOrders,
    sql,
    primaryKey: ['orderId'],
  });
  
  // Form state for adding orders
  const [newOrder, setNewOrder] = useState<Partial<Order>>({
    product: '',
    amount: 0,
    status: 'pending',
    region: 'NA',
  });
  
  // Calculate aggregates
  const totalRevenue = filteredOrders.reduce((sum, o) => sum + o.amount, 0);
  const avgOrderValue = filteredOrders.length > 0 ? totalRevenue / filteredOrders.length : 0;
  
  // Handlers
  const handleAddOrder = () => {
    if (!newOrder.product) return;
    
    const nextId = Math.max(...allOrders.map(o => o.orderId), 0) + 1;
    insert({
      orderId: nextId,
      customerId: Math.floor(Math.random() * 100) + 100,
      product: newOrder.product!,
      amount: newOrder.amount || 0,
      status: newOrder.status as Order['status'],
      region: newOrder.region as Order['region'],
    });
    
    setNewOrder({ product: '', amount: 0, status: 'pending', region: 'NA' });
  };
  
  const handleStatusChange = (orderId: number, status: Order['status']) => {
    update({ orderId }, { status });
  };
  
  const handleDelete = (orderId: number) => {
    remove({ orderId });
  };
  
  const handleCancelAll = () => {
    removeWhere(o => o.status === 'cancelled');
  };
  
  return (
    <div style={{ 
      fontFamily: 'system-ui, -apple-system, sans-serif',
      maxWidth: '1200px',
      margin: '0 auto',
      padding: '20px',
    }}>
      <h1 style={{ marginBottom: '8px' }}>🚀 DBSP React Hook Demo</h1>
      <p style={{ color: '#666', marginBottom: '24px' }}>
        Real-time incremental data processing with SQL transformations
      </p>
      
      {/* Stats Banner */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(4, 1fr)',
        gap: '16px',
        marginBottom: '24px',
      }}>
        <StatCard label="Total Orders" value={count} />
        <StatCard label="Filtered" value={filteredOrders.length} />
        <StatCard label="Revenue" value={`$${totalRevenue.toLocaleString()}`} />
        <StatCard label="Avg Order" value={`$${avgOrderValue.toFixed(2)}`} />
      </div>
      
      {/* Filters */}
      <div style={{
        background: '#f5f5f5',
        padding: '16px',
        borderRadius: '8px',
        marginBottom: '24px',
      }}>
        <h3 style={{ margin: '0 0 12px 0' }}>📊 SQL Filter (Incremental)</h3>
        <code style={{ 
          display: 'block', 
          background: '#1e1e1e', 
          color: '#9cdcfe',
          padding: '12px',
          borderRadius: '4px',
          marginBottom: '12px',
          fontSize: '14px',
        }}>
          {sql}
        </code>
        
        <div style={{ display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
          <div>
            <label style={{ display: 'block', marginBottom: '4px', fontWeight: 500 }}>
              Status
            </label>
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              style={{ padding: '8px 12px', borderRadius: '4px', border: '1px solid #ddd' }}
            >
              <option value="all">All Statuses</option>
              <option value="pending">Pending</option>
              <option value="processing">Processing</option>
              <option value="shipped">Shipped</option>
              <option value="delivered">Delivered</option>
              <option value="cancelled">Cancelled</option>
            </select>
          </div>
          
          <div>
            <label style={{ display: 'block', marginBottom: '4px', fontWeight: 500 }}>
              Min Amount
            </label>
            <input
              type="number"
              value={minAmount}
              onChange={(e) => setMinAmount(Number(e.target.value))}
              style={{ padding: '8px 12px', borderRadius: '4px', border: '1px solid #ddd', width: '120px' }}
            />
          </div>
          
          <div style={{ marginLeft: 'auto', alignSelf: 'flex-end' }}>
            <span style={{ fontSize: '12px', color: '#666' }}>
              Last update: {stats.lastUpdateMs.toFixed(2)}ms
            </span>
          </div>
        </div>
      </div>
      
      {/* Add Order Form */}
      <div style={{
        background: '#e8f4fd',
        padding: '16px',
        borderRadius: '8px',
        marginBottom: '24px',
      }}>
        <h3 style={{ margin: '0 0 12px 0' }}>➕ Add New Order</h3>
        <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap' }}>
          <input
            type="text"
            placeholder="Product"
            value={newOrder.product}
            onChange={(e) => setNewOrder({ ...newOrder, product: e.target.value })}
            style={{ padding: '8px 12px', borderRadius: '4px', border: '1px solid #ddd', flex: 1 }}
          />
          <input
            type="number"
            placeholder="Amount"
            value={newOrder.amount || ''}
            onChange={(e) => setNewOrder({ ...newOrder, amount: Number(e.target.value) })}
            style={{ padding: '8px 12px', borderRadius: '4px', border: '1px solid #ddd', width: '100px' }}
          />
          <select
            value={newOrder.status}
            onChange={(e) => setNewOrder({ ...newOrder, status: e.target.value as Order['status'] })}
            style={{ padding: '8px 12px', borderRadius: '4px', border: '1px solid #ddd' }}
          >
            <option value="pending">Pending</option>
            <option value="processing">Processing</option>
            <option value="shipped">Shipped</option>
          </select>
          <select
            value={newOrder.region}
            onChange={(e) => setNewOrder({ ...newOrder, region: e.target.value as Order['region'] })}
            style={{ padding: '8px 12px', borderRadius: '4px', border: '1px solid #ddd' }}
          >
            <option value="NA">NA</option>
            <option value="EU">EU</option>
            <option value="APAC">APAC</option>
            <option value="LATAM">LATAM</option>
          </select>
          <button
            onClick={handleAddOrder}
            style={{
              padding: '8px 20px',
              background: '#0066cc',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
            }}
          >
            Add Order
          </button>
        </div>
      </div>
      
      {/* Actions */}
      <div style={{ marginBottom: '16px' }}>
        <button
          onClick={handleCancelAll}
          style={{
            padding: '8px 16px',
            background: '#dc3545',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
          }}
        >
          🗑️ Remove All Cancelled Orders
        </button>
      </div>
      
      {/* Orders Table */}
      <table style={{ 
        width: '100%', 
        borderCollapse: 'collapse',
        background: 'white',
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
        borderRadius: '8px',
        overflow: 'hidden',
      }}>
        <thead>
          <tr style={{ background: '#f8f9fa' }}>
            <th style={thStyle}>ID</th>
            <th style={thStyle}>Product</th>
            <th style={thStyle}>Amount</th>
            <th style={thStyle}>Status</th>
            <th style={thStyle}>Region</th>
            <th style={thStyle}>Actions</th>
          </tr>
        </thead>
        <tbody>
          {filteredOrders.map((order) => (
            <tr key={order.orderId} style={{ borderBottom: '1px solid #eee' }}>
              <td style={tdStyle}>{order.orderId}</td>
              <td style={tdStyle}>{order.product}</td>
              <td style={tdStyle}>${order.amount.toLocaleString()}</td>
              <td style={tdStyle}>
                <select
                  value={order.status}
                  onChange={(e) => handleStatusChange(order.orderId, e.target.value as Order['status'])}
                  style={{
                    padding: '4px 8px',
                    borderRadius: '4px',
                    border: '1px solid #ddd',
                    background: statusColors[order.status],
                  }}
                >
                  <option value="pending">Pending</option>
                  <option value="processing">Processing</option>
                  <option value="shipped">Shipped</option>
                  <option value="delivered">Delivered</option>
                  <option value="cancelled">Cancelled</option>
                </select>
              </td>
              <td style={tdStyle}>
                <span style={{
                  padding: '4px 8px',
                  background: regionColors[order.region],
                  borderRadius: '4px',
                  fontSize: '12px',
                }}>
                  {order.region}
                </span>
              </td>
              <td style={tdStyle}>
                <button
                  onClick={() => handleDelete(order.orderId)}
                  style={{
                    padding: '4px 12px',
                    background: '#ff6b6b',
                    color: 'white',
                    border: 'none',
                    borderRadius: '4px',
                    cursor: 'pointer',
                    fontSize: '12px',
                  }}
                >
                  Delete
                </button>
              </td>
            </tr>
          ))}
          {filteredOrders.length === 0 && (
            <tr>
              <td colSpan={6} style={{ ...tdStyle, textAlign: 'center', color: '#999' }}>
                No orders match the current filter
              </td>
            </tr>
          )}
        </tbody>
      </table>
      
      {/* Performance Note */}
      <p style={{ marginTop: '24px', color: '#666', fontSize: '14px' }}>
        💡 <strong>Note:</strong> All filtering is done incrementally using DBSP. 
        Only changed rows are processed, not the entire dataset!
      </p>
    </div>
  );
}

// ============ HELPER COMPONENTS ============

function StatCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div style={{
      background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
      color: 'white',
      padding: '16px',
      borderRadius: '8px',
      textAlign: 'center',
    }}>
      <div style={{ fontSize: '24px', fontWeight: 'bold' }}>{value}</div>
      <div style={{ fontSize: '12px', opacity: 0.9 }}>{label}</div>
    </div>
  );
}

// ============ STYLES ============

const thStyle: React.CSSProperties = {
  padding: '12px 16px',
  textAlign: 'left',
  fontWeight: 600,
  fontSize: '14px',
};

const tdStyle: React.CSSProperties = {
  padding: '12px 16px',
  fontSize: '14px',
};

const statusColors: Record<string, string> = {
  pending: '#fff3cd',
  processing: '#cce5ff',
  shipped: '#d4edda',
  delivered: '#c3e6cb',
  cancelled: '#f8d7da',
};

const regionColors: Record<string, string> = {
  NA: '#e3f2fd',
  EU: '#f3e5f5',
  APAC: '#e8f5e9',
  LATAM: '#fff8e1',
};

export default DBSPExample;

