/**
 * WebSocket Server for DBSP Real-Time Stress Test
 * 
 * Streams order AND customer data with HIGHLY DYNAMIC changes:
 * - Orders with region, status, category, amounts
 * - Customers with tiers, countries - enables JOIN demonstrations
 * - Tracks both in memory for targeted updates
 * - Creates dramatic shifts in data distribution
 * 
 * Run with: node server.js
 */

import { WebSocketServer } from 'ws';

const PORT = 8765;
const ORDER_SNAPSHOT_SIZE = 100_000;
const CUSTOMER_SNAPSHOT_SIZE = 10_000; // 10K customers for 100K orders

// Order data options
const STATUSES = ['pending', 'processing', 'shipped', 'delivered', 'cancelled'];
const REGIONS = ['NA', 'EU', 'APAC', 'LATAM', 'MEA'];
const CATEGORIES = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Toys'];

// Customer data options
const TIERS = ['bronze', 'silver', 'gold', 'platinum'];
const COUNTRIES = ['USA', 'UK', 'Germany', 'Japan', 'Australia', 'Canada', 'France', 'Brazil'];
const FIRST_NAMES = ['Alice', 'Bob', 'Carol', 'David', 'Emma', 'Frank', 'Grace', 'Henry', 'Iris', 'Jack', 
                     'Kate', 'Leo', 'Maya', 'Noah', 'Olivia', 'Paul', 'Quinn', 'Rose', 'Sam', 'Tina'];
const LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 
                    'Rodriguez', 'Martinez', 'Anderson', 'Taylor', 'Thomas', 'Moore', 'Jackson'];

// Dynamic state - shifts dramatically every tick
let hotRegion = REGIONS[0];
let hotCategory = CATEGORIES[0];
let hotCustomerRange = [1, 100]; // Customer IDs that are "active"
let amountMultiplier = 1.0;
let tick = 0;

// Rotate patterns more frequently for visible changes
setInterval(() => {
  tick++;
  
  // Rotate hot region every 3 seconds
  if (tick % 3 === 0) {
    const oldRegion = hotRegion;
    hotRegion = REGIONS[(REGIONS.indexOf(hotRegion) + 1) % REGIONS.length];
    console.log(`[Server] Region shift: ${oldRegion} → ${hotRegion}`);
  }
  
  // Rotate hot category every 4 seconds
  if (tick % 4 === 0) {
    const oldCategory = hotCategory;
    hotCategory = CATEGORIES[(CATEGORIES.indexOf(hotCategory) + 1) % CATEGORIES.length];
    console.log(`[Server] Category shift: ${oldCategory} → ${hotCategory}`);
  }
  
  // Shift active customer range every 5 seconds (affects top customers)
  if (tick % 5 === 0) {
    const start = Math.floor(Math.random() * 9900) + 1;
    hotCustomerRange = [start, start + 100];
    console.log(`[Server] Active customers: ${start}-${start + 100}`);
  }
  
  // Dramatic amount fluctuation
  amountMultiplier = 0.3 + Math.abs(Math.sin(tick * 0.5)) * 1.7 + Math.random() * 0.5;
}, 1000);

function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

// ============ CUSTOMER GENERATION ============

function generateCustomer(customerId) {
  const firstName = randomChoice(FIRST_NAMES);
  const lastName = randomChoice(LAST_NAMES);
  
  // Higher IDs = newer customers = more likely bronze
  const tierWeights = customerId < 2000 
    ? [0.1, 0.2, 0.4, 0.3]   // Old customers: more gold/platinum
    : [0.5, 0.3, 0.15, 0.05]; // New customers: more bronze
  
  const tierRoll = Math.random();
  let tier = 'bronze';
  let cumulative = 0;
  for (let i = 0; i < TIERS.length; i++) {
    cumulative += tierWeights[i];
    if (tierRoll < cumulative) {
      tier = TIERS[i];
      break;
    }
  }
  
  return {
    customerId,
    name: `${firstName} ${lastName}`,
    tier,
    country: randomChoice(COUNTRIES),
    // Simulate join date (days ago)
    memberSince: Math.floor(Math.random() * 1000) + 1,
  };
}

function generateCustomerSnapshot() {
  console.log(`[Server] Generating ${CUSTOMER_SNAPSHOT_SIZE.toLocaleString()} customers...`);
  const start = Date.now();
  const customers = [];
  
  for (let i = 1; i <= CUSTOMER_SNAPSHOT_SIZE; i++) {
    customers.push(generateCustomer(i));
  }
  
  console.log(`[Server] Generated customers in ${Date.now() - start}ms`);
  return customers;
}

// Realistic status flow: pending → processing → shipped → delivered
// Orders can be cancelled at pending or processing stage
const STATUS_FLOW = {
  'pending': { next: 'processing', cancelChance: 0.05 },
  'processing': { next: 'shipped', cancelChance: 0.02 },
  'shipped': { next: 'delivered', cancelChance: 0 },
  'delivered': { next: null, cancelChance: 0 },
  'cancelled': { next: null, cancelChance: 0 },
};

function getNextStatus(currentStatus) {
  const flow = STATUS_FLOW[currentStatus];
  if (!flow || !flow.next) return currentStatus; // Terminal state
  
  // Small chance of cancellation
  if (flow.cancelChance > 0 && Math.random() < flow.cancelChance) {
    return 'cancelled';
  }
  
  return flow.next;
}

function generateOrder(id, options = {}) {
  // Use hot customer range for 60% of orders
  const useHotCustomer = Math.random() < 0.6;
  const customerId = useHotCustomer
    ? Math.floor(Math.random() * (hotCustomerRange[1] - hotCustomerRange[0])) + hotCustomerRange[0]
    : Math.floor(Math.random() * 10000) + 1;
  
  // Vary amounts dramatically
  const isHighValue = Math.random() < 0.2;
  const baseAmount = isHighValue 
    ? Math.random() * 500 + 300
    : Math.random() * 150 + 20;
  
  // Vary quantities for weighted average impact
  const quantity = isHighValue
    ? Math.floor(Math.random() * 15) + 5  // High value: 5-20 qty
    : Math.floor(Math.random() * 5) + 1;   // Normal: 1-5 qty
  
  return {
    orderId: id,
    customerId: options.customerId ?? customerId,
    productId: Math.floor(Math.random() * 5000) + 1,
    amount: Math.round(baseAmount * amountMultiplier * 100) / 100,
    quantity: options.quantity ?? quantity,
    // NEW orders always start as 'pending'
    status: options.status ?? 'pending',
    region: options.region ?? (Math.random() < 0.5 ? hotRegion : randomChoice(REGIONS)),
    category: options.category ?? (Math.random() < 0.5 ? hotCategory : randomChoice(CATEGORIES)),
  };
}

function generateOrderSnapshot() {
  console.log(`[Server] Generating ${ORDER_SNAPSHOT_SIZE.toLocaleString()} orders...`);
  const start = Date.now();
  const orders = [];
  
  // Realistic initial distribution of order statuses:
  // 25% pending, 20% processing, 25% shipped, 25% delivered, 5% cancelled
  const statusDistribution = [
    { status: 'pending', weight: 0.25 },
    { status: 'processing', weight: 0.45 },
    { status: 'shipped', weight: 0.70 },
    { status: 'delivered', weight: 0.95 },
    { status: 'cancelled', weight: 1.0 },
  ];
  
  function pickStatus() {
    const r = Math.random();
    for (const { status, weight } of statusDistribution) {
      if (r < weight) return status;
    }
    return 'pending';
  }
  
  for (let i = 0; i < ORDER_SNAPSHOT_SIZE; i++) {
    orders.push(generateOrder(i + 1, { status: pickStatus() }));
  }
  console.log(`[Server] Generated orders in ${Date.now() - start}ms`);
  return orders;
}

const wss = new WebSocketServer({ port: PORT });

console.log(`
╔════════════════════════════════════════════════════════════════╗
║           DBSP Real-Time Stress Test Server                    ║
║                 with JOIN Demonstration                        ║
╠════════════════════════════════════════════════════════════════╣
║  WebSocket: ws://localhost:${PORT}                               ║
║  Orders:    ${ORDER_SNAPSHOT_SIZE.toLocaleString().padStart(7)} rows                               ║
║  Customers: ${CUSTOMER_SNAPSHOT_SIZE.toLocaleString().padStart(7)} rows                               ║
║  Join:      Orders ⋈ Customers ON customerId                   ║
║  Dynamic:   Region, Status, Category, Tier, Amount             ║
╚════════════════════════════════════════════════════════════════╝
`);

wss.on('connection', (ws, req) => {
  const clientAddr = req.socket.remoteAddress;
  console.log(`\n[Server] Client connected from ${clientAddr}`);
  
  let deltaInterval = null;
  let orderId = ORDER_SNAPSHOT_SIZE + 1;
  let customerId = CUSTOMER_SNAPSHOT_SIZE + 1;
  let currentDeltaRate = 30;
  let currentBatchSize = 10;
  let isPaused = false;
  
  // Track existing entities for targeted updates
  const existingOrders = new Map();
  const existingCustomers = new Map();
  
  // ============ GENERATE SNAPSHOTS ============
  const orderSnapshot = generateOrderSnapshot();
  const customerSnapshot = generateCustomerSnapshot();
  
  // Store for updates
  for (const order of orderSnapshot) {
    existingOrders.set(order.orderId, order);
  }
  for (const customer of customerSnapshot) {
    existingCustomers.set(customer.customerId, customer);
  }
  
  console.log(`[Server] Sending snapshots: ${orderSnapshot.length.toLocaleString()} orders + ${customerSnapshot.length.toLocaleString()} customers...`);
  const snapshotStart = Date.now();
  
  // ============ SEND CUSTOMER SNAPSHOT FIRST (smaller, needed for joins) ============
  const CUSTOMER_CHUNK_SIZE = 2000;
  let customerChunkIndex = 0;
  const customerTotalChunks = Math.ceil(customerSnapshot.length / CUSTOMER_CHUNK_SIZE);
  
  for (let i = 0; i < customerSnapshot.length; i += CUSTOMER_CHUNK_SIZE) {
    const chunk = customerSnapshot.slice(i, i + CUSTOMER_CHUNK_SIZE);
    const isLast = i + CUSTOMER_CHUNK_SIZE >= customerSnapshot.length;
    
    try {
      ws.send(JSON.stringify({
        type: 'customer-snapshot-chunk',
        data: chunk,
        chunkIndex: customerChunkIndex++,
        totalChunks: customerTotalChunks,
        isLast,
      }));
    } catch (err) {
      console.error('[Server] Failed to send customer chunk:', err.message);
      return;
    }
  }
  
  console.log(`[Server] Customer snapshot sent in ${Date.now() - snapshotStart}ms`);
  
  // ============ SEND ORDER SNAPSHOT ============
  const ORDER_CHUNK_SIZE = 5000;
  let orderChunkIndex = 0;
  const orderTotalChunks = Math.ceil(orderSnapshot.length / ORDER_CHUNK_SIZE);
  
  for (let i = 0; i < orderSnapshot.length; i += ORDER_CHUNK_SIZE) {
    const chunk = orderSnapshot.slice(i, i + ORDER_CHUNK_SIZE);
    const isLast = i + ORDER_CHUNK_SIZE >= orderSnapshot.length;
    
    try {
      ws.send(JSON.stringify({
        type: 'snapshot-chunk',  // Keep existing type for backward compatibility
        data: chunk,
        chunkIndex: orderChunkIndex++,
        totalChunks: orderTotalChunks,
        isLast,
      }));
    } catch (err) {
      console.error('[Server] Failed to send order chunk:', err.message);
      return;
    }
  }
  
  console.log(`[Server] All snapshots sent in ${Date.now() - snapshotStart}ms`);
  
  function startDeltaStream() {
    if (deltaInterval) {
      clearInterval(deltaInterval);
    }
    
    if (isPaused) {
      console.log(`[Server] Delta stream paused`);
      return;
    }
    
    const intervalMs = Math.max(1, Math.floor(1000 / currentDeltaRate));
    
    deltaInterval = setInterval(() => {
      if (ws.readyState !== ws.OPEN || isPaused) {
        return;
      }
      
      const orderDeltas = [];
      const customerDeltas = [];
      const orderIds = Array.from(existingOrders.keys());
      const customerIds = Array.from(existingCustomers.keys());
      
      for (let i = 0; i < currentBatchSize; i++) {
        const entityType = Math.random();
        
        // ============ 85% ORDER UPDATES ============
        if (entityType < 0.85) {
          const action = Math.random();
          
          if (action < 0.30) {
            // 30% INSERTS - new orders with current hot patterns
            const newOrder = generateOrder(orderId++);
            existingOrders.set(newOrder.orderId, newOrder);
            orderDeltas.push({ op: 'insert', row: newOrder });
            
          } else if (action < 0.70) {
            // 40% UPDATES - modify existing orders dramatically
            if (orderIds.length === 0) continue;
            
            const targetId = orderIds[Math.floor(Math.random() * orderIds.length)];
            const existing = existingOrders.get(targetId);
            if (!existing) continue;
            
            const updateType = Math.random();
            let updatedOrder;
            
            if (updateType < 0.20) {
              // REGION CHANGE - move order to hot region
              updatedOrder = { ...existing, region: hotRegion };
              
            } else if (updateType < 0.40) {
              // CATEGORY CHANGE - change product category
              updatedOrder = { ...existing, category: hotCategory };
              
            } else if (updateType < 0.60) {
              // STATUS CHANGE - realistic progression through order lifecycle
              const newStatus = getNextStatus(existing.status);
              if (newStatus === existing.status) continue; // Skip if terminal state
              updatedOrder = { ...existing, status: newStatus };
              
            } else if (updateType < 0.80) {
              // AMOUNT CHANGE - significant price change (affects weighted avg)
              const multiplier = 0.5 + Math.random() * 2; // 0.5x to 2.5x
              const newAmount = Math.round(existing.amount * multiplier * 100) / 100;
              const newQuantity = Math.max(1, existing.quantity + Math.floor(Math.random() * 6) - 3);
              updatedOrder = { ...existing, amount: Math.max(10, newAmount), quantity: newQuantity };
              
            } else {
              // CUSTOMER CHANGE - reassign to hot customer (affects top customers)
              const newCustomerId = Math.floor(Math.random() * (hotCustomerRange[1] - hotCustomerRange[0])) + hotCustomerRange[0];
              updatedOrder = { ...existing, customerId: newCustomerId };
            }
            
            existingOrders.set(targetId, updatedOrder);
            orderDeltas.push({ op: 'update', row: updatedOrder });
            
          } else {
            // 30% DELETES - remove orders (balanced with inserts)
            if (orderIds.length === 0) continue;
            
            const targetId = orderIds[Math.floor(Math.random() * orderIds.length)];
            existingOrders.delete(targetId);
            orderDeltas.push({ op: 'delete', orderId: targetId });
          }
        }
        // ============ 15% CUSTOMER UPDATES ============
        else {
          const action = Math.random();
          
          if (action < 0.15) {
            // 15% NEW CUSTOMERS
            const newCustomer = generateCustomer(customerId++);
            existingCustomers.set(newCustomer.customerId, newCustomer);
            customerDeltas.push({ op: 'insert', row: newCustomer });
            
          } else if (action < 0.95) {
            // 80% TIER UPGRADES/CHANGES - this affects join results!
            if (customerIds.length === 0) continue;
            
            const targetId = customerIds[Math.floor(Math.random() * customerIds.length)];
            const existing = existingCustomers.get(targetId);
            if (!existing) continue;
            
            const currentTierIndex = TIERS.indexOf(existing.tier);
            let newTier;
            
            // 70% chance upgrade, 30% chance downgrade
            if (Math.random() < 0.7 && currentTierIndex < TIERS.length - 1) {
              newTier = TIERS[currentTierIndex + 1]; // Upgrade
            } else if (currentTierIndex > 0) {
              newTier = TIERS[currentTierIndex - 1]; // Downgrade
            } else {
              newTier = 'silver'; // Bronze can only go up
            }
            
            const updatedCustomer = { ...existing, tier: newTier };
            existingCustomers.set(targetId, updatedCustomer);
            customerDeltas.push({ op: 'update', row: updatedCustomer });
            
          } else {
            // 5% DELETE CUSTOMERS (rare - affects orphaned orders)
            if (customerIds.length === 0) continue;
            
            const targetId = customerIds[Math.floor(Math.random() * customerIds.length)];
            existingCustomers.delete(targetId);
            customerDeltas.push({ op: 'delete', customerId: targetId });
          }
        }
      }
      
      // Send deltas (combined in one message for atomicity)
      if (orderDeltas.length > 0 || customerDeltas.length > 0) {
        try {
          ws.send(JSON.stringify({
            type: 'delta',
            data: orderDeltas,
            customerData: customerDeltas,  // NEW: customer deltas for join demo
            timestamp: Date.now(),
          }));
        } catch (err) {
          console.error('[Server] Failed to send delta:', err.message);
        }
      }
    }, intervalMs);
    
    console.log(`[Server] Delta stream: ${currentDeltaRate}/s × ${currentBatchSize} = ${currentDeltaRate * currentBatchSize} rows/s (orders + customers)`);
  }
  
  setTimeout(() => startDeltaStream(), 500);
  
  ws.on('message', (message) => {
    try {
      const msg = JSON.parse(message.toString());
      
      switch (msg.type) {
        case 'set-rate':
          currentDeltaRate = Math.max(1, Math.min(1000, msg.rate || 30));
          console.log(`[Server] Rate: ${currentDeltaRate}/s`);
          startDeltaStream();
          break;
          
        case 'set-batch-size':
          currentBatchSize = Math.max(1, Math.min(10000, msg.size || 10));
          console.log(`[Server] Batch: ${currentBatchSize} rows`);
          break;
          
        case 'pause':
          isPaused = true;
          if (deltaInterval) {
            clearInterval(deltaInterval);
            deltaInterval = null;
          }
          console.log(`[Server] Paused`);
          break;
          
        case 'resume':
          isPaused = false;
          startDeltaStream();
          console.log(`[Server] Resumed`);
          break;
          
        case 'burst': {
          const burstSize = Math.min(100000, msg.size || 1000);
          
          // Themed burst - pick random targets for dramatic aggregation changes
          const burstRegion = randomChoice(REGIONS);
          const burstCategory = randomChoice(CATEGORIES);
          // New orders always start as pending (realistic)
          const burstStatus = 'pending';
          const burstCustomerStart = Math.floor(Math.random() * 9000) + 1;
          const burstAmountBias = 1.5 + Math.random() * 2;
          
          console.log(`[Server] Burst: ${burstSize.toLocaleString()} rows → ${burstRegion}/${burstCategory}/${burstStatus}`);
          
          const burstStart = Date.now();
          const burstDeltas = [];
          
          for (let i = 0; i < burstSize; i++) {
            // 80% follow burst theme for dramatic aggregation impact
            const themed = Math.random() < 0.8;
            const newOrder = themed ? {
              orderId: orderId,
              customerId: burstCustomerStart + (i % 100), // Concentrated customers
              productId: Math.floor(Math.random() * 500) + 1,
              amount: Math.round((Math.random() * 400 + 100) * burstAmountBias * 100) / 100,
              quantity: Math.floor(Math.random() * 15) + 3,
              status: burstStatus,
              region: burstRegion,
              category: burstCategory,
            } : generateOrder(orderId);
            
            existingOrders.set(orderId, newOrder);
            burstDeltas.push({ op: 'insert', row: newOrder });
            orderId++;
          }
          
          console.log(`[Server] Generated ${burstSize.toLocaleString()} rows in ${Date.now() - burstStart}ms`);
          
          try {
            const sendStart = Date.now();
            ws.send(JSON.stringify({
              type: 'delta',
              data: burstDeltas,
              customerData: [],  // No customer changes in burst
              timestamp: Date.now(),
              isBurst: true,
              burstProgress: burstDeltas.length,
              burstTotal: burstDeltas.length,
            }));
            console.log(`[Server] Burst sent in ${Date.now() - sendStart}ms`);
          } catch (err) {
            console.error('[Server] Failed to send burst:', err.message);
          }
          break;
        }
        
        case 'customer-burst': {
          // Tier upgrade burst - upgrade many customers to demonstrate join updates
          const burstSize = Math.min(1000, msg.size || 100);
          const targetTier = msg.tier || 'gold';
          
          console.log(`[Server] Customer burst: ${burstSize} upgrades to ${targetTier}`);
          
          const customerDeltas = [];
          const customerIds = Array.from(existingCustomers.keys());
          
          for (let i = 0; i < burstSize && i < customerIds.length; i++) {
            const targetId = customerIds[Math.floor(Math.random() * customerIds.length)];
            const existing = existingCustomers.get(targetId);
            if (!existing || existing.tier === targetTier) continue;
            
            const updatedCustomer = { ...existing, tier: targetTier };
            existingCustomers.set(targetId, updatedCustomer);
            customerDeltas.push({ op: 'update', row: updatedCustomer });
          }
          
          try {
            ws.send(JSON.stringify({
              type: 'delta',
              data: [],  // No order changes
              customerData: customerDeltas,
              timestamp: Date.now(),
              isBurst: true,
            }));
            console.log(`[Server] Customer burst: ${customerDeltas.length} tier upgrades`);
          } catch (err) {
            console.error('[Server] Failed to send customer burst:', err.message);
          }
          break;
        }
          
        default:
          console.log(`[Server] Unknown: ${msg.type}`);
      }
    } catch (err) {
      console.error(`[Server] Parse error:`, err.message);
    }
  });
  
  ws.on('close', () => {
    if (deltaInterval) {
      clearInterval(deltaInterval);
    }
    existingOrders.clear();
    existingCustomers.clear();
    console.log(`[Server] Client disconnected`);
  });
  
  ws.on('error', (err) => {
    console.error(`[Server] Error:`, err.message);
    if (deltaInterval) {
      clearInterval(deltaInterval);
    }
  });
});

console.log(`[Server] Waiting for connections...`);
