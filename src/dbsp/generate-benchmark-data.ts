/**
 * Script to generate and save 1M row benchmark dataset as JSON
 */

import { writeFileSync } from 'fs';
import { generateDataset } from './benchmark-data';

const ORDER_COUNT = 1_000_000;
const CUSTOMER_COUNT = 100_000;
const PRODUCT_COUNT = 50_000;

console.log('🔧 Generating 1M row benchmark dataset...');
console.log(`   Orders: ${ORDER_COUNT.toLocaleString()}`);
console.log(`   Customers: ${CUSTOMER_COUNT.toLocaleString()}`);
console.log(`   Products: ${PRODUCT_COUNT.toLocaleString()}`);

const startTime = Date.now();
const dataset = generateDataset(ORDER_COUNT, CUSTOMER_COUNT, PRODUCT_COUNT);
const genTime = Date.now() - startTime;

console.log(`\n✅ Dataset generated in ${(genTime / 1000).toFixed(1)}s`);

console.log('\n💾 Saving to JSON...');
const json = JSON.stringify(dataset, null, 0); // No pretty print for size
writeFileSync('benchmark-data-1m.json', json);

const sizeBytes = Buffer.byteLength(json, 'utf8');
const sizeMB = sizeBytes / (1024 * 1024);

console.log(`✅ Saved to benchmark-data-1m.json (${sizeMB.toFixed(1)} MB)`);
console.log('\nDataset metadata:');
console.log(dataset.metadata);

