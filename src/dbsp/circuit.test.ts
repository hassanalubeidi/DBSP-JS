import { describe, it, expect } from 'vitest';
import { Circuit, createFilterQuery, createMapQuery, createFilterMapReduceQuery } from './circuit';
import { ZSet } from './zset';

describe('Circuit', () => {
  describe('basic operations', () => {
    it('should create circuit with input', () => {
      const circuit = new Circuit();
      const input = circuit.input<number>('numbers');
      expect(input.id).toBe('numbers');
    });

    it('should process filter operation', () => {
      const circuit = new Circuit();
      const input = circuit.input<number>('numbers');
      const filtered = input.filter(x => x > 5);
      
      const results: number[][] = [];
      filtered.output((zset) => {
        results.push((zset as ZSet<number>).values());
      });

      // Step 1: Insert 3, 7, 10
      circuit.step(new Map([
        ['numbers', ZSet.fromValues([3, 7, 10])]
      ]));

      // Only 7 and 10 should pass
      expect(results[0]).toEqual(expect.arrayContaining([7, 10]));
      expect(results[0]).not.toContain(3);
    });

    it('should process map operation', () => {
      const circuit = new Circuit();
      const input = circuit.input<number>('numbers');
      const doubled = input.map(x => x * 2);
      
      const results: number[][] = [];
      doubled.output((zset) => {
        results.push((zset as ZSet<number>).values());
      });

      circuit.step(new Map([
        ['numbers', ZSet.fromValues([1, 2, 3])]
      ]));

      expect(results[0]).toEqual(expect.arrayContaining([2, 4, 6]));
    });

    it('should chain filter and map', () => {
      const circuit = new Circuit();
      const input = circuit.input<number>('numbers');
      const result = input
        .filter(x => x % 2 === 0)
        .map(x => x * 10);
      
      const results: number[][] = [];
      result.output((zset) => {
        results.push((zset as ZSet<number>).values());
      });

      circuit.step(new Map([
        ['numbers', ZSet.fromValues([1, 2, 3, 4])]
      ]));

      // 2 * 10 = 20, 4 * 10 = 40
      expect(results[0]).toEqual(expect.arrayContaining([20, 40]));
      expect(results[0]).not.toContain(10);
      expect(results[0]).not.toContain(30);
    });
  });

  describe('incremental processing', () => {
    it('should process multiple steps incrementally', () => {
      const circuit = new Circuit();
      const input = circuit.input<number>('numbers');
      const filtered = input.filter(x => x > 5);
      const integrated = filtered.integrate();
      
      const results: number[][] = [];
      integrated.output((zset) => {
        results.push((zset as ZSet<number>).values());
      });

      // Step 1: Insert 3, 7
      circuit.step(new Map([
        ['numbers', ZSet.fromValues([3, 7])]
      ]));
      
      // Step 2: Insert 10
      circuit.step(new Map([
        ['numbers', ZSet.fromValues([10])]
      ]));
      
      // Step 3: Delete 7
      circuit.step(new Map([
        ['numbers', ZSet.fromEntries([[7, -1]])]
      ]));

      // After step 1: {7}
      expect(results[0]).toEqual([7]);
      
      // After step 2: {7, 10}
      expect(results[1]).toEqual(expect.arrayContaining([7, 10]));
      
      // After step 3: {10}
      expect(results[2]).toEqual([10]);
    });

    it('should handle insertions and deletions correctly', () => {
      const circuit = new Circuit();
      const input = circuit.input<string>('names');
      const filtered = input.filter(name => name.length > 3);
      const integrated = filtered.integrate();
      
      const results: string[][] = [];
      integrated.output((zset) => {
        results.push((zset as ZSet<string>).values());
      });

      // Insert Alice, Bob, Carol
      circuit.step(new Map([
        ['names', ZSet.fromValues(['Alice', 'Bob', 'Carol'])]
      ]));
      
      // Remove Alice, add Dave
      circuit.step(new Map([
        ['names', ZSet.fromEntries([['Alice', -1], ['Dave', 1]])]
      ]));

      // After step 1: Alice, Carol (Bob too short)
      expect(results[0]).toEqual(expect.arrayContaining(['Alice', 'Carol']));
      expect(results[0]).not.toContain('Bob');
      
      // After step 2: Carol, Dave
      expect(results[1]).toEqual(expect.arrayContaining(['Carol', 'Dave']));
      expect(results[1]).not.toContain('Alice');
    });
  });

  describe('distinct operation', () => {
    it('should track distinct elements incrementally', () => {
      const circuit = new Circuit();
      const input = circuit.input<number>('numbers');
      const distinctDelta = input.distinct();
      const integrated = distinctDelta.integrate();
      
      const results: number[][] = [];
      integrated.output((zset) => {
        results.push((zset as ZSet<number>).values());
      });

      // Insert 1 twice, 2 once
      circuit.step(new Map([
        ['numbers', ZSet.fromEntries([[1, 2], [2, 1]])]
      ]));
      
      // Remove one copy of 1
      circuit.step(new Map([
        ['numbers', ZSet.fromEntries([[1, -1]])]
      ]));
      
      // Remove last copy of 1
      circuit.step(new Map([
        ['numbers', ZSet.fromEntries([[1, -1]])]
      ]));

      // After step 1: distinct {1, 2}
      expect(results[0]).toEqual(expect.arrayContaining([1, 2]));
      
      // After step 2: still {1, 2} (1 still has positive weight)
      expect(results[1]).toEqual(expect.arrayContaining([1, 2]));
      
      // After step 3: just {2}
      expect(results[2]).toEqual([2]);
    });
  });

  describe('join operation', () => {
    it('should join two streams on key', () => {
      interface Person { id: number; name: string }
      interface Salary { personId: number; amount: number }
      
      const circuit = new Circuit();
      const people = circuit.input<Person>('people');
      const salaries = circuit.input<Salary>('salaries');
      
      const joined = people.join(
        salaries,
        p => p.id,
        s => s.personId
      );
      
      const integrated = joined.integrate();
      
      const results: [Person, Salary][][] = [];
      integrated.output((zset) => {
        results.push((zset as ZSet<[Person, Salary]>).values());
      });

      // Insert people
      circuit.step(new Map<string, ZSet<unknown>>([
        ['people', ZSet.fromValues<Person>([
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
        ])],
        ['salaries', ZSet.zero<Salary>()],
      ]));
      
      // Insert salaries
      circuit.step(new Map<string, ZSet<unknown>>([
        ['people', ZSet.zero<Person>()],
        ['salaries', ZSet.fromValues<Salary>([
          { personId: 1, amount: 100 },
          { personId: 2, amount: 200 },
        ])],
      ]));

      // After step 1: no joins yet (no salaries)
      expect(results[0]).toEqual([]);
      
      // After step 2: both should be joined
      expect(results[1].length).toBe(2);
      expect(results[1].some(([p, s]) => p.name === 'Alice' && s.amount === 100)).toBe(true);
      expect(results[1].some(([p, s]) => p.name === 'Bob' && s.amount === 200)).toBe(true);
    });
  });

  describe('union operation', () => {
    it('should combine two streams', () => {
      const circuit = new Circuit();
      const stream1 = circuit.input<number>('stream1');
      const stream2 = circuit.input<number>('stream2');
      const combined = stream1.union(stream2);
      const integrated = combined.integrate();
      
      const results: number[][] = [];
      integrated.output((zset) => {
        results.push((zset as ZSet<number>).values());
      });

      circuit.step(new Map([
        ['stream1', ZSet.fromValues([1, 2])],
        ['stream2', ZSet.fromValues([3, 4])],
      ]));

      expect(results[0]).toEqual(expect.arrayContaining([1, 2, 3, 4]));
    });
  });

  describe('reset', () => {
    it('should reset stateful operators', () => {
      const circuit = new Circuit();
      const input = circuit.input<number>('numbers');
      const integrated = input.integrate();
      
      const results: number[][] = [];
      integrated.output((zset) => {
        results.push((zset as ZSet<number>).values());
      });

      circuit.step(new Map([['numbers', ZSet.fromValues([1, 2])]]));
      circuit.step(new Map([['numbers', ZSet.fromValues([3])]]));
      
      expect(results[1]).toEqual(expect.arrayContaining([1, 2, 3]));
      
      // Reset
      circuit.reset();
      results.length = 0;
      
      // Start fresh
      circuit.step(new Map([['numbers', ZSet.fromValues([10])]]));
      
      expect(results[0]).toEqual([10]);
    });
  });
});

describe('Query Builders', () => {
  it('createFilterQuery should work', () => {
    const { circuit, output } = createFilterQuery<number>(x => x > 5);
    
    const results: number[][] = [];
    output.output((zset) => {
      results.push((zset as ZSet<number>).values());
    });

    circuit.step(new Map([
      ['input', ZSet.fromValues([1, 6, 10, 3])]
    ]));

    expect(results[0]).toEqual(expect.arrayContaining([6, 10]));
  });

  it('createMapQuery should work', () => {
    const { circuit } = createMapQuery<number, string>(
      x => `value: ${x}`
    );
    
    const results: string[][] = [];
    circuit.addOutput('map_input', (zset) => {
      results.push((zset as ZSet<string>).values());
    });

    circuit.step(new Map([
      ['input', ZSet.fromValues([1, 2, 3])]
    ]));

    expect(results[0]).toEqual(expect.arrayContaining([
      'value: 1',
      'value: 2', 
      'value: 3'
    ]));
  });

  it('createFilterMapReduceQuery should chain operations', () => {
    const { circuit, mapped } = createFilterMapReduceQuery<number, number>(
      x => x % 2 === 0,  // filter evens
      x => x * 2         // double them
    );
    
    const results: number[][] = [];
    mapped.output((zset) => {
      results.push((zset as ZSet<number>).values());
    });

    circuit.step(new Map([
      ['input', ZSet.fromValues([1, 2, 3, 4, 5, 6])]
    ]));

    // 2, 4, 6 are even → doubled: 4, 8, 12
    expect(results[0]).toEqual(expect.arrayContaining([4, 8, 12]));
  });
});

describe('Real-world Example: Order Processing', () => {
  interface Order {
    orderId: number;
    customerId: number;
    amount: number;
    status: 'pending' | 'shipped' | 'delivered';
  }

  // Key by all fields for proper delta handling
  const orderKey = (o: Order) => JSON.stringify(o);

  it('should track high-value pending orders', () => {
    const circuit = new Circuit();
    const orders = circuit.input<Order>('orders', orderKey);
    
    // Query: SELECT * FROM orders WHERE status = 'pending' AND amount > 100
    const highValuePending = orders
      .filter(o => o.status === 'pending')
      .filter(o => o.amount > 100);
    
    const integratedView = highValuePending.integrate();
    
    const results: Order[][] = [];
    integratedView.output((zset) => {
      results.push((zset as ZSet<Order>).values());
    });

    // Initial orders
    circuit.step(new Map([
      ['orders', ZSet.fromValues<Order>([
        { orderId: 1, customerId: 101, amount: 50, status: 'pending' },
        { orderId: 2, customerId: 102, amount: 150, status: 'pending' },
        { orderId: 3, customerId: 103, amount: 200, status: 'shipped' },
      ], orderKey)]
    ]));

    // Order 2 is only high-value pending order
    expect(results[0].length).toBe(1);
    expect(results[0][0].orderId).toBe(2);

    // Order 2 gets shipped (delete old status, insert new with status change)
    // Note: we delete the exact old record and insert the new one
    circuit.step(new Map([
      ['orders', ZSet.fromEntries<Order>([
        [{ orderId: 2, customerId: 102, amount: 150, status: 'pending' }, -1],
        [{ orderId: 2, customerId: 102, amount: 150, status: 'shipped' }, 1],
      ], orderKey)]
    ]));

    // No more high-value pending orders (the pending one was deleted)
    expect(results[1].length).toBe(0);

    // New high-value order comes in
    circuit.step(new Map([
      ['orders', ZSet.fromValues<Order>([
        { orderId: 4, customerId: 104, amount: 500, status: 'pending' },
      ], orderKey)]
    ]));

    // Order 4 should appear
    expect(results[2].length).toBe(1);
    expect(results[2][0].orderId).toBe(4);
  });
});

