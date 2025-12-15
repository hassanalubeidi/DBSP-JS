/**
 * SQL to DBSP Compiler
 * 
 * Converts SQL statements (CREATE TABLE, CREATE VIEW) into DBSP circuits.
 * Uses node-sql-parser for parsing and generates incremental streaming circuits.
 * 
 * Based on the DBSP paper: https://github.com/vmware/database-stream-processor
 * 
 * Key DBSP concepts:
 * - Filter/Map are LINEAR operators: Q^Δ = Q (process deltas directly)
 * - Join is BILINEAR: Δ(a ⋈ b) = Δa ⋈ Δb + a ⋈ Δb + Δa ⋈ b
 * - Aggregations require integration to compute over full state
 */

import pkg from 'node-sql-parser';
const { Parser } = pkg;
import { Circuit, StreamHandle } from '../circuit';
import { ZSet } from '../zset';
import { IntegrationState, zsetGroup, DifferentiationState, numberGroup } from '../operators';

// ============ AST TYPES ============

export interface ColumnDef {
  name: string;
  type: string;
}

export interface CreateTableStatement {
  type: 'CREATE_TABLE';
  tableName: string;
  columns: ColumnDef[];
}

// WHERE condition types
export interface ComparisonCondition {
  type: 'COMPARISON';
  column: string;
  operator: '=' | '!=' | '<' | '>' | '<=' | '>=';
  value: string | number | boolean;
}

export interface AndCondition {
  type: 'AND';
  conditions: WhereCondition[];
}

export interface OrCondition {
  type: 'OR';
  conditions: WhereCondition[];
}

export interface BetweenCondition {
  type: 'BETWEEN';
  column: string;
  low: number;
  high: number;
}

export interface InCondition {
  type: 'IN';
  column: string;
  values: (string | number)[];
}

export interface IsNullCondition {
  type: 'IS_NULL';
  column: string;
}

export interface IsNotNullCondition {
  type: 'IS_NOT_NULL';
  column: string;
}

export interface NotCondition {
  type: 'NOT';
  condition: WhereCondition;
}

export interface LikeCondition {
  type: 'LIKE';
  column: string;
  pattern: string;
}

export type WhereCondition = 
  | ComparisonCondition 
  | AndCondition 
  | OrCondition 
  | BetweenCondition
  | InCondition
  | IsNullCondition
  | IsNotNullCondition
  | NotCondition
  | LikeCondition;

// Column types
export interface SimpleColumn {
  type: 'column';
  name: string;
  alias?: string;
}

// Expression argument for aggregates (can be column, *, or binary expression)
export interface AggregateArg {
  type: 'column' | 'star' | 'expression';
  column?: string;
  operator?: string;
  left?: AggregateArg;
  right?: AggregateArg;
  value?: number;  // For literal numbers
}

export interface AggregateColumn {
  type: 'aggregate';
  function: 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX';
  args: string[];  // Keep for backwards compatibility (simple column name)
  argExpr?: AggregateArg;  // New: full expression support
  alias?: string;
}

export interface ExpressionColumn {
  type: 'expression';
  operator: string;
  left: any;
  right: any;
  alias?: string;
}

export interface CaseColumn {
  type: 'case';
  conditions: { when: any; then: any }[];
  else?: any;
  alias?: string;
}

export interface FunctionColumn {
  type: 'function';
  function: string;
  args: string[];
  alias?: string;
}

export interface CastColumn {
  type: 'cast';
  expr: any;
  targetType: string;
  alias?: string;
}

export type SelectColumn = SimpleColumn | AggregateColumn | ExpressionColumn | CaseColumn | FunctionColumn | CastColumn | '*';

// ============ EXPRESSION EVALUATION HELPERS ============

/**
 * Evaluate an aggregate expression against a row
 * Returns the numeric value of the expression for the given row
 */
export function evaluateAggregateExpr(expr: AggregateArg, row: any): number {
  switch (expr.type) {
    case 'star':
      return 1;
    case 'column':
      return Number(row[expr.column!]) || 0;
    case 'expression':
      if (expr.value !== undefined) {
        return expr.value;
      }
      if (expr.operator && expr.left && expr.right) {
        const left = evaluateAggregateExpr(expr.left, row);
        const right = evaluateAggregateExpr(expr.right, row);
        switch (expr.operator) {
          case '+': return left + right;
          case '-': return left - right;
          case '*': return left * right;
          case '/': return right !== 0 ? left / right : 0;
          case '%': return right !== 0 ? left % right : 0;
          default: return 0;
        }
      }
      return 0;
    default:
      return 0;
  }
}

/**
 * Get a string representation of an aggregate expression (for alias generation)
 */
export function getExprString(expr: AggregateArg): string {
  switch (expr.type) {
    case 'star':
      return 'star';
    case 'column':
      return expr.column || '';
    case 'expression':
      if (expr.value !== undefined) {
        return String(expr.value);
      }
      if (expr.operator && expr.left && expr.right) {
        return `${getExprString(expr.left)}_${expr.operator}_${getExprString(expr.right)}`;
      }
      return 'expr';
    default:
      return 'unknown';
  }
}

export interface JoinInfo {
  type: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS';
  table: string;
  leftColumn: string;
  leftTable?: string;
  rightColumn: string;
  rightTable?: string;
}

export interface OrderByItem {
  column: string;
  direction: 'ASC' | 'DESC';
}

export interface SelectQuery {
  type: 'SELECT';
  columns: SelectColumn[];
  from: string;
  join?: JoinInfo;
  where?: WhereCondition;
  groupBy?: string[];
  having?: WhereCondition;
  orderBy?: OrderByItem[];
  limit?: number;
}

export interface UnionQuery {
  type: 'UNION';
  left: SelectQuery;
  right: SelectQuery;
  all: boolean;
}

export type Query = SelectQuery | UnionQuery;

export interface CreateViewStatement {
  type: 'CREATE_VIEW';
  viewName: string;
  query: Query;
}

export type SQLStatement = CreateTableStatement | CreateViewStatement;

export interface ParsedSQL {
  statements: SQLStatement[];
}

// ============ SQL PARSER ============

/**
 * SQLParser: Parses SQL statements using node-sql-parser
 * and converts to our simplified AST format
 */
export class SQLParser {
  private parser: typeof Parser.prototype;

  constructor() {
    this.parser = new Parser();
  }

  /**
   * Parse SQL string into our AST format
   */
  parse(sql: string): ParsedSQL {
    const statements: SQLStatement[] = [];
    
    // Split by semicolons and parse each statement
    const sqlStatements = sql
      .split(';')
      .map(s => s.trim())
      .filter(s => s.length > 0);
    
    for (const stmt of sqlStatements) {
      const parsed = this.parseStatement(stmt);
      if (parsed) {
        statements.push(parsed);
      }
    }
    
    return { statements };
  }

  private parseStatement(sql: string): SQLStatement | null {
    try {
      // Use MySQL dialect for broader compatibility
      const ast = this.parser.astify(sql, { database: 'MySQL' });
      
      // Handle array of statements
      const stmt = Array.isArray(ast) ? ast[0] : ast;
      
      if (!stmt) return null;
      
      if (stmt.type === 'create') {
        if (stmt.keyword === 'table') {
          return this.parseCreateTable(stmt);
        } else if (stmt.keyword === 'view') {
          return this.parseCreateView(stmt);
        }
      }
      
      return null;
    } catch (e) {
      console.error('Parse error:', e);
      return null;
    }
  }

  private parseCreateTable(stmt: any): CreateTableStatement {
    const tableName = stmt.table?.[0]?.table || '';
    const columns: ColumnDef[] = [];
    
    if (stmt.create_definitions) {
      for (const def of stmt.create_definitions) {
        if (def.resource === 'column') {
          columns.push({
            name: def.column?.column || '',
            type: this.normalizeType(def.definition?.dataType || ''),
          });
        }
      }
    }
    
    return {
      type: 'CREATE_TABLE',
      tableName,
      columns,
    };
  }

  private parseCreateView(stmt: any): CreateViewStatement {
    const viewName = stmt.view?.view || '';
    const query = this.parseQuery(stmt.select);
    
    return {
      type: 'CREATE_VIEW',
      viewName,
      query,
    };
  }

  private parseQuery(select: any): Query {
    // Check for UNION (set_op is "union" or "union all")
    if (select._next && select.set_op) {
      const isUnionAll = select.set_op === 'union all';
      return {
        type: 'UNION',
        left: this.parseSelectQuery(select),
        right: this.parseSelectQuery(select._next),
        all: isUnionAll,
      };
    }
    
    return this.parseSelectQuery(select);
  }

  private parseSelectQuery(select: any): SelectQuery {
    // Parse columns (including aggregates, expressions, etc.)
    const columns: SelectColumn[] = [];
    if (select.columns === '*') {
      columns.push('*');
    } else if (Array.isArray(select.columns)) {
      for (const col of select.columns) {
        const parsedCol = this.parseColumn(col);
        if (parsedCol) {
          columns.push(parsedCol);
        }
      }
    }
    
    // Parse FROM (first table)
    const from = select.from?.[0]?.table || '';
    
    // Parse JOIN (if any)
    let join: JoinInfo | undefined;
    if (select.from && select.from.length > 1) {
      const joinClause = select.from[1];
      if (joinClause.join || joinClause.table) {
        const joinStr = joinClause.join || '';
        const joinType = joinStr.includes('CROSS') ? 'CROSS' :
                         joinStr.includes('LEFT') ? 'LEFT' :
                         joinStr.includes('RIGHT') ? 'RIGHT' :
                         joinStr.includes('FULL') ? 'FULL' : 'INNER';
        
        join = {
          type: joinType,
          table: joinClause.table,
          leftColumn: joinClause.on?.left?.column || '',
          leftTable: joinClause.on?.left?.table || undefined,
          rightColumn: joinClause.on?.right?.column || '',
          rightTable: joinClause.on?.right?.table || undefined,
        };
      }
    }
    
    // Parse WHERE
    let where: WhereCondition | undefined;
    if (select.where) {
      where = this.parseWhere(select.where);
    }
    
    // Parse GROUP BY
    let groupBy: string[] | undefined;
    if (select.groupby?.columns) {
      groupBy = select.groupby.columns.map((col: any) => col.column);
    }
    
    // Parse HAVING
    let having: WhereCondition | undefined;
    if (select.having) {
      having = this.parseWhere(select.having);
    }
    
    // Parse ORDER BY
    let orderBy: OrderByItem[] | undefined;
    if (select.orderby) {
      orderBy = select.orderby.map((item: any) => ({
        column: item.expr?.column || '',
        direction: item.type?.toUpperCase() === 'DESC' ? 'DESC' : 'ASC',
      }));
    }
    
    // Parse LIMIT
    let limit: number | undefined;
    if (select.limit) {
      limit = select.limit.value?.[0]?.value || select.limit.value || undefined;
    }
    
    return {
      type: 'SELECT',
      columns,
      from,
      join,
      where,
      groupBy,
      having,
      orderBy,
      limit,
    };
  }

  private parseColumn(col: any): SelectColumn | null {
    if (col.expr?.type === 'column_ref') {
      if (col.expr.column === '*') {
        return '*';
      }
      return {
        type: 'column',
        name: col.expr.column,
        alias: col.as || undefined,
      };
    } else if (col.expr?.type === 'star') {
      return '*';
    } else if (col.expr?.type === 'aggr_func') {
      // Aggregate function (COUNT, SUM, AVG, MIN, MAX)
      const funcName = col.expr.name.toUpperCase();
      const args: string[] = [];
      let argExpr: AggregateArg | undefined;
      
      if (col.expr.args?.expr?.type === 'star') {
        args.push('*');
        argExpr = { type: 'star' };
      } else if (col.expr.args?.expr?.type === 'column_ref') {
        const colName = col.expr.args.expr.column;
        args.push(colName);
        argExpr = { type: 'column', column: colName };
      } else if (col.expr.args?.expr?.type === 'binary_expr') {
        // Complex expression inside aggregate: SUM(amount * quantity)
        argExpr = this.parseAggregateExpr(col.expr.args.expr);
        // For backwards compatibility, create a placeholder arg name
        args.push('_expr_');
      }
      
      return {
        type: 'aggregate',
        function: funcName as AggregateColumn['function'],
        args,
        argExpr,
        alias: col.as || undefined,
      };
    } else if (col.expr?.type === 'binary_expr') {
      // Arithmetic expression
      return {
        type: 'expression',
        operator: col.expr.operator,
        left: col.expr.left,
        right: col.expr.right,
        alias: col.as || undefined,
      };
    } else if (col.expr?.type === 'case') {
      // CASE WHEN expression
      const conditions = col.expr.args?.map((arg: any) => ({
        when: arg.cond,
        then: arg.result,
      })) || [];
      return {
        type: 'case',
        conditions,
        else: col.expr.else,
        alias: col.as || undefined,
      };
    } else if (col.expr?.type === 'function') {
      // Regular function (COALESCE, etc.)
      const args: string[] = [];
      if (col.expr.args?.value) {
        for (const arg of col.expr.args.value) {
          if (arg.type === 'column_ref') {
            args.push(arg.column);
          }
        }
      }
      return {
        type: 'function',
        function: col.expr.name?.name?.[0]?.value?.toUpperCase() || col.expr.name?.toUpperCase() || '',
        args,
        alias: col.as || undefined,
      };
    } else if (col.expr?.type === 'cast') {
      return {
        type: 'cast',
        expr: col.expr.expr,
        targetType: col.expr.target?.dataType || '',
        alias: col.as || undefined,
      };
    }
    
    return null;
  }

  /**
   * Parse an expression inside an aggregate function
   * Handles: column_ref, number, binary_expr (arithmetic)
   */
  private parseAggregateExpr(expr: any): AggregateArg {
    if (expr.type === 'column_ref') {
      return { type: 'column', column: expr.column };
    } else if (expr.type === 'number') {
      return { type: 'expression', value: expr.value };
    } else if (expr.type === 'binary_expr') {
      return {
        type: 'expression',
        operator: expr.operator,
        left: this.parseAggregateExpr(expr.left),
        right: this.parseAggregateExpr(expr.right),
      };
    } else if (expr.type === 'star') {
      return { type: 'star' };
    }
    // Default: treat as column
    return { type: 'column', column: expr.column || expr.value || '' };
  }

  private parseWhere(whereAst: any): WhereCondition {
    // Handle BETWEEN
    if (whereAst.type === 'binary_expr' && whereAst.operator === 'BETWEEN') {
      return {
        type: 'BETWEEN',
        column: whereAst.left?.column || '',
        low: whereAst.right?.value?.[0]?.value || 0,
        high: whereAst.right?.value?.[1]?.value || 0,
      };
    }
    
    // Handle IN
    if (whereAst.type === 'binary_expr' && whereAst.operator === 'IN') {
      const values = whereAst.right?.value?.map((v: any) => v.value) || [];
      return {
        type: 'IN',
        column: whereAst.left?.column || '',
        values,
      };
    }
    
    // Handle IS NULL
    if (whereAst.type === 'binary_expr' && whereAst.operator === 'IS') {
      if (whereAst.right?.type === 'null') {
        return {
          type: 'IS_NULL',
          column: whereAst.left?.column || '',
        };
      }
    }
    
    // Handle IS NOT NULL (operator is 'IS NOT')
    if (whereAst.type === 'binary_expr' && whereAst.operator === 'IS NOT') {
      if (whereAst.right?.type === 'null') {
        return {
          type: 'IS_NOT_NULL',
          column: whereAst.left?.column || '',
        };
      }
    }
    
    // Handle NOT
    if (whereAst.type === 'unary_expr' && whereAst.operator === 'NOT') {
      return {
        type: 'NOT',
        condition: this.parseWhere(whereAst.expr),
      };
    }
    
    // Handle LIKE
    if (whereAst.type === 'binary_expr' && whereAst.operator === 'LIKE') {
      return {
        type: 'LIKE',
        column: whereAst.left?.column || '',
        pattern: whereAst.right?.value || '',
      };
    }
    
    if (whereAst.type === 'binary_expr') {
      if (whereAst.operator === 'AND') {
        return {
          type: 'AND',
          conditions: [
            this.parseWhere(whereAst.left),
            this.parseWhere(whereAst.right),
          ],
        };
      } else if (whereAst.operator === 'OR') {
        return {
          type: 'OR',
          conditions: [
            this.parseWhere(whereAst.left),
            this.parseWhere(whereAst.right),
          ],
        };
      } else {
        // Comparison operator
        const column = whereAst.left?.column || '';
        const operator = whereAst.operator as ComparisonCondition['operator'];
        let value: string | number | boolean = '';
        
        if (whereAst.right?.type === 'string' || whereAst.right?.type === 'single_quote_string') {
          value = whereAst.right.value;
        } else if (whereAst.right?.type === 'number') {
          value = whereAst.right.value;
        } else if (whereAst.right?.type === 'bool') {
          value = whereAst.right.value;
        } else if (whereAst.right?.type === 'double_quote_string') {
          value = whereAst.right.value;
        }
        
        return {
          type: 'COMPARISON',
          column,
          operator,
          value,
        };
      }
    }
    
    // Default fallback
    return {
      type: 'COMPARISON',
      column: '',
      operator: '=',
      value: '',
    };
  }

  private normalizeType(type: string): string {
    const upper = type.toUpperCase();
    if (upper.startsWith('VARCHAR')) return 'VARCHAR';
    if (upper.startsWith('INT')) return 'INT';
    if (upper.startsWith('DECIMAL')) return 'DECIMAL';
    if (upper.startsWith('TIMESTAMP')) return 'TIMESTAMP';
    return upper;
  }
}

// ============ SQL COMPILER ============

export interface CompileResult {
  circuit: Circuit;
  tables: Record<string, StreamHandle<any>>;
  views: Record<string, StreamHandle<any>>;
}

/**
 * SQLCompiler: Compiles parsed SQL into DBSP circuits
 * 
 * Key insights from DBSP paper:
 * - Filter is a LINEAR operator, so filter^Δ = filter (works directly on deltas)
 * - Map/Projection is LINEAR, so map^Δ = map
 * - Join is BILINEAR: requires special handling for incrementality
 * - Aggregations require integration to maintain full state
 */
export class SQLCompiler {
  /**
   * Compile SQL string to a DBSP circuit
   */
  compile(sql: string): CompileResult {
    const parser = new SQLParser();
    const ast = parser.parse(sql);
    
    const circuit = new Circuit();
    const tables: Record<string, StreamHandle<any>> = {};
    const views: Record<string, StreamHandle<any>> = {};
    
    // First pass: Create inputs for all tables
    for (const stmt of ast.statements) {
      if (stmt.type === 'CREATE_TABLE') {
        const keyFn = (row: any) => JSON.stringify(row);
        tables[stmt.tableName] = circuit.input(stmt.tableName, keyFn);
      }
    }
    
    // Second pass: Create views
    for (const stmt of ast.statements) {
      if (stmt.type === 'CREATE_VIEW') {
        const view = this.compileQuery(stmt.query, tables, circuit);
        if (view) {
          views[stmt.viewName] = view;
        }
      }
    }
    
    return { circuit, tables, views };
  }

  private compileQuery(
    query: Query,
    tables: Record<string, StreamHandle<any>>,
    circuit: Circuit
  ): StreamHandle<any> | null {
    if (query.type === 'UNION') {
      return this.compileUnion(query, tables, circuit);
    }
    return this.compileSelect(query, tables, circuit);
  }

  private compileSelect(
    query: SelectQuery,
    tables: Record<string, StreamHandle<any>>,
    circuit: Circuit
  ): StreamHandle<any> | null {
    let stream: StreamHandle<any> | null = null;
    
    // Handle JOIN
    if (query.join) {
      stream = this.compileJoin(query, tables);
    } else {
      stream = tables[query.from];
    }
    
    if (!stream) {
      console.error(`Table ${query.from} not found`);
      return null;
    }
    
    // Apply WHERE clause filter (LINEAR operator - works on deltas directly!)
    if (query.where) {
      const predicate = this.compileWhere(query.where);
      stream = stream.filter(predicate);
    }
    
    // Handle aggregations
    const hasAggregates = query.columns.some(
      c => c !== '*' && typeof c === 'object' && c.type === 'aggregate'
    );
    
    if (hasAggregates) {
      stream = this.compileAggregation(stream, query, circuit);
    } else {
      // Apply column projection if not SELECT *
      const hasSelectAll = query.columns.includes('*');
      if (!hasSelectAll) {
        const cols = query.columns
          .filter((c): c is SimpleColumn => c !== '*' && typeof c === 'object' && c.type === 'column')
          .map(c => c.name);
        
        if (cols.length > 0) {
          stream = stream.map((row: any) => {
            const result: any = {};
            for (const col of cols) {
              result[col] = row[col];
            }
            return result;
          });
        }
      }
    }
    
    return stream;
  }

  private compileJoin(
    query: SelectQuery,
    tables: Record<string, StreamHandle<any>>
  ): StreamHandle<any> | null {
    const leftTable = tables[query.from];
    const rightTable = query.join ? tables[query.join.table] : null;
    
    if (!leftTable || !rightTable || !query.join) {
      console.error('Join tables not found');
      return null;
    }
    
    const join = query.join;
    
    // Create key functions for the join
    // The key function extracts the join column value
    const leftKeyFn = (row: any) => row[join.leftColumn];
    const rightKeyFn = (row: any) => row[join.rightColumn];
    
    // Use the circuit's join operator (bilinear - handles incrementality)
    // Δ(a ⋈ b) = Δa ⋈ Δb + a ⋈ Δb + Δa ⋈ b
    return leftTable.join(rightTable, leftKeyFn, rightKeyFn);
  }

  private compileAggregation(
    stream: StreamHandle<any>,
    query: SelectQuery,
    circuit: Circuit
  ): StreamHandle<any> {
    // Find aggregate functions and group by columns
    const aggregates = query.columns.filter(
      (c): c is AggregateColumn => c !== '*' && typeof c === 'object' && c.type === 'aggregate'
    );
    
    // Find simple columns (for GROUP BY output)
    const simpleColumns = query.columns.filter(
      (c): c is SimpleColumn => c !== '*' && typeof c === 'object' && c.type === 'column'
    );
    
    const groupByColumns = query.groupBy || [];
    const hasGroupBy = groupByColumns.length > 0;
    
    if (aggregates.length === 0) {
      return stream;
    }
    
    // ============ GROUP BY AGGREGATION ============
    // 
    // In DBSP, incremental GROUP BY works as follows:
    // 1. Maintain integrated state (full data) grouped by key
    // 2. When delta arrives, update affected groups
    // 3. Output delta of changes to each group's aggregated values
    //
    // For each group, we track:
    // - Previous aggregated value (to compute delta)
    // - Current aggregated value (after applying input delta)
    
    if (hasGroupBy) {
      // Create key function for grouping
      const getGroupKey = (row: any): string => {
        return groupByColumns.map(col => String(row[col] ?? '')).join('::');
      };
      
      // Use module-level helper functions for expression evaluation
      const evaluateExpr = evaluateAggregateExpr;
      const getExprStr = getExprString;
      
      // State: Map of groupKey -> { sum, count, rows (for min/max) }
      type GroupState = {
        sum: Map<string, number>;  // per aggregate column
        count: number;
        min: Map<string, number>;
        max: Map<string, number>;
      };
      
      const groupStates = new Map<string, GroupState>();
      const previousResults = new Map<string, any>(); // Previous output per group
      
      return circuit.addStatefulOperator(
        `groupby_agg_${stream.id}`,
        [stream.id],
        (inputs: ZSet<any>[]) => {
          const delta = inputs[0];
          const affectedGroups = new Set<string>();
          
          // Apply delta to group states
          for (const [row, weight] of delta.entries()) {
            const groupKey = getGroupKey(row);
            affectedGroups.add(groupKey);
            
            // Get or create group state
            let state = groupStates.get(groupKey);
            if (!state) {
              state = {
                sum: new Map(),
                count: 0,
                min: new Map(),
                max: new Map(),
              };
              groupStates.set(groupKey, state);
            }
            
            // Update count
            state.count += weight;
            
            // Update sums and track values for min/max
            for (const agg of aggregates) {
              const col = agg.args[0];
              // Use expression if available, otherwise fall back to column name
              const exprStr = agg.argExpr ? getExprStr(agg.argExpr) : (col === '*' ? 'star' : col);
              const alias = agg.alias || `${agg.function.toLowerCase()}_${exprStr}`;
              
              if (agg.function === 'SUM') {
                const currentSum = state.sum.get(alias) || 0;
                // Evaluate expression or simple column
                const value = agg.argExpr 
                  ? evaluateExpr(agg.argExpr, row)
                  : (col === '*' ? 1 : (Number(row[col]) || 0));
                state.sum.set(alias, currentSum + value * weight);
              } else if (agg.function === 'COUNT') {
                const currentSum = state.sum.get(alias) || 0;
                state.sum.set(alias, currentSum + weight);
              } else if (agg.function === 'AVG') {
                // For AVG, we track sum separately
                const sumKey = `_sum_for_avg_${alias}`;
                const currentSum = state.sum.get(sumKey) || 0;
                const value = agg.argExpr 
                  ? evaluateExpr(agg.argExpr, row)
                  : (col === '*' ? 1 : (Number(row[col]) || 0));
                state.sum.set(sumKey, currentSum + value * weight);
              } else if (agg.function === 'MIN' || agg.function === 'MAX') {
                // Track all values for min/max
                const value = agg.argExpr 
                  ? evaluateExpr(agg.argExpr, row)
                  : (col === '*' ? 1 : (Number(row[col]) || 0));
                // Simple tracking: just keep running min/max (note: doesn't handle deletes perfectly)
                if (weight > 0) {
                  if (agg.function === 'MIN') {
                    const currentMin = state.min.get(alias);
                    if (currentMin === undefined || value < currentMin) {
                      state.min.set(alias, value);
                    }
                  } else {
                    const currentMax = state.max.get(alias);
                    if (currentMax === undefined || value > currentMax) {
                      state.max.set(alias, value);
                    }
                  }
                }
              }
            }
          }
          
          // Build output delta: for each affected group, emit:
          // - Remove previous aggregated row (weight -1)
          // - Add new aggregated row (weight +1)
          const outputEntries: [any, number][] = [];
          
          for (const groupKey of affectedGroups) {
            const state = groupStates.get(groupKey);
            if (!state) continue;
            
            // Remove previous result if it existed
            const prevResult = previousResults.get(groupKey);
            if (prevResult) {
              outputEntries.push([prevResult, -1]);
            }
            
            // If count <= 0, group is deleted
            if (state.count <= 0) {
              groupStates.delete(groupKey);
              previousResults.delete(groupKey);
              continue;
            }
            
            // Build new aggregated row
            const newRow: any = {};
            
            // Add group by columns
            // Parse group key back to values (we need a sample row)
            const keyParts = groupKey.split('::');
            for (let i = 0; i < groupByColumns.length; i++) {
              newRow[groupByColumns[i]] = keyParts[i];
            }
            
            // Add simple columns that match group by
            for (const col of simpleColumns) {
              if (groupByColumns.includes(col.name)) {
                const idx = groupByColumns.indexOf(col.name);
                newRow[col.alias || col.name] = keyParts[idx];
              }
            }
            
            // Add aggregates
            for (const agg of aggregates) {
              const col = agg.args[0];
              const exprStr = agg.argExpr ? getExprStr(agg.argExpr) : (col === '*' ? 'star' : col);
              const alias = agg.alias || `${agg.function.toLowerCase()}_${exprStr}`;
              
              switch (agg.function) {
                case 'COUNT':
                  newRow[alias] = state.sum.get(alias) || 0;
                  break;
                case 'SUM':
                  newRow[alias] = state.sum.get(alias) || 0;
                  break;
                case 'AVG': {
                  const sumKey = `_sum_for_avg_${alias}`;
                  const sum = state.sum.get(sumKey) || 0;
                  newRow[alias] = state.count > 0 ? sum / state.count : 0;
                  break;
                }
                case 'MIN':
                  newRow[alias] = state.min.get(alias) ?? 0;
                  break;
                case 'MAX':
                  newRow[alias] = state.max.get(alias) ?? 0;
                  break;
                default:
                  newRow[alias] = 0;
              }
            }
            
            // Store new result and emit
            previousResults.set(groupKey, newRow);
            outputEntries.push([newRow, 1]);
          }
          
          // Return output delta as ZSet
          // IMPORTANT: Key by full row content, NOT just group key!
          // This prevents old_row (-1) and new_row (+1) from cancelling when they have same group key
          // but different aggregated values
          const outputKeyFn = (row: any) => JSON.stringify(row);
          return ZSet.fromEntries(outputEntries, outputKeyFn);
        },
        () => {
          groupStates.clear();
          previousResults.clear();
        }
      );
    }
    
    // ============ GLOBAL AGGREGATION (no GROUP BY) ============
    
    const firstAgg = aggregates[0];
    const column = firstAgg.args[0];
    
    switch (firstAgg.function) {
      case 'COUNT':
        // COUNT is linear over ZSets (sum of weights)
        return stream.count() as unknown as StreamHandle<any>;
      
      case 'SUM':
        return stream.sum((row: any) => row[column] || 0) as unknown as StreamHandle<any>;
      
      case 'AVG': {
        const intState = new IntegrationState(zsetGroup<any>());
        const diffState = new DifferentiationState(numberGroup());
        
        return circuit.addStatefulOperator(
          `avg_${stream.id}`,
          [stream.id],
          (inputs: ZSet<any>[]) => {
            const integrated = intState.step(inputs[0]);
            const sum = integrated.sum((row: any) => row[column] || 0);
            const count = integrated.count();
            const avg = count > 0 ? sum / count : 0;
            return diffState.step(avg);
          },
          () => {
            intState.reset();
            diffState.reset();
          }
        ) as unknown as StreamHandle<any>;
      }
      
      case 'MIN': {
        const intState = new IntegrationState(zsetGroup<any>());
        const diffState = new DifferentiationState(numberGroup());
        
        return circuit.addStatefulOperator(
          `min_${stream.id}`,
          [stream.id],
          (inputs: ZSet<any>[]) => {
            const integrated = intState.step(inputs[0]);
            const values = integrated.values();
            const min = values.length > 0 
              ? Math.min(...values.map((row: any) => row[column] || Infinity))
              : 0;
            return diffState.step(min);
          },
          () => {
            intState.reset();
            diffState.reset();
          }
        ) as unknown as StreamHandle<any>;
      }
      
      case 'MAX': {
        const intState = new IntegrationState(zsetGroup<any>());
        const diffState = new DifferentiationState(numberGroup());
        
        return circuit.addStatefulOperator(
          `max_${stream.id}`,
          [stream.id],
          (inputs: ZSet<any>[]) => {
            const integrated = intState.step(inputs[0]);
            const values = integrated.values();
            const max = values.length > 0 
              ? Math.max(...values.map((row: any) => row[column] || -Infinity))
              : 0;
            return diffState.step(max);
          },
          () => {
            intState.reset();
            diffState.reset();
          }
        ) as unknown as StreamHandle<any>;
      }
      
      default:
        return stream;
    }
  }

  private compileUnion(
    query: UnionQuery,
    tables: Record<string, StreamHandle<any>>,
    circuit: Circuit
  ): StreamHandle<any> | null {
    const left = this.compileSelect(query.left, tables, circuit);
    const right = this.compileSelect(query.right, tables, circuit);
    
    if (!left || !right) {
      return null;
    }
    
    // Union is ZSet addition (linear!)
    // For UNION ALL, just add
    // For UNION (distinct), add then apply distinct
    const union = left.union(right);
    
    if (!query.all) {
      // UNION (not ALL) needs distinct
      return union.distinct();
    }
    
    return union;
  }

  private compileWhere(where: WhereCondition): (row: any) => boolean {
    switch (where.type) {
      case 'COMPARISON':
        return this.compileComparison(where);
      case 'AND':
        return this.compileAnd(where);
      case 'OR':
        return this.compileOr(where);
      case 'BETWEEN':
        return this.compileBetween(where);
      case 'IN':
        return this.compileIn(where);
      case 'IS_NULL':
        return this.compileIsNull(where);
      case 'IS_NOT_NULL':
        return this.compileIsNotNull(where);
      case 'NOT':
        return this.compileNot(where);
      case 'LIKE':
        return this.compileLike(where);
    }
  }

  private compileComparison(cond: ComparisonCondition): (row: any) => boolean {
    return (row: any) => {
      const rowValue = row[cond.column];
      const compareValue = cond.value;
      
      switch (cond.operator) {
        case '=':
          return rowValue === compareValue;
        case '!=':
          return rowValue !== compareValue;
        case '<':
          return rowValue < compareValue;
        case '>':
          return rowValue > compareValue;
        case '<=':
          return rowValue <= compareValue;
        case '>=':
          return rowValue >= compareValue;
        default:
          return false;
      }
    };
  }

  private compileAnd(cond: AndCondition): (row: any) => boolean {
    const predicates = cond.conditions.map(c => this.compileWhere(c));
    return (row: any) => predicates.every(p => p(row));
  }

  private compileOr(cond: OrCondition): (row: any) => boolean {
    const predicates = cond.conditions.map(c => this.compileWhere(c));
    return (row: any) => predicates.some(p => p(row));
  }

  private compileBetween(cond: BetweenCondition): (row: any) => boolean {
    return (row: any) => {
      const value = row[cond.column];
      return value >= cond.low && value <= cond.high;
    };
  }

  private compileIn(cond: InCondition): (row: any) => boolean {
    const valueSet = new Set(cond.values);
    return (row: any) => valueSet.has(row[cond.column]);
  }

  private compileIsNull(cond: IsNullCondition): (row: any) => boolean {
    return (row: any) => row[cond.column] === null || row[cond.column] === undefined;
  }

  private compileIsNotNull(cond: IsNotNullCondition): (row: any) => boolean {
    return (row: any) => row[cond.column] !== null && row[cond.column] !== undefined;
  }

  private compileNot(cond: NotCondition): (row: any) => boolean {
    const innerPredicate = this.compileWhere(cond.condition);
    return (row: any) => !innerPredicate(row);
  }

  private compileLike(cond: LikeCondition): (row: any) => boolean {
    // Convert SQL LIKE pattern to regex
    const pattern = cond.pattern
      .replace(/%/g, '.*')
      .replace(/_/g, '.');
    const regex = new RegExp(`^${pattern}$`, 'i');
    
    return (row: any) => {
      const value = row[cond.column];
      return typeof value === 'string' && regex.test(value);
    };
  }
}
