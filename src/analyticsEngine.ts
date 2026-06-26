import { eventBus, AppEvent } from './eventBus';
import { pool } from './db';

// Simulated BQ CDC stream (Datastream)
export function initAnalyticsEngine() {
  console.log('[Analytics] Initializing Real-time CDC Pipeline (Datastream)...');

  // Helper to stream order state change to bq_raw_orders
  async function streamOrderToBq(orderNumber: string) {
    try {
      const { rows } = await pool.query(
        `SELECT order_number, customer_name, total_amount, status 
         FROM orders 
         WHERE order_number = $1`,
        [orderNumber]
      );
      if (rows.length > 0) {
        const o = rows[0];
        await pool.query(
          `INSERT INTO bq_raw_orders (order_number, customer_name, total_amount, status, updated_at)
           VALUES ($1, $2, $3, $4, now())`,
          [o.order_number, o.customer_name, Number(o.total_amount), o.status]
        );
        console.log(`[CDC Datastream] Streamed Order change: ${orderNumber} (${o.status}) to BigQuery raw table.`);
      }
    } catch (err) {
      console.error('[CDC Datastream] Error streaming order change:', err);
    }
  }

  // Helper to stream transaction state change to bq_raw_bank_transactions
  async function streamTxToBq(bankReference: string) {
    try {
      const { rows } = await pool.query(
        `SELECT bank_reference, bank_name, amount, status, transaction_time 
         FROM bank_transactions 
         WHERE bank_reference = $1`,
        [bankReference]
      );
      if (rows.length > 0) {
        const tx = rows[0];
        await pool.query(
          `INSERT INTO bq_raw_bank_transactions (bank_reference, bank_name, amount, status, transaction_time)
           VALUES ($1, $2, $3, $4, $5)`,
          [tx.bank_reference, tx.bank_name, Number(tx.amount), tx.status, tx.transaction_time]
        );
        console.log(`[CDC Datastream] Streamed Bank Transaction change: ${bankReference} (${tx.status}) to BigQuery raw table.`);
      }
    } catch (err) {
      console.error('[CDC Datastream] Error streaming transaction change:', err);
    }
  }

  // 1) Listen to all Order lifecycle related events
  eventBus.on('*', async (event: AppEvent) => {
    const type = event.event_type;
    const isOrderEvent = type.startsWith('order.') || 
                         type.startsWith('sourcing.') || 
                         type.startsWith('wms.') || 
                         type.startsWith('logistics.') ||
                         type === 'payment.reconciled';
    
    if (isOrderEvent) {
      const payload = event.payload ?? {};
      const orderNumber = payload.order_number || (payload.order && payload.order.order_number);
      if (orderNumber) {
        // Wait a brief delay to let read model projection finish first
        setTimeout(() => streamOrderToBq(orderNumber), 100);
      }
    }
  });

  // 2) Listen to bank transaction received event
  eventBus.on('bank.transaction.received', async (event: AppEvent) => {
    const tx = event.payload ?? {};
    if (tx.bank_reference) {
      setTimeout(() => streamTxToBq(tx.bank_reference), 150);
    }
  });

  // 3) Listen to payment reconciled event (to stream the updated status)
  eventBus.on('payment.reconciled', async (event: AppEvent) => {
    const { transaction_id } = event.payload ?? {};
    if (transaction_id) {
      setTimeout(() => streamTxToBq(transaction_id), 150);
    }
  });
}

// dbt Transformation Runner
export async function runDbtModels(): Promise<{ success: boolean; logs: string[] }> {
  const logs: string[] = [];
  const start = Date.now();
  const timestamp = () => new Date().toLocaleTimeString('vi-VN');

  logs.push(`${timestamp()} | Concurrency: 1`);
  logs.push(`${timestamp()} | `);

  try {
    // Model 1: bq_dim_customers
    logs.push(`${timestamp()} | 1 of 3 START table bq_dim_customers................................. [RUN]`);
    const m1Start = Date.now();
    
    // Clear and rebuild dim table
    await pool.query('TRUNCATE bq_dim_customers');
    await pool.query(
      `INSERT INTO bq_dim_customers (customer_name, total_orders, total_revenue, outstanding_ar, last_purchase)
       WITH latest_orders AS (
         SELECT 
           order_number,
           customer_name,
           total_amount,
           status,
           updated_at,
           ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY updated_at DESC) as rn
         FROM bq_raw_orders
       )
       SELECT 
         customer_name,
         COUNT(DISTINCT order_number) as total_orders,
         SUM(CASE WHEN status != 'DRAFT' THEN total_amount ELSE 0 END) as total_revenue,
         SUM(CASE WHEN status NOT IN ('RECONCILED', 'CLOSED') THEN total_amount ELSE 0 END) as outstanding_ar,
         MAX(updated_at) as last_purchase
       FROM latest_orders
       WHERE rn = 1
       GROUP BY customer_name`
    );
    const m1Time = ((Date.now() - m1Start) / 1000).toFixed(2);
    logs.push(`${timestamp()} | 1 of 3 OK created table bq_dim_customers............................ [SELECT in ${m1Time}s]`);

    // Model 2: bq_fct_sales_performance
    logs.push(`${timestamp()} | 2 of 3 START table bq_fct_sales_performance......................... [RUN]`);
    const m2Start = Date.now();
    await pool.query('TRUNCATE bq_fct_sales_performance');
    await pool.query(
      `INSERT INTO bq_fct_sales_performance (sales_date, total_orders, total_revenue)
       WITH latest_orders AS (
         SELECT 
           order_number,
           customer_name,
           total_amount,
           status,
           updated_at,
           ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY updated_at DESC) as rn
         FROM bq_raw_orders
       )
       SELECT 
         DATE(updated_at) as sales_date,
         COUNT(DISTINCT order_number) as total_orders,
         SUM(total_amount) as total_revenue
       FROM latest_orders
       WHERE rn = 1 AND status != 'DRAFT'
       GROUP BY DATE(updated_at)`
    );
    const m2Time = ((Date.now() - m2Start) / 1000).toFixed(2);
    logs.push(`${timestamp()} | 2 of 3 OK created table bq_fct_sales_performance.................... [SELECT in ${m2Time}s]`);

    // Model 3: bq_fct_cash_flow
    logs.push(`${timestamp()} | 3 of 3 START table bq_fct_cash_flow................................. [RUN]`);
    const m3Start = Date.now();
    await pool.query('TRUNCATE bq_fct_cash_flow');
    await pool.query(
      `INSERT INTO bq_fct_cash_flow (recon_date, total_deposits, matched_amount, unmatched_amount, reconciliation_rate)
       WITH latest_transactions AS (
         SELECT 
           bank_reference,
           bank_name,
           amount,
           status,
           transaction_time,
           ROW_NUMBER() OVER (PARTITION BY bank_reference ORDER BY id DESC) as rn
         FROM bq_raw_bank_transactions
       )
       SELECT 
         DATE(transaction_time) as recon_date,
         SUM(amount) as total_deposits,
         SUM(CASE WHEN status = 'MATCHED' THEN amount ELSE 0 END) as matched_amount,
         SUM(CASE WHEN status != 'MATCHED' THEN amount ELSE 0 END) as unmatched_amount,
         CASE 
           WHEN SUM(amount) > 0 THEN ROUND((SUM(CASE WHEN status = 'MATCHED' THEN amount ELSE 0 END) / SUM(amount)) * 100, 2)
           ELSE 0
         END as reconciliation_rate
       FROM latest_transactions
       WHERE rn = 1
       GROUP BY DATE(transaction_time)`
    );
    const m3Time = ((Date.now() - m3Start) / 1000).toFixed(2);
    logs.push(`${timestamp()} | 3 of 3 OK created table bq_fct_cash_flow............................ [SELECT in ${m3Time}s]`);

    const totalTime = ((Date.now() - start) / 1000).toFixed(2);
    logs.push(`${timestamp()} | `);
    logs.push(`${timestamp()} | Finished running 3 models in ${totalTime}s.`);
    logs.push(`${timestamp()} | `);
    logs.push(`${timestamp()} | Completed successfully!`);
    
    return { success: true, logs };
  } catch (err) {
    logs.push(`${timestamp()} | ERROR running dbt models: ${String(err)}`);
    return { success: false, logs };
  }
}
