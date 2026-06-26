import { eventBus, AppEvent } from './eventBus';
import { pool } from './db';

export function initReconciliationEngine() {
  console.log('[Reconciliation] Initializing Cash Reconciliation Engine...');

  eventBus.on('bank.transaction.received', async (event: AppEvent) => {
    const tx = event.payload ?? {};
    const txId = tx.id;
    const amount = Number(tx.amount);
    const desc = tx.description || '';
    
    console.log(`[Reconciliation] Processing incoming bank transaction: ${tx.bank_reference} (${amount}đ, desc: "${desc}")`);

    try {
      // 1) Query all outstanding orders (current_stage < 8 or stage 8 is active but not completed)
      const outstandingRes = await pool.query(
        `SELECT order_number, customer_name, total_amount 
         FROM order_lifecycle_view 
         WHERE current_stage < 8 OR (current_stage = 8 AND stage_status != 'completed')`
      );
      const outstandingOrders = outstandingRes.rows;

      // 2) Look for Perfect Match (Contains SO-2026-XXXX and amount matches exactly)
      const orderNumMatch = desc.match(/SO-2026-\d{4}/i);
      if (orderNumMatch) {
        const orderNumber = orderNumMatch[0].toUpperCase();
        const matchedOrder = outstandingOrders.find(
          o => o.order_number.toUpperCase() === orderNumber
        );

        if (matchedOrder && Number(matchedOrder.total_amount) === amount) {
          console.log(`[Reconciliation] PERFECT MATCH detected for order ${orderNumber}! Auto-reconciling...`);
          
          const client = await pool.connect();
          try {
            await client.query('BEGIN');
            
            // Update transaction
            await client.query(
              `UPDATE bank_transactions 
               SET status = 'MATCHED', matched_order_number = $1, match_type = 'PERFECT' 
               WHERE id = $2`,
              [orderNumber, txId]
            );

            // Update write model order status
            await client.query(
              `UPDATE orders 
               SET status = 'RECONCILED' 
               WHERE order_number = $1`,
              [orderNumber]
            );

            await client.query('COMMIT');
            
            // Publish payment.reconciled domain event
            eventBus.publish('payment.reconciled', {
              order_number: orderNumber,
              payment_amount: amount,
              transaction_id: tx.bank_reference
            });

            console.log(`[Reconciliation] Auto-reconciled order ${orderNumber} successfully.`);
            return;
          } catch (err) {
            await client.query('ROLLBACK');
            throw err;
          } finally {
            client.release();
          }
        }
      }

      // 3) Try Fuzzy Match
      // Kịch bản a: Khớp số tiền chính xác và độc bản trong số các đơn hàng đang chờ
      const sameAmountOrders = outstandingOrders.filter(o => Number(o.total_amount) === amount);
      if (sameAmountOrders.length === 1) {
        const candidate = sameAmountOrders[0];
        const reason = `Khớp số tiền lẻ chính xác (${amount.toLocaleString('vi-VN')} đ) với đơn hàng đang chờ của ${candidate.customer_name}.`;
        console.log(`[Reconciliation] FUZZY MATCH suggested by AI Agent: ${candidate.order_number} (${reason})`);
        
        await pool.query(
          `UPDATE bank_transactions 
           SET status = 'PENDING_APPROVAL', matched_order_number = $1, match_type = 'FUZZY', suggested_reason = $2 
           WHERE id = $3`,
          [candidate.order_number, reason, txId]
        );
        return;
      }

      // Kịch bản b: Khớp tên khách hàng trong mô tả chuyển khoản
      const descLower = desc.toLowerCase();
      const matchedByNameOrders = outstandingOrders.filter(o => {
        const namePart = o.customer_name.toLowerCase().replace(/khách hàng\s+/i, '').trim();
        return namePart.length > 2 && descLower.includes(namePart);
      });

      if (matchedByNameOrders.length === 1) {
        const candidate = matchedByNameOrders[0];
        const reason = `Khớp tên khách hàng "${candidate.customer_name}" xuất hiện trong nội dung giao dịch.`;
        console.log(`[Reconciliation] FUZZY MATCH suggested by AI Agent: ${candidate.order_number} (${reason})`);
        
        await pool.query(
          `UPDATE bank_transactions 
           SET status = 'PENDING_APPROVAL', matched_order_number = $1, match_type = 'FUZZY', suggested_reason = $2 
           WHERE id = $3`,
          [candidate.order_number, reason, txId]
        );
        return;
      }

      // 4) Unmatched exception
      console.log(`[Reconciliation] No match found for transaction ${tx.bank_reference}. Marked as Exception (UNMATCHED).`);
      await pool.query(
        `UPDATE bank_transactions 
         SET status = 'UNMATCHED', match_type = 'NONE' 
         WHERE id = $1`,
        [txId]
      );

    } catch (err) {
      console.error('[Reconciliation] Error in reconciliation worker:', err);
    }
  });
}
