import { eventBus, AppEvent } from './eventBus';
import { pool } from './db';

export function initProjector() {
  console.log('[Projector] Initializing Order Lifecycle Projector...');

  // Helper to build/update the history array in order_lifecycle_view
  async function updateStageInReadModel(orderNumber: string, stageIndex: number, status: 'active' | 'completed' | 'stuck' | 'returned', message: string) {
    const timestamp = new Date().toISOString();
    
    // Get existing record to fetch current history
    const { rows } = await pool.query('SELECT * FROM order_lifecycle_view WHERE order_number = $1', [orderNumber]);
    if (rows.length === 0) {
      console.warn(`[Projector] Order ${orderNumber} not found in order_lifecycle_view. Cannot update stage ${stageIndex}.`);
      return;
    }
    
    const view = rows[0];
    let history = view.history || [];
    
    // Check if this stage entry already exists in history, if so update it, else push
    const existingIndex = history.findIndex((h: any) => h.stage === stageIndex);
    if (existingIndex >= 0) {
      history[existingIndex] = {
        ...history[existingIndex],
        status,
        timestamp,
        message
      };
    } else {
      history.push({
        stage: stageIndex,
        status,
        timestamp,
        message
      });
    }

    // If we mark this stage as completed, make sure preceding stages are also marked completed in history
    if (status === 'completed') {
      for (let i = 1; i < stageIndex; i++) {
        const idx = history.findIndex((h: any) => h.stage === i);
        if (idx >= 0) {
          history[idx].status = 'completed';
        } else {
          history.push({
            stage: i,
            status: 'completed',
            timestamp,
            message: 'Tự động hoàn thành theo luồng'
          });
        }
      }
    }

    // Determine new current stage and stage_status
    let current_stage = stageIndex;
    let stage_status = status;

    if (status === 'completed' && stageIndex < 9) {
      current_stage = stageIndex + 1;
      stage_status = 'active';
      // Add active state for the next stage in history if it doesn't exist
      const nextIdx = history.findIndex((h: any) => h.stage === current_stage);
      if (nextIdx < 0) {
        history.push({
          stage: current_stage,
          status: 'active',
          timestamp,
          message: 'Đang xử lý...'
        });
      } else {
        history[nextIdx].status = 'active';
        history[nextIdx].timestamp = timestamp;
        history[nextIdx].message = 'Đang xử lý...';
      }
    }

    await pool.query(
      `UPDATE order_lifecycle_view
       SET current_stage = $1, stage_status = $2, history = $3, updated_at = now()
       WHERE order_number = $4`,
      [current_stage, stage_status, JSON.stringify(history), orderNumber]
    );
    console.log(`[Projector] Projected order ${orderNumber} to stage ${current_stage} (${stage_status})`);
  }

  // 1) Handle order.created event
  eventBus.on('order.created', async (event: AppEvent) => {
    const { order } = event.payload ?? {};
    try {
      const history = [
        { stage: 1, status: 'completed', timestamp: event.published_at, message: 'Đơn hàng được tạo thành công' },
        { stage: 2, status: 'active', timestamp: event.published_at, message: 'Đang chờ xác nhận...' }
      ];

      await pool.query(
        `INSERT INTO order_lifecycle_view (order_number, customer_name, total_amount, current_stage, stage_status, history, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (order_number) DO UPDATE
         SET customer_name = EXCLUDED.customer_name, total_amount = EXCLUDED.total_amount, current_stage = EXCLUDED.current_stage, stage_status = EXCLUDED.stage_status, history = EXCLUDED.history, updated_at = now()`,
        [order.order_number, order.customer_name, order.total_amount, 2, 'active', JSON.stringify(history), event.published_at]
      );
      console.log(`[Projector] Initialized Read Model for order ${order.order_number}`);
    } catch (err) {
      console.error(`[Projector] Failed to project order.created:`, err);
    }
  });

  // 2) Handle order.approved
  eventBus.on('order.approved', async (event: AppEvent) => {
    const { order_number } = event.payload ?? {};
    await updateStageInReadModel(order_number, 2, 'completed', 'Đơn hàng đã được duyệt bởi Sales');
  });

  // 3) Handle sourcing.completed
  eventBus.on('sourcing.completed', async (event: AppEvent) => {
    const { order_number } = event.payload ?? {};
    await updateStageInReadModel(order_number, 3, 'completed', 'Đã mua hàng và điều phối kho');
  });

  // 4) Handle wms.allocated
  eventBus.on('wms.allocated', async (event: AppEvent) => {
    const { order_number } = event.payload ?? {};
    await updateStageInReadModel(order_number, 4, 'completed', 'Kho đã phân bổ và giữ tồn kho thành công');
  });

  // 5) Handle wms.packed
  eventBus.on('wms.packed', async (event: AppEvent) => {
    const { order_number } = event.payload ?? {};
    await updateStageInReadModel(order_number, 5, 'completed', 'Kho đã đóng gói sản phẩm');
  });

  // 6) Handle logistics.shipped
  eventBus.on('logistics.shipped', async (event: AppEvent) => {
    const { order_number, tracking_number } = event.payload ?? {};
    await updateStageInReadModel(order_number, 6, 'completed', `Đã giao cho đối tác vận chuyển. Mã vận đơn: ${tracking_number}`);
  });

  // 7) Handle logistics.delivered
  eventBus.on('logistics.delivered', async (event: AppEvent) => {
    const { order_number } = event.payload ?? {};
    await updateStageInReadModel(order_number, 7, 'completed', 'Khách hàng đã nhận được hàng');
  });

  // 8) Handle payment.reconciled
  eventBus.on('payment.reconciled', async (event: AppEvent) => {
    const { order_number, payment_amount, transaction_id } = event.payload ?? {};
    await updateStageInReadModel(order_number, 8, 'completed', `Đã nhận tiền và đối soát tự động: ${Number(payment_amount).toLocaleString('vi-VN')} đ. Giao dịch ngân hàng: ${transaction_id}`);
  });

  // 9) Handle order.closed
  eventBus.on('order.closed', async (event: AppEvent) => {
    const { order_number } = event.payload ?? {};
    await updateStageInReadModel(order_number, 9, 'completed', 'Hồ sơ đơn hàng đã đóng hoàn tất.');
  });

  // 10) Handle order.stuck_toggled
  eventBus.on('order.stuck_toggled', async (event: AppEvent) => {
    const { order_number, is_stuck, reason } = event.payload ?? {};
    const { rows } = await pool.query('SELECT current_stage FROM order_lifecycle_view WHERE order_number = $1', [order_number]);
    if (rows.length > 0) {
      const currentStage = rows[0].current_stage;
      if (is_stuck) {
        await updateStageInReadModel(order_number, currentStage, 'stuck', `Sự cố: ${reason}`);
      } else {
        await updateStageInReadModel(order_number, currentStage, 'active', 'Đã giải quyết sự cố, tiếp tục quy trình.');
      }
    }
  });

  // 11) Handle order.returned
  eventBus.on('order.returned', async (event: AppEvent) => {
    const { order_number, reason } = event.payload ?? {};
    const { rows } = await pool.query('SELECT current_stage FROM order_lifecycle_view WHERE order_number = $1', [order_number]);
    if (rows.length > 0) {
      const currentStage = rows[0].current_stage;
      await updateStageInReadModel(order_number, currentStage, 'returned', `Kích hoạt luồng Trả hàng (D-053). Lý do: ${reason}`);
    }
  });
}
