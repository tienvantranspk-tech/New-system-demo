import { pool } from './db';

export class OutboxWorker {
  private timer: NodeJS.Timeout | null = null;
  private isProcessing = false;
  private port: number;

  constructor(port = 8080) {
    this.port = port;
  }

  /**
   * Start polling the outbox table.
   */
  start() {
    console.log('[Outbox Worker] Starting background worker (polling every 4s)...');
    this.timer = setInterval(() => this.processOutbox(), 4000);
  }

  /**
   * Stop polling.
   */
  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    console.log('[Outbox Worker] Stopped background worker.');
  }

  /**
   * Process pending or failed outbox events.
   */
  async processOutbox() {
    if (this.isProcessing) return;
    this.isProcessing = true;

    try {
      // Select up to 5 events that are PENDING or FAILED with less than 5 attempts
      const { rows } = await pool.query(
        `SELECT * FROM outbox_events
         WHERE status = 'PENDING' OR (status = 'FAILED' AND attempts < 5)
         ORDER BY id ASC LIMIT 5`
      );

      if (rows.length === 0) {
        this.isProcessing = false;
        return;
      }

      console.log(`[Outbox Worker] Outbox check: Found ${rows.length} event(s) to process.`);

      for (const event of rows) {
        await this.syncEventWithFAST(event);
      }
    } catch (err) {
      console.error('[Outbox Worker] Error in outbox processing loop:', err);
    } finally {
      this.isProcessing = false;
    }
  }

  private async syncEventWithFAST(event: any) {
    const nextAttempts = event.attempts + 1;
    console.log(`[Outbox Worker] Attempting to sync event ${event.event_id} (Attempt #${nextAttempts}) to FAST...`);

    try {
      // Call our mock FAST endpoint
      const response = await fetch(`http://localhost:${this.port}/api/fast-accounting/sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          event_id: event.event_id,
          event_type: event.event_type,
          payload: event.payload,
        }),
      });

      if (response.ok) {
        // Successful sync
        await pool.query(
          `UPDATE outbox_events
           SET status = 'SENT', attempts = $1, last_attempt = now()
           WHERE id = $2`,
          [nextAttempts, event.id]
        );
        console.log(`\x1b[32m[Outbox Worker] Event ${event.event_id} synced with FAST successfully.\x1b[0m`);
      } else {
        // The mock server returned a failure code (e.g. 503 Service Unavailable)
        const errorText = await response.text();
        await pool.query(
          `UPDATE outbox_events
           SET status = 'FAILED', attempts = $1, last_attempt = now()
           WHERE id = $2`,
          [nextAttempts, event.id]
        );
        console.warn(`\x1b[31m[Outbox Worker] Event ${event.event_id} sync failed (HTTP ${response.status}: ${errorText}).\x1b[0m`);
      }
    } catch (err: any) {
      // Network error (e.g. port closed, server down)
      await pool.query(
        `UPDATE outbox_events
         SET status = 'FAILED', attempts = $1, last_attempt = now()
         WHERE id = $2`,
        [nextAttempts, event.id]
      );
      console.warn(`\x1b[31m[Outbox Worker] Event ${event.event_id} sync failed (Network error: ${err.message}).\x1b[0m`);
    }
  }
}
