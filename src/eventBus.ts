import { EventEmitter } from 'node:events';
import crypto from 'node:crypto';
import { pool } from './db';

export interface AppEvent {
  event_id: string;
  event_type: string;
  payload: any;
  published_at: string;
}

class LocalEventBus extends EventEmitter {
  constructor() {
    super();
    this.setupSubscribers();
  }

  /**
   * Publish an event asynchronously to simulate Cloud Pub/Sub latency.
   */
  publish(eventType: string, payload: any) {
    const event: AppEvent = {
      event_id: crypto.randomUUID(),
      event_type: eventType,
      payload,
      published_at: new Date().toISOString(),
    };

    console.log(`[Pub/Sub] Publishing event: ${eventType} (ID: ${event.event_id})`);

    // Simulate 100ms - 200ms of message broker delivery delay
    const latency = Math.floor(Math.random() * 100) + 100;
    setTimeout(() => {
      this.emit(eventType, event);
      // Emit wildcard for subscribers listening to all events (like Audit logger)
      this.emit('*', event);
    }, latency);
  }

  private setupSubscribers() {
    // Subscriber 1: Audit Log Subscriber
    // Persists all published events to the database (simulating a central logging store)
    this.on('*', async (event: AppEvent) => {
      try {
        console.log(`[Subscriber: AuditLog] Processing event ${event.event_type}...`);
        await pool.query(
          `INSERT INTO audit_events (event_id, event_type, payload, published_at)
           VALUES ($1, $2, $3, $4) ON CONFLICT (event_id) DO NOTHING`,
          [event.event_id, event.event_type, JSON.stringify(event.payload), event.published_at]
        );
        console.log(`[Subscriber: AuditLog] Persisted event ${event.event_id} successfully.`);
      } catch (err) {
        console.error(`[Subscriber: AuditLog] Failed to persist event:`, err);
      }
    });

    // Subscriber 2: Stock Alert Subscriber
    // Detects when item stock is low and triggers a warning log
    this.on('inventory.item.updated', (event: AppEvent) => {
      const { sku, name, quantity } = event.payload ?? {};
      const qty = Number(quantity);
      if (qty < 50) {
        console.log(`\x1b[33m[Subscriber: AlertSystem] WARNING: Low stock alert! Item "${name}" (${sku}) is down to ${qty} units.\x1b[0m`);
      }
    });
  }
}

export const eventBus = new LocalEventBus();
