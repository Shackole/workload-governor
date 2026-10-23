/**
 * tests/integration/event_deduplication_race.test.ts
 *
 * Integration test for concurrent ingestion worker event deduplication (#1023).
 *
 * Acceptance Criteria verified:
 *  - Use ON CONFLICT (tx_hash, event_idx) DO NOTHING in event persistence queries.
 *  - Add integration test reproducing concurrent worker insertion across parallel
 *    workers processing adjacent / overlapping ledger batches.
 */

import { PoolClient } from 'pg';
import { OrgEvent, saveEvent, saveDeduplicatedEvent } from '../../src/db';

interface StoredEventRow extends OrgEvent {
  event_idx: number;
}

describe('Concurrent Ingestion Worker Event Deduplication (#1023)', () => {
  let eventsTable: Map<string, StoredEventRow>;
  let executedSqlStatements: string[];

  /**
   * Simulates PostgreSQL atomic unique constraint enforcement on
   * `events (tx_hash, event_idx)` with `ON CONFLICT (tx_hash, event_idx) DO NOTHING`.
   */
  const mockConcurrentDbClient = {
    query: jest.fn(async (sql: string, params?: unknown[]) => {
      executedSqlStatements.push(sql);

      // Simulate variable network / lock scheduling jitter under concurrent workers
      await new Promise((resolve) => setTimeout(resolve, Math.floor(Math.random() * 5)));

      if (sql.includes('INSERT INTO events')) {
        const [org_id, event_type, issue_id, contributor, tx_hash, event_idx, occurred_at] =
          params as [
            string,
            OrgEvent['event_type'],
            string,
            string,
            string,
            number,
            Date,
          ];

        const compositeKey = `${tx_hash}:${event_idx}`;
        const hasConflictClause = /ON\s+CONFLICT\s*\(\s*tx_hash\s*,\s*event_idx\s*\)\s*DO\s+NOTHING/i.test(
          sql,
        );

        if (eventsTable.has(compositeKey)) {
          if (!hasConflictClause) {
            throw new Error(
              `duplicate key value violates unique constraint "events_tx_hash_event_idx_key" (${compositeKey})`,
            );
          }
          return { rows: [], rowCount: 0 };
        }

        const row: StoredEventRow = {
          org_id,
          event_type,
          issue_id,
          contributor,
          tx_hash,
          event_idx,
          occurred_at,
        };
        eventsTable.set(compositeKey, row);
        return { rows: [row], rowCount: 1 };
      }

      return { rows: [], rowCount: 0 };
    }),
  };

  beforeEach(() => {
    eventsTable = new Map<string, StoredEventRow>();
    executedSqlStatements = [];
    jest.clearAllMocks();
  });

  it('uses ON CONFLICT (tx_hash, event_idx) DO NOTHING in saveEvent queries', async () => {
    const event: OrgEvent = {
      org_id: 'org_stellar',
      event_type: 'applied',
      issue_id: '42',
      contributor: 'GCONTRIBUTOR111111111111111111111111111111111111111111111',
      tx_hash: '0xabc123def456',
      event_idx: 2,
      occurred_at: new Date('2026-10-23T00:00:00Z'),
    };

    await saveEvent(event, mockConcurrentDbClient as unknown as PoolClient);

    expect(mockConcurrentDbClient.query).toHaveBeenCalledTimes(1);
    const [sql, params] = mockConcurrentDbClient.query.mock.calls[0] as [string, unknown[]];
    expect(sql).toMatch(/ON\s+CONFLICT\s*\(\s*tx_hash\s*,\s*event_idx\s*\)\s*DO\s+NOTHING/i);
    expect(params).toEqual([
      'org_stellar',
      'applied',
      '42',
      'GCONTRIBUTOR111111111111111111111111111111111111111111111',
      '0xabc123def456',
      2,
      event.occurred_at,
    ]);
  });

  it('prevents duplicate rows and race errors when 4 parallel ingestion workers process overlapping ledger batches', async () => {
    // Build a set of 15 unique events spanning ledgers 1000..1014, where boundary
    // events are shared across adjacent worker batches
    const createBatchEvents = (startIdx: number, count: number): OrgEvent[] =>
      Array.from({ length: count }, (_, offset) => {
        const idx = startIdx + offset;
        const txNum = Math.floor(idx / 2);
        const eventIdx = idx % 2;
        return {
          org_id: 'org_governor',
          event_type: idx % 3 === 0 ? 'assigned' : 'applied',
          issue_id: `issue_${100 + idx}`,
          contributor: `GWORKER_TEST_CONTRIBUTOR_${idx}`,
          tx_hash: `tx_hash_${txNum.toString(16).padStart(64, '0')}`,
          event_idx: eventIdx,
          occurred_at: new Date(1760000000000 + idx * 1000),
        };
      });

    // 4 concurrent workers with overlapping boundary windows:
    // Worker 1: events 0..5
    // Worker 2: events 3..9   (overlaps 3, 4, 5 with Worker 1)
    // Worker 3: events 7..12  (overlaps 7, 8, 9 with Worker 2)
    // Worker 4: events 0..14  (full replay worker racing with Workers 1-3)
    const worker1Batch = createBatchEvents(0, 6);
    const worker2Batch = createBatchEvents(3, 7);
    const worker3Batch = createBatchEvents(7, 6);
    const worker4Batch = createBatchEvents(0, 15);

    const runWorker = async (batch: OrgEvent[]) => {
      let insertedCount = 0;
      let skippedCount = 0;
      for (const ev of batch) {
        const inserted = await saveDeduplicatedEvent(
          ev,
          mockConcurrentDbClient as unknown as PoolClient,
        );
        if (inserted) insertedCount++;
        else skippedCount++;
      }
      return { insertedCount, skippedCount };
    };

    // Launch all 4 workers concurrently
    const results = await Promise.all([
      runWorker(worker1Batch),
      runWorker(worker2Batch),
      runWorker(worker3Batch),
      runWorker(worker4Batch),
    ]);

    const totalInserted = results.reduce((sum, r) => sum + r.insertedCount, 0);
    const totalSkipped = results.reduce((sum, r) => sum + r.skippedCount, 0);
    const totalAttempted =
      worker1Batch.length + worker2Batch.length + worker3Batch.length + worker4Batch.length;

    // Exactly 15 unique (tx_hash, event_idx) pairs exist across all 34 attempts
    expect(totalAttempted).toBe(34);
    expect(totalInserted).toBe(15);
    expect(totalSkipped).toBe(19);
    expect(eventsTable.size).toBe(15);

    // Every executed query used ON CONFLICT (tx_hash, event_idx) DO NOTHING
    for (const sql of executedSqlStatements) {
      expect(sql).toMatch(/ON\s+CONFLICT\s*\(\s*tx_hash\s*,\s*event_idx\s*\)\s*DO\s+NOTHING/i);
    }
  });
});
