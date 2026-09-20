using System.Data.Common;
using Dapper;
using Npgsql;
using TinyOutbox.Core;

namespace TinyOutbox.Storage.PostgreSql;

public class PostgreSqlOutboxStorage : IOutboxStorage
{
    private readonly PostgreSqlOutboxOptions _options;

    public PostgreSqlOutboxStorage(PostgreSqlOutboxOptions options)
    {
        _options = options;
        if (_options.AutoMigrate)
        {
            EnsureTablesCreated();
        }
    }

    private void EnsureTablesCreated()
    {
        using var connection = new NpgsqlConnection(_options.ConnectionString);
        connection.Open();

        var sql = $@"
            -- Outbox Tablosu
            CREATE TABLE IF NOT EXISTS {_options.TableName} (
                id UUID PRIMARY KEY,
                event_type VARCHAR(500) NOT NULL,
                payload JSONB NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL,
                scheduled_at_utc TIMESTAMPTZ NOT NULL,
                processed_at_utc TIMESTAMPTZ NULL,
                retry_count INT NOT NULL DEFAULT 0,
                last_error TEXT NULL,
                status SMALLINT NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_fetch 
            ON {_options.TableName} (scheduled_at_utc, status) 
            WHERE status = 0;

            -- Inbox Tablosu (Idempotency için)
            CREATE TABLE IF NOT EXISTS {_options.InboxTableName} (
                message_id UUID PRIMARY KEY,
                event_type VARCHAR(500) NOT NULL,
                received_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );";

        connection.Execute(sql);
    }

    public async Task EnqueueAsync(OutboxMessage message, object? dbTransaction = null, CancellationToken ct = default)
    {
        var sql = $@"
            INSERT INTO {_options.TableName} 
            (id, event_type, payload, created_at_utc, scheduled_at_utc, retry_count, status)
            VALUES (@Id, @EventType, @Payload::jsonb, @CreatedAtUtc, @ScheduledAtUtc, @RetryCount, @Status);";

        var parameters = new
        {
            message.Id,
            message.EventType,
            message.Payload,
            message.CreatedAtUtc,
            message.ScheduledAtUtc,
            message.RetryCount,
            Status = (short)message.Status
        };

        if (dbTransaction is DbTransaction tx)
        {
            await tx.Connection!.ExecuteAsync(new CommandDefinition(sql, parameters, transaction: tx, cancellationToken: ct));
        }
        else
        {
            await using var conn = new NpgsqlConnection(_options.ConnectionString);
            await conn.ExecuteAsync(new CommandDefinition(sql, parameters, cancellationToken: ct));
        }
    }

    public async Task<IReadOnlyList<OutboxMessage>> FetchPendingAsync(int batchSize, CancellationToken ct = default)
    {
        var sql = $@"
            WITH picked AS (
                SELECT id 
                FROM {_options.TableName}
                WHERE status = 0 AND scheduled_at_utc <= NOW()
                ORDER BY created_at_utc ASC
                LIMIT @BatchSize
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {_options.TableName} t
            SET status = 1 -- Processing
            FROM picked p
            WHERE t.id = p.id
            RETURNING t.id, t.event_type AS EventType, t.payload, t.created_at_utc AS CreatedAtUtc, 
                      t.scheduled_at_utc AS ScheduledAtUtc, t.retry_count AS RetryCount, t.status AS Status;";

        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        var result = await conn.QueryAsync<OutboxMessage>(new CommandDefinition(sql, new { BatchSize = batchSize }, cancellationToken: ct));
        return result.ToList();
    }

    public async Task MarkAsCompletedAsync(Guid messageId, CancellationToken ct = default)
    {
        var sql = $@"
            UPDATE {_options.TableName}
            SET status = 2, processed_at_utc = NOW()
            WHERE id = @Id;";

        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        await conn.ExecuteAsync(new CommandDefinition(sql, new { Id = messageId }, cancellationToken: ct));
    }

    public async Task MarkAsFailedAsync(Guid messageId, string errorMessage, int maxRetries, CancellationToken ct = default)
    {
        var sql = $@"
            UPDATE {_options.TableName}
            SET retry_count = retry_count + 1,
                last_error = @Error,
                status = CASE WHEN retry_count + 1 >= @MaxRetries THEN 3 ELSE 0 END,
                scheduled_at_utc = NOW() + (INTERVAL '2 seconds' * POWER(2, retry_count))
            WHERE id = @Id;";

        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        await conn.ExecuteAsync(new CommandDefinition(sql, new { Id = messageId, Error = errorMessage, MaxRetries = maxRetries }, cancellationToken: ct));
    }
}