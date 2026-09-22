using Dapper;
using Npgsql;
using TinyOutbox.Core;

namespace TinyOutbox.Storage.PostgreSql;

public class PostgreSqlOutboxStorage : IOutboxStorage
{
    private readonly string _connectionString;
    private readonly string _tableName;

    public PostgreSqlOutboxStorage(string connectionString, string tableName = "tiny_outbox_messages")
    {
        _connectionString = connectionString;
        _tableName = tableName;
    }

    public PostgreSqlOutboxStorage(PostgreSqlOutboxOptions options)
        : this(options.ConnectionString, options.TableName)
    {
    }

    public async Task EnqueueAsync(OutboxMessage message, object? dbTransaction = null, CancellationToken cancellationToken = default)
    {
        var sql = $@"
            INSERT INTO {_tableName} (id, event_type, payload, created_at_utc, scheduled_at_utc, status, retry_count)
            VALUES (@Id, @EventType, @Payload, @CreatedAtUtc, @ScheduledAtUtc, @Status, @RetryCount);";

        var parameters = new
        {
            message.Id,
            message.EventType,
            message.Payload,
            message.CreatedAtUtc,
            message.ScheduledAtUtc,
            Status = (short)message.Status,
            message.RetryCount
        };

        if (dbTransaction is NpgsqlTransaction tx)
        {
            await tx.Connection!.ExecuteAsync(new CommandDefinition(
                sql,
                parameters,
                transaction: tx,
                cancellationToken: cancellationToken));
            return;
        }

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await connection.ExecuteAsync(new CommandDefinition(
            sql,
            parameters,
            cancellationToken: cancellationToken));
    }

    public async Task<IReadOnlyList<OutboxMessage>> FetchPendingMessagesAsync(int batchSize, CancellationToken cancellationToken = default)
    {
        var sql = $@"
            SELECT 
                id AS Id,
                event_type AS EventType,
                payload AS Payload,
                created_at_utc AS CreatedAtUtc,
                scheduled_at_utc AS ScheduledAtUtc,
                processed_at_utc AS ProcessedAtUtc,
                retry_count AS RetryCount,
                last_error AS LastError,
                status AS Status
            FROM {_tableName}
            WHERE status = {(short)OutboxStatus.Pending} AND scheduled_at_utc <= @Now
            ORDER BY scheduled_at_utc ASC
            LIMIT @BatchSize
            FOR UPDATE SKIP LOCKED;";

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var messages = await connection.QueryAsync<OutboxMessage>(new CommandDefinition(
            sql,
            new { Now = DateTime.UtcNow, BatchSize = batchSize },
            cancellationToken: cancellationToken));

        return messages.ToList();
    }

    public async Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var sql = $@"
            UPDATE {_tableName}
            SET status = {(short)OutboxStatus.Completed},
                processed_at_utc = @ProcessedAtUtc,
                last_error = NULL
            WHERE id = @Id;";

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await connection.ExecuteAsync(new CommandDefinition(
            sql,
            new { Id = messageId, ProcessedAtUtc = DateTime.UtcNow },
            cancellationToken: cancellationToken));
    }

    public async Task MarkAsFailedAsync(Guid messageId, string error, DateTime nextRetryUtc, CancellationToken cancellationToken = default)
    {
        var sql = $@"
            UPDATE {_tableName}
            SET retry_count = retry_count + 1,
                last_error = @Error,
                scheduled_at_utc = @NextRetryUtc,
                status = {(short)OutboxStatus.Pending}
            WHERE id = @Id;";

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await connection.ExecuteAsync(new CommandDefinition(
            sql,
            new { Id = messageId, Error = error, NextRetryUtc = nextRetryUtc },
            cancellationToken: cancellationToken));
    }

    public async Task<int> CleanupOldMessagesAsync(DateTime processedThresholdUtc, DateTime failedThresholdUtc, int batchSize, CancellationToken cancellationToken = default)
    {
        var sql = $@"
            WITH to_delete AS (
                SELECT id FROM {_tableName}
                WHERE (status = {(short)OutboxStatus.Completed} AND processed_at_utc < @ProcessedThreshold)
                   OR (status = {(short)OutboxStatus.Failed} AND created_at_utc < @FailedThreshold)
                LIMIT @BatchSize
            )
            DELETE FROM {_tableName}
            WHERE id IN (SELECT id FROM to_delete);";

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        return await connection.ExecuteAsync(new CommandDefinition(
            sql,
            new
            {
                ProcessedThreshold = processedThresholdUtc,
                FailedThreshold = failedThresholdUtc,
                BatchSize = batchSize
            },
            cancellationToken: cancellationToken));
    }
}