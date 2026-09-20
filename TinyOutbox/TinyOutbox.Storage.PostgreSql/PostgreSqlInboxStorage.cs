using System.Data.Common;
using Dapper;
using Npgsql;
using TinyOutbox.Core.Services.Abstract;

namespace TinyOutbox.Storage.PostgreSql;

public class PostgreSqlInboxStorage : IInboxStorage
{
    private readonly PostgreSqlOutboxOptions _options;

    public PostgreSqlInboxStorage(PostgreSqlOutboxOptions options)
    {
        _options = options;
    }

    public async Task<bool> HasProcessedAsync(Guid messageId, CancellationToken ct = default)
    {
        var sql = $"SELECT EXISTS(SELECT 1 FROM {_options.InboxTableName} WHERE message_id = @MessageId);";

        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        return await conn.ExecuteScalarAsync<bool>(new CommandDefinition(sql, new { MessageId = messageId }, cancellationToken: ct));
    }

    public async Task MarkAsProcessedAsync(Guid messageId, string eventType, object? dbTransaction = null, CancellationToken ct = default)
    {
        // ON CONFLICT DO NOTHING ile çakışmaları yutuyoruz
        var sql = $@"
            INSERT INTO {_options.InboxTableName} (message_id, event_type, received_at_utc)
            VALUES (@MessageId, @EventType, NOW())
            ON CONFLICT (message_id) DO NOTHING;";

        var parameters = new { MessageId = messageId, EventType = eventType };

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
}