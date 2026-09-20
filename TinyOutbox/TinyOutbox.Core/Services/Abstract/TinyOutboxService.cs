using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace TinyOutbox.Core.Services.Abstract;

public class TinyOutboxService : ITinyOutbox
{
    private readonly IOutboxStorage _storage;

    public TinyOutboxService(IOutboxStorage storage)
    {
        _storage = storage;
    }

    public async Task PublishAsync<TEvent>(TEvent @event, object? dbTransaction = null, CancellationToken ct = default)
        where TEvent : class
    {
        var message = new OutboxMessage
        {
            EventType = typeof(TEvent).AssemblyQualifiedName ?? typeof(TEvent).FullName ?? typeof(TEvent).Name,
            Payload = JsonSerializer.Serialize(@event),
            CreatedAtUtc = DateTime.UtcNow,
            ScheduledAtUtc = DateTime.UtcNow,
            Status = OutboxStatus.Pending
        };

        await _storage.EnqueueAsync(message, dbTransaction, ct);
    }
}

