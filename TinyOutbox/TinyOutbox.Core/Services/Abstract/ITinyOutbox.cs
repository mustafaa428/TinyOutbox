using System.Text.Json;

namespace TinyOutbox.Core.Services.Abstract;

public interface ITinyOutbox
{
    Task PublishAsync<TEvent>(TEvent @event, object? dbTransaction = null, CancellationToken ct = default)
        where TEvent : class;
}

