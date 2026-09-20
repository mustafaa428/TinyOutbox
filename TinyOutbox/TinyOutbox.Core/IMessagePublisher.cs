namespace TinyOutbox.Core;

public interface IMessagePublisher
{
    Task PublishAsync(OutboxMessage message, CancellationToken ct = default);
}