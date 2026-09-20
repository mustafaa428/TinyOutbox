namespace TinyOutbox.Core.Services.Abstract;

public interface IMessagePublisher
{
    Task PublishAsync(OutboxMessage message, CancellationToken ct = default);
}