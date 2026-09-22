namespace TinyOutbox.Core;

public interface IOutboxStorage
{
    Task EnqueueAsync(OutboxMessage message, object? dbTransaction = null, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<OutboxMessage>> FetchPendingMessagesAsync(int batchSize, CancellationToken cancellationToken = default);
    Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default);
    Task MarkAsFailedAsync(Guid messageId, string error, DateTime nextRetryUtc, CancellationToken cancellationToken = default);
    Task<int> CleanupOldMessagesAsync(DateTime processedThresholdUtc, DateTime failedThresholdUtc, int batchSize, CancellationToken cancellationToken = default);
}