using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TinyOutbox.Core;

namespace TinyOutbox.Hosting;

public class OutboxBackgroundWorker : BackgroundService
{
    private readonly IOutboxStorage _storage;
    private readonly IMessagePublisher _publisher;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxBackgroundWorker> _logger;

    public OutboxBackgroundWorker(
        IOutboxStorage storage,
        IMessagePublisher publisher,
        OutboxOptions options,
        ILogger<OutboxBackgroundWorker> logger)
    {
        _storage = storage;
        _publisher = publisher;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_options.PollingInterval);

        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                var messages = await _storage.FetchPendingAsync(_options.BatchSize, stoppingToken);

                if (messages.Count == 0) continue;

                foreach (var message in messages)
                {
                    try
                    {
                        await _publisher.PublishAsync(message, stoppingToken);
                        await _storage.MarkAsCompletedAsync(message.Id, stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to publish Outbox message {MessageId}", message.Id);
                        await _storage.MarkAsFailedAsync(message.Id, ex.Message, _options.MaxRetryCount, stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in Outbox processing cycle.");
            }
        }
    }
}