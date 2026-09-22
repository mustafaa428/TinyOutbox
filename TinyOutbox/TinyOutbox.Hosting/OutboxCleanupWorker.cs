using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TinyOutbox.Core;

namespace TinyOutbox.Hosting;

public class OutboxCleanupWorker : BackgroundService
{
    private readonly IOutboxStorage _storage;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxCleanupWorker> _logger;

    public OutboxCleanupWorker(
        IOutboxStorage storage,
        IOptions<OutboxOptions> options,
        ILogger<OutboxCleanupWorker> logger)
    {
        _storage = storage;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.EnableCleanup)
        {
            _logger.LogInformation("TinyOutbox cleanup worker is disabled.");
            return;
        }

        _logger.LogInformation(
            "TinyOutbox cleanup worker started. Interval: {Interval}, Retention: {Retention}",
            _options.CleanupInterval,
            _options.RetentionPeriod);

        using var timer = new PeriodicTimer(_options.CleanupInterval);

        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                var processedThreshold = DateTime.UtcNow - _options.RetentionPeriod;
                var failedThreshold = DateTime.UtcNow - _options.FailedRetentionPeriod;

                var deletedCount = await _storage.CleanupOldMessagesAsync(
                    processedThreshold,
                    failedThreshold,
                    _options.CleanupBatchSize,
                    stoppingToken);

                if (deletedCount > 0)
                {
                    _logger.LogInformation("TinyOutbox cleanup completed. Deleted messages: {Count}", deletedCount);
                }
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogError(ex, "An error occurred while cleaning up old outbox messages.");
            }
        }
    }
}