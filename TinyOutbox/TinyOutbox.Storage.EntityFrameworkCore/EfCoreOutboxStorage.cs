using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using TinyOutbox.Core;

namespace TinyOutbox.Storage.EntityFrameworkCore;

public class EfCoreOutboxStorage<TDbContext> : IOutboxStorage where TDbContext : DbContext
{
    private readonly IServiceProvider _serviceProvider;
    private readonly string _workerInstanceId;

    public EfCoreOutboxStorage(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
        _workerInstanceId = $"{Environment.MachineName}-{Guid.NewGuid():N}";
    }

    public async Task EnqueueAsync(OutboxMessage message, object? dbTransaction = null, CancellationToken cancellationToken = default)
    {
        if (dbTransaction is TDbContext externalContext)
        {
            await externalContext.Set<OutboxMessage>().AddAsync(message, cancellationToken);
            return;
        }

        using var scope = _serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TDbContext>();

        await context.Set<OutboxMessage>().AddAsync(message, cancellationToken);
        await context.SaveChangesAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<OutboxMessage>> FetchPendingMessagesAsync(int batchSize, CancellationToken cancellationToken = default)
    {
        using var scope = _serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TDbContext>();
        var now = DateTime.UtcNow;
        var lockExpiration = now.AddMinutes(2);

        // 1. İşlenmeyi bekleyen aday ID'leri tespit et
        var candidateIds = await context.Set<OutboxMessage>()
            .Where(x => x.Status == OutboxStatus.Pending
                        && x.ScheduledAtUtc <= now
                        && (x.LockExpiresAtUtc == null || x.LockExpiresAtUtc < now))
            .OrderBy(x => x.ScheduledAtUtc)
            .Take(batchSize)
            .Select(x => x.Id)
            .ToListAsync(cancellationToken);

        if (candidateIds.Count == 0)
        {
            return Array.Empty<OutboxMessage>();
        }

        // 2. Aday kayıtları bu instance adına kilitle (Eşzamanlı pod çakışmasını önler)
        await context.Set<OutboxMessage>()
            .Where(x => candidateIds.Contains(x.Id) && (x.LockExpiresAtUtc == null || x.LockExpiresAtUtc < now))
            .ExecuteUpdateAsync(s => s
                .SetProperty(b => b.LockedBy, _workerInstanceId)
                .SetProperty(b => b.LockExpiresAtUtc, lockExpiration), cancellationToken);

        // 3. Sadece bu instance'ın başarıyla kilitlediği kayıtları getir
        return await context.Set<OutboxMessage>()
            .AsNoTracking()
            .Where(x => candidateIds.Contains(x.Id) && x.LockedBy == _workerInstanceId)
            .ToListAsync(cancellationToken);
    }

    public async Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        using var scope = _serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TDbContext>();

        await context.Set<OutboxMessage>()
            .Where(x => x.Id == messageId)
            .ExecuteUpdateAsync(s => s
                .SetProperty(b => b.Status, OutboxStatus.Completed)
                .SetProperty(b => b.ProcessedAtUtc, DateTime.UtcNow)
                .SetProperty(b => b.LockedBy, (string?)null)
                .SetProperty(b => b.LockExpiresAtUtc, (DateTime?)null), cancellationToken);
    }
    public async Task MarkAsFailedAsync(Guid messageId, string error, DateTime nextRetryUtc, CancellationToken cancellationToken = default)
    {
        using var scope = _serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TDbContext>();

        await context.Set<OutboxMessage>()
            .Where(x => x.Id == messageId)
            .ExecuteUpdateAsync(s => s
                .SetProperty(b => b.RetryCount, b => b.RetryCount + 1)
                .SetProperty(b => b.LastError, error)
                .SetProperty(b => b.ScheduledAtUtc, nextRetryUtc)
                .SetProperty(b => b.Status, OutboxStatus.Pending)
                .SetProperty(b => b.LockedBy, (string?)null)
                .SetProperty(b => b.LockExpiresAtUtc, (DateTime?)null), cancellationToken);
    }

    public async Task<int> CleanupOldMessagesAsync(DateTime processedThresholdUtc, DateTime failedThresholdUtc, int batchSize, CancellationToken cancellationToken = default)
    {
        using var scope = _serviceProvider.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<TDbContext>();

        var deletedCompleted = await context.Set<OutboxMessage>()
            .Where(x => x.Status == OutboxStatus.Completed && x.ProcessedAtUtc != null && x.ProcessedAtUtc < processedThresholdUtc)
            .Take(batchSize)
            .ExecuteDeleteAsync(cancellationToken);

        var deletedFailed = await context.Set<OutboxMessage>()
            .Where(x => x.Status == OutboxStatus.Failed && x.CreatedAtUtc < failedThresholdUtc)
            .Take(batchSize)
            .ExecuteDeleteAsync(cancellationToken);

        return deletedCompleted + deletedFailed;
    }
}