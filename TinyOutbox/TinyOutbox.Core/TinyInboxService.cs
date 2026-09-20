namespace TinyOutbox.Core;

public class TinyInboxService : ITinyInbox
{
    private readonly IInboxStorage _storage;

    public TinyInboxService(IInboxStorage storage)
    {
        _storage = storage;
    }

    public Task<bool> HasProcessedAsync(Guid messageId, CancellationToken ct = default)
    {
        return _storage.HasProcessedAsync(messageId, ct);
    }

    public async Task<bool> ExecuteAsync(Guid messageId, string eventType, Func<Task> action, CancellationToken ct = default)
    {
        if (await _storage.HasProcessedAsync(messageId, ct))
        {
            return false; // Zaten işlenmiş, tekrar çalıştırma
        }

        await action();
        await _storage.MarkAsProcessedAsync(messageId, eventType, null, ct);
        return true;
    }
}