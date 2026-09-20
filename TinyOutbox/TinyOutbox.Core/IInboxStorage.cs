namespace TinyOutbox.Core;

public interface IInboxStorage
{
    // Mesaj daha önce başarıyla işlendi mi?
    Task<bool> HasProcessedAsync(Guid messageId, CancellationToken ct = default);

    // Mesaj başarıyla işlendikten sonra inbox'a kaydet
    Task MarkAsProcessedAsync(Guid messageId, string eventType, object? dbTransaction = null, CancellationToken ct = default);
}

public interface ITinyInbox
{
    Task<bool> HasProcessedAsync(Guid messageId, CancellationToken ct = default);

    // Mesaj henüz işlenmediyse action'ı çalıştırır ve inbox'a kaydeder (Idempotent execution)
    Task<bool> ExecuteAsync(Guid messageId, string eventType, Func<Task> action, CancellationToken ct = default);
}