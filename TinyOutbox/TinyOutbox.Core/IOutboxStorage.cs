namespace TinyOutbox.Core;

public interface IOutboxStorage
{
    // Transaction sırasında aynı DB bağlantısı/transaction nesnesi aktarılabilir
    Task EnqueueAsync(OutboxMessage message, object? dbTransaction = null, CancellationToken ct = default);

    // Arka plan worker'ı için kilitli toplu çekme
    Task<IReadOnlyList<OutboxMessage>> FetchPendingAsync(int batchSize, CancellationToken ct = default);

    // Başarıyla broker'a giden mesajı tamamlandı işaretle
    Task MarkAsCompletedAsync(Guid messageId, CancellationToken ct = default);

    // Başarısız olan mesaj için retry ve hata güncelle
    Task MarkAsFailedAsync(Guid messageId, string errorMessage, int maxRetries, CancellationToken ct = default);
}