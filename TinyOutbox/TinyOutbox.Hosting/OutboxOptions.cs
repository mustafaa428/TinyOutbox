namespace TinyOutbox.Hosting;

public class OutboxOptions
{
    public int BatchSize { get; set; } = 50;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(500);
    public int MaxRetryCount { get; set; } = 5;

    // --- Tablo Şişmesini Önleyen Retention Ayarları ---
    /// Eski işlenmiş ve hatalı mesajların otomatik temizlenmesini etkinleştirir.
    public bool EnableCleanup { get; set; } = true;

    /// Temizlik işinin çalışma sıklığı (Örn: Haftada bir, günde bir). Varsayılan: 24 saat.
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(24);

    /// İşlenmiş (Processed) mesajların silinmeden önce tabloda tutulacağı süre. Varsayılan: 7 gün.
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(7);

    /// Hatalı (Failed) mesajların silinmeden önce tabloda tutulacağı süre. Varsayılan: 30 gün.
    public TimeSpan FailedRetentionPeriod { get; set; } = TimeSpan.FromDays(30);

    /// Veritabanını kilitlememek adına tek bir döngüde silinecek kayıt sınırı.
    public int CleanupBatchSize { get; set; } = 1000;
}