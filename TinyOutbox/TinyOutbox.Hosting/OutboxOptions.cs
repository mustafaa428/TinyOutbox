namespace TinyOutbox.Hosting;

public class OutboxOptions
{
    public int BatchSize { get; set; } = 50;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(500);
    public int MaxRetryCount { get; set; } = 5;
}