namespace TinyOutbox.Core.Services.Abstract;

public class OutboxMessage
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string EventType { get; set; } = default!;
    public string Payload { get; set; } = default!;
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime ScheduledAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime? ProcessedAtUtc { get; set; }
    public int RetryCount { get; set; } = 0;
    public string? LastError { get; set; }
    public OutboxStatus Status { get; set; } = OutboxStatus.Pending;
}