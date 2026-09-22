namespace TinyOutbox.Core;

public class OutboxMessage
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string EventType { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime ScheduledAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime? ProcessedAtUtc { get; set; }
    public int RetryCount { get; set; }
    public string? LastError { get; set; }
    public OutboxStatus Status { get; set; } = OutboxStatus.Pending;

    // Generic EF Core Concurrency Lock için:
    public string? LockedBy { get; set; }
    public DateTime? LockExpiresAtUtc { get; set; }
}