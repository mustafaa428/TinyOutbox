namespace TinyOutbox.Core;

public enum OutboxStatus : byte
{
    Pending = 0,
    Processing = 1,
    Completed = 2,
    Failed = 3
}