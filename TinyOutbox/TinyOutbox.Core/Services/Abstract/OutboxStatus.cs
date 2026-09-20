namespace TinyOutbox.Core.Services.Abstract;

public enum OutboxStatus : byte
{
    Pending = 0,
    Processing = 1,
    Completed = 2,
    Failed = 3
}