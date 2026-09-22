using Microsoft.EntityFrameworkCore;
using TinyOutbox.Core;

namespace TinyOutbox.Storage.EntityFrameworkCore;

public static class ModelBuilderExtensions
{
    public static ModelBuilder ApplyTinyOutbox(this ModelBuilder modelBuilder, string outboxTableName = "tiny_outbox_messages")
    {
        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            entity.ToTable(outboxTableName);
            entity.HasKey(e => e.Id);

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.EventType).HasColumnName("event_type").HasMaxLength(500).IsRequired();
            entity.Property(e => e.Payload).HasColumnName("payload").IsRequired();
            entity.Property(e => e.Status).HasColumnName("status").IsRequired();
            entity.Property(e => e.RetryCount).HasColumnName("retry_count");
            entity.Property(e => e.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.Property(e => e.ScheduledAtUtc).HasColumnName("scheduled_at_utc");
            entity.Property(e => e.ProcessedAtUtc).HasColumnName("processed_at_utc");
            entity.Property(e => e.LastError).HasColumnName("last_error");
            entity.Property(e => e.LockedBy).HasColumnName("locked_by").HasMaxLength(150);
            entity.Property(e => e.LockExpiresAtUtc).HasColumnName("lock_expires_at_utc");

            entity.HasIndex(e => new { e.Status, e.ScheduledAtUtc, e.LockExpiresAtUtc });
        });

        return modelBuilder;
    }
}