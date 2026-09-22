using Microsoft.EntityFrameworkCore;
using TinyOutbox.Core;
using TinyOutbox.Storage.EntityFrameworkCore;

namespace TinyOutbox.IntegrationTests;

public class TestDbContext : DbContext
{
    public TestDbContext(DbContextOptions<TestDbContext> options) : base(options) { }

    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.ApplyTinyOutbox("ef_outbox_messages");
    }
}