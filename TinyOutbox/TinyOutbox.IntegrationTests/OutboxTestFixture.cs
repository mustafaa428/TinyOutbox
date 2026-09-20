using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using Xunit;

namespace TinyOutbox.IntegrationTests;

public class OutboxTestFixture : IAsyncLifetime
{
    public PostgreSqlContainer Postgres { get; } = new PostgreSqlBuilder("postgres:16-alpine")
        .WithDatabase("test_db")
        .WithUsername("test_user")
        .WithPassword("test_password")
        .Build();

    public RabbitMqContainer RabbitMq { get; } = new RabbitMqBuilder("rabbitmq:3-management-alpine")
        .WithUsername("guest")
        .WithPassword("guest")
        .Build();

    public async Task InitializeAsync()
    {
        await Task.WhenAll(Postgres.StartAsync(), RabbitMq.StartAsync());
    }

    public async Task DisposeAsync()
    {
        await Task.WhenAll(Postgres.DisposeAsync().AsTask(), RabbitMq.DisposeAsync().AsTask());
    }
}