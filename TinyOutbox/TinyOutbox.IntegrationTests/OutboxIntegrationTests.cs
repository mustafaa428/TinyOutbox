using Dapper;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json.Nodes;
using TinyOutbox.Core;
using TinyOutbox.Core.Services.Abstract;
using TinyOutbox.Core.Services.Concrate;
using TinyOutbox.Hosting;
using TinyOutbox.Storage.EntityFrameworkCore;
using TinyOutbox.Storage.PostgreSql;
using TinyOutbox.Transport.RabbitMQ;
using Xunit;

namespace TinyOutbox.IntegrationTests;

public class OutboxIntegrationTests : IClassFixture<OutboxTestFixture>
{
    private readonly OutboxTestFixture _fixture;

    public OutboxIntegrationTests(OutboxTestFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task EnsureTablesCreatedAsync(string connectionString)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        const string sql = @"
            CREATE TABLE IF NOT EXISTS tiny_outbox_messages (
                id UUID PRIMARY KEY,
                event_type VARCHAR(500) NOT NULL,
                payload TEXT NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL,
                scheduled_at_utc TIMESTAMPTZ NOT NULL,
                processed_at_utc TIMESTAMPTZ,
                retry_count INT NOT NULL DEFAULT 0,
                last_error TEXT,
                status SMALLINT NOT NULL DEFAULT 0,
                locked_by VARCHAR(150),
                lock_expires_at_utc TIMESTAMPTZ
            );

            CREATE TABLE IF NOT EXISTS tiny_inbox_messages (
                message_id UUID PRIMARY KEY,
                event_type VARCHAR(500) NOT NULL,
                received_at_utc TIMESTAMPTZ NOT NULL,
                processed_at_utc TIMESTAMPTZ
            );";

        await conn.ExecuteAsync(sql);
    }
    [Fact]
    public async Task OutboxWorker_Should_PublishMessageToRabbitMq_And_MarkAsCompletedInDatabase()
    {
        // 1. Arrange
        var dbConnectionString = _fixture.Postgres.GetConnectionString();
        await EnsureTablesCreatedAsync(dbConnectionString);

        var rabbitPort = _fixture.RabbitMq.GetMappedPublicPort(5672);

        var storageOptions = new PostgreSqlOutboxOptions
        {
            ConnectionString = dbConnectionString,
            TableName = "tiny_outbox_messages",
            AutoMigrate = true
        };
        var storage = new PostgreSqlOutboxStorage(storageOptions.ConnectionString, storageOptions.TableName);

        var rabbitOptions = new RabbitMqOutboxOptions
        {
            HostName = _fixture.RabbitMq.Hostname,
            Port = rabbitPort,
            UserName = "guest",
            Password = "guest",
            ExchangeName = "tiny.test.exchange",
            ExchangeType = "topic"
        };
        var publisher = new RabbitMqMessagePublisher(rabbitOptions);

        var outboxOptions = new OutboxOptions
        {
            BatchSize = 10,
            PollingInterval = TimeSpan.FromMilliseconds(100),
            MaxRetryCount = 3
        };

        var worker = new OutboxBackgroundWorker(
            storage,
            publisher,
            outboxOptions,
            NullLogger<OutboxBackgroundWorker>.Instance
        );

        // RabbitMQ consumer hazırla
        var factory = new ConnectionFactory
        {
            HostName = _fixture.RabbitMq.Hostname,
            Port = rabbitPort,
            UserName = "guest",
            Password = "guest"
        };
        await using var connection = await factory.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync("tiny.test.exchange", "topic", durable: true, autoDelete: false);
        var queue = await channel.QueueDeclareAsync(queue: "", exclusive: true, autoDelete: true);
        await channel.QueueBindAsync(queue.QueueName, "tiny.test.exchange", "#");

        var receivedMessageTcs = new TaskCompletionSource<string>();
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (model, ea) =>
        {
            var body = Encoding.UTF8.GetString(ea.Body.ToArray());
            receivedMessageTcs.TrySetResult(body);
            return Task.CompletedTask;
        };
        await channel.BasicConsumeAsync(queue.QueueName, autoAck: true, consumer: consumer);

        // 2. Act
        var messageId = Guid.NewGuid();
        var payloadJson = "{\"OrderId\":12345,\"TotalAmount\":99.90}";

        var testMessage = new OutboxMessage
        {
            Id = messageId,
            EventType = "OrderCreatedEvent",
            Payload = payloadJson,
            CreatedAtUtc = DateTime.UtcNow,
            ScheduledAtUtc = DateTime.UtcNow,
            Status = OutboxStatus.Pending
        };

        await storage.EnqueueAsync(testMessage, dbTransaction: null, CancellationToken.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var workerTask = worker.StartAsync(cts.Token);

        // 3. Assert
        var receivedPayload = await receivedMessageTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        JsonNode.DeepEquals(JsonNode.Parse(receivedPayload), JsonNode.Parse(payloadJson)).Should().BeTrue();

        await worker.StopAsync(CancellationToken.None);

        await using var verifyConn = new NpgsqlConnection(dbConnectionString);
        var status = await verifyConn.QuerySingleAsync<short>(
            "SELECT status FROM tiny_outbox_messages WHERE id = @Id",
            new { Id = messageId }
        );

        status.Should().Be((short)OutboxStatus.Completed);
    }

    [Fact]
    public async Task TinyInbox_Should_ProcessMessageOnlyOnce_When_DuplicateMessagesReceived()
    {
        // 1. Arrange
        var dbConnectionString = _fixture.Postgres.GetConnectionString();
        await EnsureTablesCreatedAsync(dbConnectionString);

        var options = new PostgreSqlOutboxOptions
        {
            ConnectionString = dbConnectionString,
            InboxTableName = "tiny_inbox_messages",
            AutoMigrate = true
        };

        var inboxStorage = new PostgreSqlInboxStorage(options);
        var inboxService = new TinyInboxService(inboxStorage);

        var duplicateMessageId = Guid.NewGuid();
        const string eventType = "PaymentReceivedEvent";
        var executionCount = 0;

        Task BusinessLogicAsync()
        {
            Interlocked.Increment(ref executionCount);
            return Task.CompletedTask;
        }

        // 2. Act
        var firstCallResult = await inboxService.ExecuteAsync(duplicateMessageId, eventType, BusinessLogicAsync);
        var secondCallResult = await inboxService.ExecuteAsync(duplicateMessageId, eventType, BusinessLogicAsync);

        // 3. Assert
        firstCallResult.Should().BeTrue();
        secondCallResult.Should().BeFalse();
        executionCount.Should().Be(1);

        var hasProcessed = await inboxStorage.HasProcessedAsync(duplicateMessageId);
        hasProcessed.Should().BeTrue();
    }

    [Fact]
    public async Task EfCoreStorage_Should_FetchPending_LockRecord_And_CleanupOldMessages()
    {
        // 1. Arrange: TestDbContext yapılandırması
        var dbConnectionString = _fixture.Postgres.GetConnectionString();

        var services = new ServiceCollection();
        services.AddDbContext<TestDbContext>(options =>
            options.UseNpgsql(dbConnectionString));

        var serviceProvider = services.BuildServiceProvider();

        // Veritabanı tablosunu oluştur
        using (var scope = serviceProvider.CreateScope())
        {
            var ctx = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            await ctx.Database.EnsureCreatedAsync();
        }

        var efStorage = new EfCoreOutboxStorage<TestDbContext>(serviceProvider);

        // 2. Act: Yeni mesaj ekle
        var messageId = Guid.NewGuid();
        var message = new OutboxMessage
        {
            Id = messageId,
            EventType = "UserRegisteredEvent",
            Payload = "{\"UserId\":\"usr_99\"}",
            CreatedAtUtc = DateTime.UtcNow,
            ScheduledAtUtc = DateTime.UtcNow,
            Status = OutboxStatus.Pending
        };

        await efStorage.EnqueueAsync(message, null, CancellationToken.None);

        // Mesajları çek
        var pendingList = await efStorage.FetchPendingMessagesAsync(10, CancellationToken.None);

        // 3. Assert: Kilit kontrolü
        pendingList.Should().ContainSingle(x => x.Id == messageId);
        var lockedMessage = pendingList.First(x => x.Id == messageId);
        lockedMessage.LockedBy.Should().NotBeNullOrWhiteSpace();
        lockedMessage.LockExpiresAtUtc.Should().BeAfter(DateTime.UtcNow);

        // İşlendi (Completed) olarak işaretle
        await efStorage.MarkAsProcessedAsync(messageId, CancellationToken.None);

        using (var scope = serviceProvider.CreateScope())
        {
            var ctx = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var saved = await ctx.OutboxMessages.FindAsync(messageId);
            saved.Should().NotBeNull();
            saved!.Status.Should().Be(OutboxStatus.Completed);
            saved.LockedBy.Should().BeNull();
            saved.LockExpiresAtUtc.Should().BeNull();
        }

        // 4. Cleanup Testi: ExecuteUpdateAsync ile tarihi geçmişe çekip Cleanup metodunu test et
        using (var scope = serviceProvider.CreateScope())
        {
            var ctx = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var oldDate = DateTime.UtcNow.AddDays(-10);
            await ctx.OutboxMessages
                .Where(x => x.Id == messageId)
                .ExecuteUpdateAsync(s => s.SetProperty(b => b.ProcessedAtUtc, oldDate));
        }

        var deletedCount = await efStorage.CleanupOldMessagesAsync(
            processedThresholdUtc: DateTime.UtcNow.AddDays(-7),
            failedThresholdUtc: DateTime.UtcNow.AddDays(-30),
            batchSize: 100,
            cancellationToken: CancellationToken.None);

        deletedCount.Should().Be(1);

        using (var scope = serviceProvider.CreateScope())
        {
            var ctx = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var deletedRecord = await ctx.OutboxMessages.FindAsync(messageId);
            deletedRecord.Should().BeNull();
        }
    }
}