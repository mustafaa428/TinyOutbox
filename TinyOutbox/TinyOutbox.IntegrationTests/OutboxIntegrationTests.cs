
using Dapper;
using FluentAssertions;
using global::TinyOutbox.Core;
using global::TinyOutbox.Hosting;
using global::TinyOutbox.Storage.PostgreSql;
using global::TinyOutbox.Transport.RabbitMQ;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json.Nodes;
using TinyOutbox.Core.Services.Abstract;
using Xunit;

namespace TinyOutbox.IntegrationTests;

public class OutboxIntegrationTests : IClassFixture<OutboxTestFixture>
{
    private readonly OutboxTestFixture _fixture;

    public OutboxIntegrationTests(OutboxTestFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task OutboxWorker_Should_PublishMessageToRabbitMq_And_MarkAsCompletedInDatabase()
    {
        // 1. Arrange - Ayarlar ve Bileşenlerin Başlatılması
        var dbConnectionString = _fixture.Postgres.GetConnectionString();
        var rabbitPort = _fixture.RabbitMq.GetMappedPublicPort(5672);

        var storageOptions = new PostgreSqlOutboxOptions
        {
            ConnectionString = dbConnectionString,
            TableName = "tiny_outbox_messages",
            AutoMigrate = true
        };
        var storage = new PostgreSqlOutboxStorage(storageOptions);

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

        // RabbitMQ üzerinde dinleyici bir consumer açıp mesajın geldiğini dinleyelim
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

        // 2. Act - Veritabanına Outbox mesajı yaz
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

        await storage.EnqueueAsync(testMessage);

        // Worker'ı arka planda başlat
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var workerTask = worker.StartAsync(cts.Token);

        // 3. Assert - Mesajın RabbitMQ'ya düşmesini bekle
        var receivedPayload = await receivedMessageTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        JsonNode.DeepEquals(JsonNode.Parse(receivedPayload), JsonNode.Parse(payloadJson))
        .Should().BeTrue();

        // Worker'ı durdur
        await worker.StopAsync(CancellationToken.None);

        // Veritabanındaki mesajın durumunu sorgula (Completed = 2 olmalı)
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

        // İş mantığını temsil eden fonksiyon
        Task BusinessLogicAsync()
        {
            Interlocked.Increment(ref executionCount);
            return Task.CompletedTask;
        }

        // 2. Act
        // İlk mesaj geldiğinde
        var firstCallResult = await inboxService.ExecuteAsync(
            duplicateMessageId,
            eventType,
            BusinessLogicAsync
        );

        // Aynı mesaj (aynı message_id ile) tekrar geldiğinde (Retry veya ağ hatası senaryosu)
        var secondCallResult = await inboxService.ExecuteAsync(
            duplicateMessageId,
            eventType,
            BusinessLogicAsync
        );

        // 3. Assert
        // İlk çağrı başarıyla işletilmiş olmalı
        firstCallResult.Should().BeTrue();

        // İkinci çağrı idempotency kontrolüne takılıp işletilmemeli (false dönmeli)
        secondCallResult.Should().BeFalse();

        // İş mantığı toplamda kesinlikle sadece 1 defa çalışmış olmalı
        executionCount.Should().Be(1);

        // Depolamada mesajın var olduğu doğrulanmalı
        var hasProcessed = await inboxStorage.HasProcessedAsync(duplicateMessageId);
        hasProcessed.Should().BeTrue();
    }
}