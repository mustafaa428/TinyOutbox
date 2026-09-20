<div align="center">

# 📦 TinyOutbox

**A high-performance, lightweight, and zero-boilerplate Transactional Outbox & Inbox library for .NET.**

[![Build & Test](https://github.com/mustafaa428/TinyOutbox/actions/workflows/ci.yml/badge.svg)](https://github.com/mustafaa428/TinyOutbox/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-purple.svg)](https://dotnet.microsoft.com/)

Designed for clean architectures and microservices that require bulletproof message delivery without the weight of full service bus frameworks.

[Features](#-key-features) • [Architecture](#-architecture) • [Quick Start](#-quick-start) • [Schema](#-database-schema) • [Tests](#-testing)

---

</div>

## 🎯 Why TinyOutbox?

In distributed architectures, publishing messages to a message broker during database transactions introduces the **Dual-Write Problem**. If the database commit succeeds but the message broker is unreachable, data inconsistency occurs.

**TinyOutbox** solves this cleanly using the **Transactional Outbox Pattern**:
- **Atomic Operations:** Events are written to the local database in the *exact same transaction* as your domain data.
- **Lock-Free Concurrency:** Uses PostgreSQL's native `FOR UPDATE SKIP LOCKED` so multiple background worker instances or Kubernetes pods can scale horizontally without race conditions or duplicate deliveries.
- **Consumer Idempotency:** The companion `TinyInbox` prevents double-processing when consumers encounter retries or network replays.

---

## ✨ Key Features

- **⚡ Zero Bloat:** No heavy dependencies or opinionated framework lock-in. Minimal memory and CPU allocation.
- **🔒 Concurrency Safe:** Native horizontal scaling support via `SKIP LOCKED`.
- **🔁 Resilient Retries:** Built-in exponential backoff for transient broker outages (`2^retry_count`).
- **🛡️ Idempotent Consumer (Inbox):** Built-in deduplication pattern via `ITinyInbox`.
- **🚀 Pluggable Architecture:** Storage (PostgreSQL) and Transport (RabbitMQ) layers are decoupled from the core contract.
- **🛠️ Zero-Config Setup:** Automatically generates optimized tables and indexes on boot if enabled.

---

## 🏛️ Architecture

```text
[ Your Application / CQRS Handler ]
│
├─ 1. Write domain entities + Enqueue event (Single Database Transaction)
▼
[ PostgreSQL: tiny_outbox_messages ]
│
├─ 2. Batch poll & lock pending messages (FOR UPDATE SKIP LOCKED)
▼
[ TinyOutbox.Hosting: OutboxBackgroundWorker ]
│
├─ 3. Reliable publish via persistent connection
▼
[ RabbitMQ / Message Broker ]
│
├─ 4. Deliver message to consumer
▼
[ TinyInbox: ITinyInbox.ExecuteAsync ]
│
├─ 5. Check idempotency record (tiny_inbox_messages)
└─ 6. Execute business logic ONLY IF NOT PROCESSED
```

---

## 🚀 Quick Start

### 1. Installation

Install the packages for your project needs via .NET CLI:

```bash
# Core contracts and background worker engine
dotnet add package TinyOutbox.Core
dotnet add package TinyOutbox.Hosting

# PostgreSQL storage provider (Dapper / Npgsql)
dotnet add package TinyOutbox.Storage.PostgreSql

# RabbitMQ transport provider
dotnet add package TinyOutbox.Transport.RabbitMQ
```

### 2. Dependency Injection Setup (`Program.cs`)

```csharp
using TinyOutbox.Core;
using TinyOutbox.Hosting;
using TinyOutbox.Storage.PostgreSql;
using TinyOutbox.Transport.RabbitMQ;

var builder = WebApplication.CreateBuilder(args);

// Register TinyOutbox with fluent chaining
builder.Services.AddTinyOutbox(options =>
{
    options.BatchSize = 100;
    options.PollingInterval = TimeSpan.FromMilliseconds(250);
    options.MaxRetryCount = 5;
})
.UsePostgreSql(builder.Configuration.GetConnectionString("Database")!, opt =>
{
    opt.TableName = "tiny_outbox_messages";
    opt.AutoMigrate = true; // Automatically creates tables and indexes on startup
})
.UseRabbitMQ(opt =>
{
    opt.HostName = "localhost";
    opt.Port = 5672;
    opt.UserName = "guest";
    opt.Password = "guest";
    opt.ExchangeName = "domain.events.exchange";
    opt.ExchangeType = "topic";
});

var app = builder.Build();
app.Run();
```

### 3. Producing Events (Transactional Outbox)

Attach the outbox write to your existing transactional context:

```csharp
app.MapPost("/orders", async (CreateOrderRequest request, ITinyOutbox outbox, NpgsqlDataSource dataSource) =>
{
    await using var connection = await dataSource.OpenConnectionAsync();
    await using var tx = await connection.BeginTransactionAsync();

    // 1. Save Domain Entity
    var orderId = Guid.NewGuid();
    await connection.ExecuteAsync(
        "INSERT INTO orders (id, customer_id, total) VALUES (@Id, @CustomerId, @Total);",
        new { Id = orderId, request.CustomerId, request.Total },
        tx);

    // 2. Enqueue Outbox Event (Atomic write within the same transaction)
    var orderEvent = new OrderCreatedEvent(orderId, request.CustomerId, request.Total, DateTime.UtcNow);
    await outbox.PublishAsync(orderEvent, tx);

    // 3. Commit atomically
    await tx.CommitAsync();

    return Results.Accepted($"/orders/{orderId}");
});
```

### 4. Consuming Events Idempotently (TinyInbox)

Protect consumers against at-least-once message delivery duplicates:

```csharp
public class OrderCreatedConsumer
{
    private readonly ITinyInbox _inbox;
    private readonly IStockService _stockService;
    private readonly ILogger<OrderCreatedConsumer> _logger;

    public OrderCreatedConsumer(ITinyInbox inbox, IStockService stockService, ILogger<OrderCreatedConsumer> logger)
    {
        _inbox = inbox;
        _stockService = stockService;
        _logger = logger;
    }

    public async Task OnMessageReceived(Guid messageId, OrderCreatedEvent message)
    {
        // Executes the delegate ONLY if messageId has not been processed yet
        var executed = await _inbox.ExecuteAsync(messageId, nameof(OrderCreatedEvent), async () =>
        {
            await _stockService.ReserveStockAsync(message.OrderId);
        });

        if (!executed)
        {
            _logger.LogInformation("Message {MessageId} was already processed. Skipped.", messageId);
        }
    }
}
```

---

## 🗄️ Database Schema

If `AutoMigrate = false`, apply the migration manually to your database (e.g., via EF Core, Flyway, or DbUp):

```sql
-- Outbox Table
CREATE TABLE IF NOT EXISTS tiny_outbox_messages (
    id UUID PRIMARY KEY,
    event_type VARCHAR(500) NOT NULL,
    payload JSONB NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL,
    scheduled_at_utc TIMESTAMPTZ NOT NULL,
    processed_at_utc TIMESTAMPTZ NULL,
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    status SMALLINT NOT NULL DEFAULT 0 -- 0: Pending, 1: Processing, 2: Completed, 3: Failed
);

-- Fetch Performance Index
CREATE INDEX IF NOT EXISTS idx_tiny_outbox_messages_fetch 
ON tiny_outbox_messages (scheduled_at_utc, status) 
WHERE status = 0;

-- Inbox Table (Consumer Deduplication)
CREATE TABLE IF NOT EXISTS tiny_inbox_messages (
    message_id UUID PRIMARY KEY,
    event_type VARCHAR(500) NOT NULL,
    received_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## 🧪 Testing

The test suite runs end-to-end integration tests using **[Testcontainers for .NET](https://dotnet.testcontainers.org/)** to spin up real, ephemeral Docker instances of PostgreSQL and RabbitMQ:

```bash
# Ensure Docker daemon is running, then execute:
dotnet test
```

---

## 🤝 Contributing

Contributions, issues, and feature requests are welcome!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'feat: Add AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📄 License

Distributed under the **MIT License**. See `LICENSE` for more information.