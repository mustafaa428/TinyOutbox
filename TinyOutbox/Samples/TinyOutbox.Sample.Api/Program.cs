using TinyOutbox.Hosting;
using TinyOutbox.Storage.PostgreSql;
using TinyOutbox.Transport.RabbitMQ;
using TinyOutbox.Core;

var builder = WebApplication.CreateBuilder(args);

// TinyOutbox Kaydı
builder.Services.AddTinyOutbox(opt =>
{
    opt.BatchSize = 100;
    opt.PollingInterval = TimeSpan.FromMilliseconds(250);
})
.UsePostgreSql(builder.Configuration.GetConnectionString("Postgres")!)
.UseRabbitMQ(opt =>
{
    opt.HostName = "localhost";
});

var app = builder.Build();

app.MapPost("/orders", async (ITinyOutbox outbox) =>
{
    // 1. Sipariş veritabanına yazılır...

    // 2. Outbox'a event fırlatılır
    await outbox.PublishAsync(new { OrderId = Guid.NewGuid(), Amount = 250.0m });

    return Results.Ok("Sipariş alındı, event outbox kuyruğuna yazıldı.");
});

app.Run();