using System.Text;
using RabbitMQ.Client;
using TinyOutbox.Core.Services.Abstract;

namespace TinyOutbox.Transport.RabbitMQ;

public class RabbitMqMessagePublisher : IMessagePublisher, IAsyncDisposable
{
    private readonly RabbitMqOutboxOptions _options;
    private IConnection? _connection;
    private IChannel? _channel;
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    public RabbitMqMessagePublisher(RabbitMqOutboxOptions options)
    {
        _options = options;
    }

    private async Task EnsureConnectedAsync(CancellationToken ct)
    {
        if (_channel is not null) return;

        await _semaphore.WaitAsync(ct);
        try
        {
            if (_channel is not null) return;

            var factory = new ConnectionFactory
            {
                HostName = _options.HostName,
                Port = _options.Port,
                UserName = _options.UserName,
                Password = _options.Password
            };

            _connection = await factory.CreateConnectionAsync(ct);
            _channel = await _connection.CreateChannelAsync(cancellationToken: ct);

            await _channel.ExchangeDeclareAsync(
                exchange: _options.ExchangeName,
                type: _options.ExchangeType,
                durable: true,
                autoDelete: false,
                cancellationToken: ct);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async Task PublishAsync(OutboxMessage message, CancellationToken ct = default)
    {
        await EnsureConnectedAsync(ct);

        var body = Encoding.UTF8.GetBytes(message.Payload);
        var routingKey = message.EventType.Split(',')[0].Split('.').Last().ToLowerInvariant();

        var props = new BasicProperties
        {
            MessageId = message.Id.ToString(),
            Type = message.EventType,
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            DeliveryMode = DeliveryModes.Persistent
        };

        await _channel!.BasicPublishAsync(
            exchange: _options.ExchangeName,
            routingKey: routingKey,
            mandatory: false,
            basicProperties: props,
            body: body,
            cancellationToken: ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel != null) await _channel.CloseAsync();
        if (_connection != null) await _connection.CloseAsync();
        _semaphore.Dispose();
    }
}