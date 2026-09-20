using Microsoft.Extensions.DependencyInjection;
using TinyOutbox.Core.Services.Abstract;
using TinyOutbox.Hosting;

namespace TinyOutbox.Transport.RabbitMQ;

public static class RabbitMqExtensions
{
    public static TinyOutboxBuilder UseRabbitMQ(this TinyOutboxBuilder builder, Action<RabbitMqOutboxOptions> configure)
    {
        var options = new RabbitMqOutboxOptions();
        configure(options);

        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton<IMessagePublisher, RabbitMqMessagePublisher>();

        return builder;
    }
}