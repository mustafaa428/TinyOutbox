using Microsoft.Extensions.DependencyInjection;
using TinyOutbox.Core.Services.Abstract;
using TinyOutbox.Core.Services.Concrate;

namespace TinyOutbox.Hosting;

public class TinyOutboxBuilder
{
    public IServiceCollection Services { get; }

    public TinyOutboxBuilder(IServiceCollection services)
    {
        Services = services;
    }
}

public static class TinyOutboxExtensions
{
    public static TinyOutboxBuilder AddTinyOutbox(this IServiceCollection services, Action<OutboxOptions>? configure = null)
    {
        var options = new OutboxOptions();
        configure?.Invoke(options);

        services.AddSingleton(options);
        services.AddScoped<ITinyOutbox, TinyOutboxService>();
        services.AddHostedService<OutboxBackgroundWorker>();
        services.AddScoped<ITinyInbox, TinyInboxService>();
        return new TinyOutboxBuilder(services);
    }
}