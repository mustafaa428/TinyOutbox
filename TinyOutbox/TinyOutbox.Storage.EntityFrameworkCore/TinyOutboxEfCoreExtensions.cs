using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using TinyOutbox.Core;

namespace TinyOutbox.Storage.EntityFrameworkCore;

public static class TinyOutboxEfCoreExtensions
{
    public static IServiceCollection UseEntityFrameworkCore<TDbContext>(this IServiceCollection services)
        where TDbContext : DbContext
    {
        services.AddSingleton<IOutboxStorage, EfCoreOutboxStorage<TDbContext>>();
        return services;
    }
}