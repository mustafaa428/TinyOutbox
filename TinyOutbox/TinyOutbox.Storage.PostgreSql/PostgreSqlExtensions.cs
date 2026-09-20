using Microsoft.Extensions.DependencyInjection;
using TinyOutbox.Core;
using TinyOutbox.Hosting;

namespace TinyOutbox.Storage.PostgreSql;

public static class PostgreSqlExtensions
{
    public static TinyOutboxBuilder UsePostgreSql(this TinyOutboxBuilder builder, string connectionString, Action<PostgreSqlOutboxOptions>? configure = null)
    {
        var options = new PostgreSqlOutboxOptions { ConnectionString = connectionString };
        configure?.Invoke(options);

        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton<IOutboxStorage, PostgreSqlOutboxStorage>();
        builder.Services.AddSingleton<IInboxStorage, PostgreSqlInboxStorage>();
        return builder;
    }
}