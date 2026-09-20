namespace TinyOutbox.Storage.PostgreSql;

public class PostgreSqlOutboxOptions
{
    public string ConnectionString { get; set; } = default!;
    public string TableName { get; set; } = "tiny_outbox_messages";
    public bool AutoMigrate { get; set; } = true;
    public string InboxTableName { get; set; } = "tiny_inbox_messages";
}