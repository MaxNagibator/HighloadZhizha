using System.Text.Json;
using Confluent.Kafka;
using Npgsql;

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole(options => options.IncludeScopes = true);
builder.Services.AddHostedService<BankEventsConsumer>();
await builder.Build().RunAsync();

sealed record BankEvent(
    Guid EventId,
    string EventType,
    DateTimeOffset OccurredAt,
    Guid CorrelationId,
    Guid AccountId,
    long Amount,
    long Balance,
    string? TraceId = null);

sealed class BankEventsConsumer(ILogger<BankEventsConsumer> logger, IConfiguration configuration) : BackgroundService
{
    private readonly string _brokers = configuration["KAFKA_BROKERS"] ?? "localhost:9092";
    private readonly string _topic = configuration["KAFKA_TOPIC"] ?? "bank.account.events";
    private readonly string _connectionString = configuration["DATABASE_URL"] ?? "Host=localhost;Port=5432;Database=highload;Username=bank;Password=bank";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await MigrateAsync(stoppingToken);
        var config = new ConsumerConfig
        {
            BootstrapServers = _brokers,
            GroupId = "payment-service-v1",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = true
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_topic);
        logger.LogInformation("PaymentService consuming {Topic} from {Brokers}", _topic, _brokers);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                var bankEvent = JsonSerializer.Deserialize<BankEvent>(result.Message.Value, new JsonSerializerOptions(JsonSerializerDefaults.Web));
                if (bankEvent is null) throw new JsonException("Kafka message is empty or invalid");

                var inserted = await SaveEventAsync(bankEvent, result.TopicPartitionOffset.ToString(), stoppingToken);
                consumer.Commit(result);
                logger.LogInformation(
                    "Bank event {Result}: {EventType}, eventId={EventId}, correlationId={CorrelationId}, traceId={TraceId}, accountId={AccountId}, amount={Amount}, balance={Balance}",
                    inserted ? "processed" : "already processed", bankEvent.EventType, bankEvent.EventId,
                    bankEvent.CorrelationId, bankEvent.TraceId, bankEvent.AccountId, bankEvent.Amount, bankEvent.Balance);
            }
            catch (ConsumeException ex)
            {
                logger.LogError(ex, "Kafka consume error: {Reason}", ex.Error.Reason);
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception ex)
            {
                logger.LogError(ex, "Event processing failed; Kafka offset will not be committed");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }

        consumer.Close();
    }

    private async Task MigrateAsync(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            CREATE TABLE IF NOT EXISTS processed_bank_events (
                event_id uuid PRIMARY KEY,
                event_type text NOT NULL,
                correlation_id uuid NOT NULL,
                account_id uuid NOT NULL,
                amount bigint NOT NULL,
                balance bigint NOT NULL,
                kafka_offset text NOT NULL,
                occurred_at timestamptz NOT NULL,
                processed_at timestamptz NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS ix_processed_bank_events_correlation ON processed_bank_events(correlation_id);
            """, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<bool> SaveEventAsync(BankEvent bankEvent, string offset, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand("""
            INSERT INTO processed_bank_events(event_id, event_type, correlation_id, account_id, amount, balance, kafka_offset, occurred_at)
            VALUES (@eventId, @eventType, @correlationId, @accountId, @amount, @balance, @offset, @occurredAt)
            ON CONFLICT (event_id) DO NOTHING;
            """, connection);
        command.Parameters.AddWithValue("eventId", bankEvent.EventId);
        command.Parameters.AddWithValue("eventType", bankEvent.EventType);
        command.Parameters.AddWithValue("correlationId", bankEvent.CorrelationId);
        command.Parameters.AddWithValue("accountId", bankEvent.AccountId);
        command.Parameters.AddWithValue("amount", bankEvent.Amount);
        command.Parameters.AddWithValue("balance", bankEvent.Balance);
        command.Parameters.AddWithValue("offset", offset);
        command.Parameters.AddWithValue("occurredAt", bankEvent.OccurredAt);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }
}
