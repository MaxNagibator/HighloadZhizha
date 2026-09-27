using System.Security.Cryptography;
using Confluent.Kafka;

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole(options => options.IncludeScopes = true);
builder.Services.AddHostedService<BankEventNotificationConsumer>();
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

sealed class BankEventNotificationConsumer(
    ILogger<BankEventNotificationConsumer> logger,
    IConfiguration configuration) : BackgroundService
{
    private readonly string _brokers = configuration["KAFKA_BROKERS"] ?? "localhost:9092";
    private readonly string _topic = configuration["KAFKA_TOPIC"] ?? "bank.account.events";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _brokers,
            GroupId = "notification-service-v1",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = false
        };

        using var consumer = new ConsumerBuilder<string, BankEvent>(config)
            .SetValueDeserializer(new JsonBankEventDeserializer())
            .Build();
        consumer.Subscribe(_topic);
        logger.LogInformation("NotificationService consuming {Topic} from {Brokers}", _topic, _brokers);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                var bankEvent = result.Message.Value;
                var sms = SmsNotification.From(bankEvent);

                using (logger.BeginScope(new Dictionary<string, object>
                {
                    ["notificationId"] = bankEvent.EventId,
                    ["eventId"] = bankEvent.EventId,
                    ["eventType"] = bankEvent.EventType,
                    ["correlationId"] = bankEvent.CorrelationId,
                    ["traceId"] = bankEvent.TraceId ?? string.Empty,
                    ["accountId"] = bankEvent.AccountId,
                    ["channel"] = "SMS",
                    ["provider"] = "loki-log-simulator",
                    ["recipient"] = sms.MaskedRecipient,
                    ["deliveryStatus"] = "SENT",
                    ["kafkaPartition"] = result.Partition.Value,
                    ["kafkaOffset"] = result.Offset.Value
                }))
                {
                    logger.LogInformation("SMS_SENT: {SmsText}", sms.Text);
                }

                consumer.Commit(result);
            }
            catch (ConsumeException ex)
            {
                logger.LogError(ex, "Kafka consume error: {Reason}", ex.Error.Reason);
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception ex)
            {
                logger.LogError(ex, "Notification processing failed; Kafka offset will not be committed");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }

        consumer.Close();
    }
}

sealed record SmsNotification(string MaskedRecipient, string Text)
{
    public static SmsNotification From(BankEvent bankEvent)
    {
        var account = bankEvent.AccountId.ToString("N")[..8];
        var text = bankEvent.EventType switch
        {
            "AccountCreated" => $"GreenBank: счёт {account} открыт. Баланс 0.00 ₽.",
            "AccountToppedUp" => $"GreenBank: счёт {account} пополнен на {Money(bankEvent.Amount)} ₽. Баланс {Money(bankEvent.Balance)} ₽.",
            _ => $"GreenBank: по счёту {account} зарегистрировано событие {bankEvent.EventType}."
        };
        return new SmsNotification(Mask(bankEvent.AccountId), text);
    }

    private static string Money(long minorUnits) => (minorUnits / 100m).ToString("F2", System.Globalization.CultureInfo.InvariantCulture);

    private static string Mask(Guid accountId)
    {
        var hash = SHA256.HashData(accountId.ToByteArray());
        var suffix = BitConverter.ToUInt32(hash, 0) % 10_000;
        return $"+7*** ***-{suffix:D4}";
    }
}

sealed class JsonBankEventDeserializer : IDeserializer<BankEvent>
{
    private static readonly System.Text.Json.JsonSerializerOptions Options = new(System.Text.Json.JsonSerializerDefaults.Web);

    public BankEvent Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        isNull
            ? throw new System.Text.Json.JsonException("Kafka message is null")
            : System.Text.Json.JsonSerializer.Deserialize<BankEvent>(data, Options)
              ?? throw new System.Text.Json.JsonException("Kafka message does not contain a bank event");
}
