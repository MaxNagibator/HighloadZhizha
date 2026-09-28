using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpClient("bank", client =>
{
    client.BaseAddress = new Uri(builder.Configuration["BANK_URL"] ?? "http://green-bank-service");
});
builder.Services.AddSingleton<LoadManager>();

var app = builder.Build();
app.Use(async (context, next) =>
{
    var traceId = context.Request.Headers["X-Trace-ID"].FirstOrDefault();
    if (string.IsNullOrWhiteSpace(traceId) || traceId.Length > 128)
        traceId = Guid.NewGuid().ToString("N");

    context.Response.Headers["X-Trace-ID"] = traceId;
    var log = context.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger("HttpRequestTracing");
    using var scope = log.BeginScope(new Dictionary<string, object> { ["traceId"] = traceId });
    var started = Stopwatch.StartNew();
    log.LogInformation("HTTP_REQUEST_STARTED method={Method} path={Path}", context.Request.Method, context.Request.Path);
    try
    {
        await next(context);
    }
    catch (Exception exception)
    {
        log.LogError(exception, "HTTP_REQUEST_FAILED method={Method} path={Path} durationMs={DurationMs}",
            context.Request.Method, context.Request.Path, started.ElapsedMilliseconds);
        throw;
    }
    finally
    {
        log.LogInformation("HTTP_REQUEST_COMPLETED method={Method} path={Path} status={Status} durationMs={DurationMs}",
            context.Request.Method, context.Request.Path, context.Response.StatusCode, started.ElapsedMilliseconds);
    }
});
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));
app.MapGet("/api/status", (LoadManager manager) => manager.Status());
app.MapPost("/api/start", (LoadSettings settings, LoadManager manager) =>
{
    var errors = settings.Validate();
    return errors.Count > 0
        ? Results.ValidationProblem(errors)
        : Results.Ok(manager.Start(settings));
});
app.MapPost("/api/stop", async (LoadManager manager) => Results.Ok(await manager.StopAsync()));
app.MapGet("/", () => Results.Content(Ui.Html, "text/html; charset=utf-8"));

app.Lifetime.ApplicationStopping.Register(() => app.Services.GetRequiredService<LoadManager>().StopAsync().GetAwaiter().GetResult());
app.Run();

public sealed record LoadSettings(
    int Workers = 10,
    double RequestsPerSecondPerWorker = 1,
    int CreateAccountPercent = 20,
    int MinTopUp = 100,
    int MaxTopUp = 10000,
    int TimeoutSeconds = 5)
{
    public Dictionary<string, string[]> Validate()
    {
        var result = new Dictionary<string, string[]>();
        if (Workers is < 1 or > 200) result[nameof(Workers)] = ["Допустимо от 1 до 200 воркеров."];
        if (RequestsPerSecondPerWorker is < 0.1 or > 100) result[nameof(RequestsPerSecondPerWorker)] = ["Допустимо от 0.1 до 100 RPS на воркер."];
        if (CreateAccountPercent is < 0 or > 100) result[nameof(CreateAccountPercent)] = ["Процент должен быть от 0 до 100."];
        if (MinTopUp < 1 || MaxTopUp < MinTopUp || MaxTopUp > 10_000_000) result[nameof(MinTopUp)] = ["Проверьте диапазон суммы пополнения."];
        if (TimeoutSeconds is < 1 or > 60) result[nameof(TimeoutSeconds)] = ["Таймаут должен быть от 1 до 60 секунд."];
        return result;
    }
}

public sealed class LoadManager(IHttpClientFactory clients, ILogger<LoadManager> logger)
{
    private readonly object _gate = new();
    private readonly ConcurrentQueue<string> _accounts = new();
    private CancellationTokenSource? _cancellation;
    private Task[] _tasks = [];
    private LoadSettings? _settings;
    private DateTimeOffset? _startedAt;
    private DateTimeOffset? _stoppedAt;
    private long _requests, _successes, _failures, _created, _topUps;
    private string? _lastError;

    public object Start(LoadSettings settings)
    {
        lock (_gate)
        {
            if (_cancellation is { IsCancellationRequested: false })
                return Status();

            _settings = settings;
            _startedAt = DateTimeOffset.UtcNow;
            _stoppedAt = null;
            Interlocked.Exchange(ref _requests, 0);
            Interlocked.Exchange(ref _successes, 0);
            Interlocked.Exchange(ref _failures, 0);
            Interlocked.Exchange(ref _created, 0);
            Interlocked.Exchange(ref _topUps, 0);
            _lastError = null;
            _cancellation = new CancellationTokenSource();
            _tasks = Enumerable.Range(1, settings.Workers)
                .Select(id => RunWorkerAsync(id, settings, _cancellation.Token))
                .ToArray();
            logger.LogInformation("LOAD_STARTED workers={Workers} rpsPerWorker={Rps} targetRps={TargetRps} createPercent={CreatePercent}",
                settings.Workers, settings.RequestsPerSecondPerWorker, settings.Workers * settings.RequestsPerSecondPerWorker, settings.CreateAccountPercent);
            return Status();
        }
    }

    public async Task<object> StopAsync()
    {
        Task[] tasks;
        lock (_gate)
        {
            if (_cancellation is null || _cancellation.IsCancellationRequested) return Status();
            _cancellation.Cancel();
            tasks = _tasks;
        }
        try { await Task.WhenAll(tasks); } catch (OperationCanceledException) { }
        lock (_gate) _stoppedAt = DateTimeOffset.UtcNow;
        logger.LogInformation("LOAD_STOPPED requests={Requests} successes={Successes} failures={Failures}", _requests, _successes, _failures);
        return Status();
    }

    public object Status()
    {
        var running = _cancellation is { IsCancellationRequested: false };
        var end = running ? DateTimeOffset.UtcNow : _stoppedAt ?? DateTimeOffset.UtcNow;
        var seconds = _startedAt is null ? 0 : Math.Max(0.001, (end - _startedAt.Value).TotalSeconds);
        return new
        {
            running,
            settings = _settings,
            startedAt = _startedAt,
            stoppedAt = _stoppedAt,
            elapsedSeconds = Math.Round(seconds, 1),
            requests = Interlocked.Read(ref _requests),
            successes = Interlocked.Read(ref _successes),
            failures = Interlocked.Read(ref _failures),
            accountsCreated = Interlocked.Read(ref _created),
            topUps = Interlocked.Read(ref _topUps),
            knownAccounts = _accounts.Count,
            actualRps = _startedAt is null ? 0 : Math.Round(Interlocked.Read(ref _requests) / seconds, 2),
            targetRps = _settings is null ? 0 : _settings.Workers * _settings.RequestsPerSecondPerWorker,
            lastError = _lastError
        };
    }

    private async Task RunWorkerAsync(int workerId, LoadSettings settings, CancellationToken cancellationToken)
    {
        var interval = TimeSpan.FromSeconds(1d / settings.RequestsPerSecondPerWorker);
        const int maxInFlightPerWorker = 100;
        var inFlight = new HashSet<Task>();
        var intervalTicks = Math.Max(1L, (long)(interval.TotalSeconds * Stopwatch.Frequency));
        var nextTick = Stopwatch.GetTimestamp();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                nextTick += intervalTicks;
                var delayTicks = nextTick - Stopwatch.GetTimestamp();
                if (delayTicks > 0)
                    await Task.Delay(TimeSpan.FromSeconds((double)delayTicks / Stopwatch.Frequency), cancellationToken);

                inFlight.RemoveWhere(static task => task.IsCompleted);
                if (inFlight.Count >= maxInFlightPerWorker)
                {
                    nextTick = Stopwatch.GetTimestamp();
                    continue;
                }

                inFlight.Add(SendRequestAsync(workerId, settings, cancellationToken));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        finally
        {
            await Task.WhenAll(inFlight);
        }
    }

    private async Task SendRequestAsync(int workerId, LoadSettings settings, CancellationToken cancellationToken)
    {
        var create = _accounts.IsEmpty || Random.Shared.Next(100) < settings.CreateAccountPercent;
        var correlationId = Guid.NewGuid();
        var traceId = Guid.NewGuid().ToString("N");
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(settings.TimeoutSeconds));
        var stopwatch = Stopwatch.StartNew();
        Interlocked.Increment(ref _requests);

        try
        {
            var client = clients.CreateClient("bank");
            HttpResponseMessage response;
            string operation;
            if (create)
            {
                operation = "create-account";
                using var request = CreateRequest("/accounts", new { ownerName = $"Load test worker {workerId}", correlationId }, traceId);
                response = await client.SendAsync(request, timeout.Token);
                var body = await response.Content.ReadAsStringAsync(timeout.Token);
                response.EnsureSuccessStatusCode();
                using var json = JsonDocument.Parse(body);
                var accountId = json.RootElement.GetProperty("accountId").GetString();
                if (!string.IsNullOrWhiteSpace(accountId)) _accounts.Enqueue(accountId);
                Interlocked.Increment(ref _created);
            }
            else
            {
                operation = "top-up";
                if (!_accounts.TryDequeue(out var accountId)) return;
                _accounts.Enqueue(accountId);
                var amount = Random.Shared.Next(settings.MinTopUp, settings.MaxTopUp + 1);
                var categories = new[] { "groceries", "transport", "entertainment", "other" };
                var category = categories[Random.Shared.Next(categories.Length)];
                using var request = CreateRequest($"/accounts/{accountId}/top-ups", new { amount, correlationId, category }, traceId);
                response = await client.SendAsync(request, timeout.Token);
                response.EnsureSuccessStatusCode();
                Interlocked.Increment(ref _topUps);
            }
            Interlocked.Increment(ref _successes);
            logger.LogInformation("LOAD_REQUEST operation={Operation} status=success durationMs={DurationMs} correlationId={CorrelationId} traceId={TraceId}",
                operation, stopwatch.ElapsedMilliseconds, correlationId, traceId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception exception)
        {
            Interlocked.Increment(ref _failures);
            _lastError = exception.Message;
            logger.LogWarning(exception, "LOAD_REQUEST status=failed durationMs={DurationMs} correlationId={CorrelationId} traceId={TraceId}", stopwatch.ElapsedMilliseconds, correlationId, traceId);
        }
    }

    private static HttpRequestMessage CreateRequest<T>(string path, T body, string traceId)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, path)
        {
            Content = JsonContent.Create(body)
        };
        request.Headers.Add("X-Trace-ID", traceId);
        return request;
    }
}

public static class Ui
{
    public const string Html = """
<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>HighloadZhizha Control</title><style>
:root{color-scheme:dark;font-family:Inter,system-ui,sans-serif;background:#07130f;color:#eafff5}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 20% 0,#163c2d 0,#07130f 42%);min-height:100vh}.wrap{max-width:1100px;margin:auto;padding:38px 22px}h1{font-size:clamp(30px,5vw,55px);margin:0}.sub{color:#8cb9a7;margin:7px 0 28px}.panel{background:#0d211a;border:1px solid #214a39;border-radius:18px;padding:22px;box-shadow:0 20px 60px #0006}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px}label{color:#91b6a7;font-size:13px}input{width:100%;margin-top:7px;padding:12px;background:#07130f;border:1px solid #315b4a;border-radius:10px;color:white;font-size:17px}.buttons{display:flex;gap:12px;margin-top:20px}button,a.btn{border:0;border-radius:11px;padding:13px 22px;font-weight:800;cursor:pointer;text-decoration:none}.start{background:#58e39b;color:#052216}.stop{background:#ff647c;color:white}.grafana{background:#f6a531;color:#261500}.stats{margin-top:20px;grid-template-columns:repeat(auto-fit,minmax(145px,1fr))}.stat{background:#081812;border:1px solid #193c2e;border-radius:14px;padding:16px}.value{font-size:28px;font-weight:800}.name{color:#7da795;font-size:12px}.running{color:#58e39b}.stopped{color:#ff8295}#error{color:#ff9dac;margin-top:14px;min-height:20px}code{color:#8fffc2}</style></head>
<body><main class="wrap"><h1>HighloadZhizha</h1><p class="sub">Пульт синтетической нагрузки · GreenBank → Kafka → Payment + Notifications</p>
<section class="panel"><div class="grid">
<label>Параллельные воркеры<input id="workers" type="number" value="10" min="1" max="200"></label>
<label>RPS на воркер<input id="rps" type="number" value="1" min="0.1" max="100" step="0.1"></label>
<label>Создание счетов, %<input id="create" type="number" value="20" min="0" max="100"></label>
<label>Мин. пополнение, коп.<input id="min" type="number" value="100" min="1"></label>
<label>Макс. пополнение, коп.<input id="max" type="number" value="10000" min="1"></label>
<label>HTTP-таймаут, сек.<input id="timeout" type="number" value="5" min="1" max="60"></label></div>
<div class="buttons"><button class="start" onclick="start()">▶ Запустить</button><button class="stop" onclick="stop()">■ Остановить</button><a class="btn grafana" href="http://grafana.localhost:9080/d/highload-zhizha/highloadzhizha-e28094-nagruzka-i-sobytija" target="_blank">Grafana ↗</a></div><div id="error"></div></section>
<section class="grid stats"><div class="stat"><div id="state" class="value">—</div><div class="name">Состояние</div></div><div class="stat"><div id="actual" class="value">0</div><div class="name">Фактический RPS</div></div><div class="stat"><div id="requests" class="value">0</div><div class="name">Запросов</div></div><div class="stat"><div id="successes" class="value">0</div><div class="name">Успешно</div></div><div class="stat"><div id="failures" class="value">0</div><div class="name">Ошибок</div></div><div class="stat"><div id="accounts" class="value">0</div><div class="name">Счетов</div></div><div class="stat"><div id="topups" class="value">0</div><div class="name">Пополнений</div></div></section></main>
<script>const el=id=>document.getElementById(id);async function call(url,body){let r=await fetch(url,{method:'POST',headers:{'content-type':'application/json'},body:body&&JSON.stringify(body)});let x=await r.json();if(!r.ok)throw Error(Object.values(x.errors||{}).flat().join(' '));return x}async function start(){try{el('error').textContent='';await call('/api/start',{workers:+el('workers').value,requestsPerSecondPerWorker:+el('rps').value,createAccountPercent:+el('create').value,minTopUp:+el('min').value,maxTopUp:+el('max').value,timeoutSeconds:+el('timeout').value});refresh()}catch(e){el('error').textContent=e.message}}async function stop(){await call('/api/stop');refresh()}async function refresh(){try{let s=await fetch('/api/status').then(r=>r.json());el('state').textContent=s.running?'РАБОТАЕТ':'СТОП';el('state').className='value '+(s.running?'running':'stopped');el('actual').textContent=s.actualRps+' / '+s.targetRps;el('requests').textContent=s.requests;el('successes').textContent=s.successes;el('failures').textContent=s.failures;el('accounts').textContent=s.accountsCreated;el('topups').textContent=s.topUps;el('error').textContent=s.lastError||''}catch(e){el('error').textContent=e.message}}setInterval(refresh,1000);refresh()</script></body></html>
""";
}
