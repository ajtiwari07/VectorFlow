using Azure.AI.OpenAI;
using Azure.Identity;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using VectorFlow.Buffering;
using VectorFlow.Embedding;
using VectorFlow.Scheduling;
using VectorFlow.Search;
using VectorFlow.Writing;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace VectorFlow;

/// <summary>
/// The public entry point for the VectorFlow SDK.
/// Handles embedding generation, memory buffering, and scheduled writes
/// to the vector database without impacting live read queries.
/// </summary>
public class VectorFlowClient : IAsyncDisposable
{
    private readonly Container _container;
    private readonly EmbeddingBatcher _embeddingBatcher;
    private readonly IIngestionBuffer<VectorRecord> _buffer;
    private readonly IBatchWriter _writer;
    private readonly IWriteScheduler _scheduler;
    private readonly ILogger<VectorFlowClient> _logger;
    private readonly bool _searchEnabled;
    private readonly CancellationTokenSource _drainCts = new();
    private readonly Task _drainTask;
    private readonly TaskCompletionSource _drainStarted = new();

    private long _totalRecordsIngested;
    private long _totalRecordsWritten;
    private double _totalRuConsumed;

    /// <summary>
    /// Creates a new VectorFlowClient. The SDK handles all internal connections
    /// to Azure OpenAI and Cosmos DB using the provided options.
    /// </summary>
    public VectorFlowClient(VectorFlowOptions options, ILogger<VectorFlowClient>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.CosmosEndpoint);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.CosmosDatabase);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.CosmosContainer);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.OpenAIEndpoint);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.OpenAIDeployment);

        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<VectorFlowClient>.Instance;

        // Cosmos DB: prefer API key if provided, otherwise use TokenCredential
        CosmosClient cosmosClient;
        if (!string.IsNullOrWhiteSpace(options.CosmosKey))
        {
            cosmosClient = new CosmosClient(options.CosmosEndpoint, options.CosmosKey, new CosmosClientOptions
            {
                AllowBulkExecution = true
            });
        }
        else
        {
            var credential = options.Credential ?? new DefaultAzureCredential();
            cosmosClient = new CosmosClient(options.CosmosEndpoint, credential, new CosmosClientOptions
            {
                AllowBulkExecution = true
            });
        }
        var container = cosmosClient.GetContainer(options.CosmosDatabase, options.CosmosContainer);
        _container = container;
        _writer = new CosmosBatchWriter(container);
        _searchEnabled = options.EnableSearch;

        // Azure OpenAI: prefer API key if provided, otherwise use TokenCredential
        AzureOpenAIClient openAIClient;
        if (!string.IsNullOrWhiteSpace(options.OpenAIKey))
        {
            openAIClient = new AzureOpenAIClient(
                new Uri(options.OpenAIEndpoint),
                new System.ClientModel.ApiKeyCredential(options.OpenAIKey));
        }
        else
        {
            var credential = options.Credential ?? new DefaultAzureCredential();
            openAIClient = new AzureOpenAIClient(new Uri(options.OpenAIEndpoint), credential);
        }
        var embeddingClient = openAIClient.GetEmbeddingClient(options.OpenAIDeployment);
        IEmbeddingService embeddingService = new AzureOpenAIEmbeddingService(embeddingClient, options.EmbeddingDimensions, null);

        // Wrap with Redis cache if configured
        if (!string.IsNullOrWhiteSpace(options.RedisConnectionString))
        {
            var redis = ConnectionMultiplexer.Connect(options.RedisConnectionString);
            embeddingService = new CachedEmbeddingService(embeddingService, redis, options.RedisCacheTtl);
        }

        _embeddingBatcher = new EmbeddingBatcher(
            embeddingService,
            options.MaxEmbeddingBatchSize,
            options.MaxConcurrentEmbeddingCalls);
        _buffer = new ChannelBuffer<VectorRecord>(options.BufferCapacity);
        _scheduler = new AdaptiveWriteScheduler(options, null);

        // Start the background drain loop
        _drainTask = DrainLoopAsync(_drainCts.Token);
    }

    internal VectorFlowClient(
        EmbeddingBatcher embeddingBatcher,
        IIngestionBuffer<VectorRecord> buffer,
        IBatchWriter writer,
        IWriteScheduler scheduler,
        ILogger<VectorFlowClient>? logger = null)
        : this(
            embeddingBatcher,
            buffer,
            writer,
            writer is CosmosBatchWriter cosmosWriter
                ? cosmosWriter.Container
                : null!,
            scheduler,
            writer is CosmosBatchWriter,
            logger)
    {
    }

    internal VectorFlowClient(
        EmbeddingBatcher embeddingBatcher,
        IIngestionBuffer<VectorRecord> buffer,
        IBatchWriter writer,
        Container container,
        IWriteScheduler scheduler,
        bool enableSearch = true,
        ILogger<VectorFlowClient>? logger = null)
    {
        _container = container;
        _embeddingBatcher = embeddingBatcher;
        _buffer = buffer;
        _writer = writer;
        _scheduler = scheduler;
        _searchEnabled = enableSearch;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<VectorFlowClient>.Instance;

        _drainTask = DrainLoopAsync(_drainCts.Token);
    }

    /// <summary>
    /// Ingest texts: generates embeddings and buffers for scheduled write.
    /// Returns immediately after buffering (non-blocking for the caller).
    /// </summary>
    public async Task IngestAsync(
        string documentId,
        string partitionKey,
        IReadOnlyList<string> texts,
        IReadOnlyList<int>? tokenCounts = null,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Ingesting {Count} chunks for document {DocumentId}", texts.Count, documentId);

        // Generate embeddings (batched + rate-limited)
        var embeddings = await _embeddingBatcher.BatchEmbedAsync(texts, cancellationToken);

        // Create vector records and enqueue into buffer
        for (int i = 0; i < texts.Count; i++)
        {
            var record = new VectorRecord
            {
                UserId = partitionKey,
                DocumentId = documentId,
                ChunkIndex = i,
                Text = texts[i],
                TokenCount = tokenCounts?[i] ?? 0,
                Embedding = embeddings[i]
            };

            await _buffer.EnqueueAsync(record, cancellationToken);
        }

        Interlocked.Add(ref _totalRecordsIngested, texts.Count);
        _logger.LogDebug("Buffered {Count} records. Pending: {Pending}", texts.Count, _buffer.PendingCount);
    }

    /// <summary>
    /// Ingest pre-embedded records directly (skip embedding step).
    /// </summary>
    public async Task IngestRecordsAsync(
        IReadOnlyList<VectorRecord> records,
        CancellationToken cancellationToken = default)
    {
        await _buffer.EnqueueManyAsync(records, cancellationToken);
        Interlocked.Add(ref _totalRecordsIngested, records.Count);
    }

    /// <summary>
    /// Searches the configured Cosmos DB vector container using semantic similarity.
    /// </summary>
    public async Task<IReadOnlyList<SearchResult>> SearchAsync(
        string queryText,
        SearchOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (!_searchEnabled)
        {
            throw new InvalidOperationException("Semantic search is disabled for this VectorFlowClient.");
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(queryText);

        options ??= new SearchOptions();
        ArgumentOutOfRangeException.ThrowIfLessThan(options.TopK, 1);

        var queryEmbedding = (await _embeddingBatcher.BatchEmbedAsync([queryText], cancellationToken))[0];
        var queryDefinition = BuildSearchQueryDefinition(options, queryEmbedding);
        var requestOptions = string.IsNullOrWhiteSpace(options.PartitionKey)
            ? null
            : new QueryRequestOptions { PartitionKey = new PartitionKey(options.PartitionKey) };

        var iterator = _container.GetItemQueryStreamIterator(
            queryDefinition,
            requestOptions: requestOptions);

        var results = new List<SearchResult>();

        while (iterator.HasMoreResults)
        {
            using var response = await iterator.ReadNextAsync(cancellationToken);
            response.EnsureSuccessStatusCode();

            if (response.Content is null)
            {
                continue;
            }

            var page = await JsonSerializer.DeserializeAsync<SearchQueryPage>(
                response.Content,
                cancellationToken: cancellationToken);

            if (page?.Documents is null)
            {
                continue;
            }

            foreach (var record in page.Documents)
            {
                if (options.ScoreThreshold > 0.0 && record.Score < options.ScoreThreshold)
                {
                    continue;
                }

                results.Add(new SearchResult
                {
                    Id = record.Id,
                    DocumentId = record.DocumentId,
                    Text = record.Text,
                    ChunkIndex = record.ChunkIndex,
                    Score = record.Score,
                    Metadata = ConvertMetadata(record.Metadata)
                });
            }
        }

        return results;
    }

    /// <summary>
    /// Wait until all currently buffered records have been written.
    /// </summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Flush requested. Pending: {Pending}", _buffer.PendingCount);

        // Wait until all ingested records have been written (not just dequeued from buffer)
        var ingested = Interlocked.Read(ref _totalRecordsIngested);
        while (Interlocked.Read(ref _totalRecordsWritten) < ingested)
        {
            await Task.Delay(50, cancellationToken);
        }

        _logger.LogInformation("Flush complete. Total written: {Written}", Interlocked.Read(ref _totalRecordsWritten));
    }

    /// <summary>
    /// Current SDK telemetry.
    /// </summary>
    public VectorFlowStats GetStats() => new(
        TotalIngested: Interlocked.Read(ref _totalRecordsIngested),
        TotalWritten: Interlocked.Read(ref _totalRecordsWritten),
        PendingInBuffer: _buffer.PendingCount,
        TotalRuConsumed: _totalRuConsumed
    );

    private async Task DrainLoopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("VectorFlow drain loop started");
        _drainStarted.TrySetResult();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var schedule = _scheduler.GetNextSchedule();

                // Wait for the scheduled interval
                await Task.Delay(schedule.DelayBeforeFlush, cancellationToken);

                // Non-blocking drain: read whatever is available up to batch size
                var batch = _buffer.TryReadBatch(schedule.BatchSize);

                if (batch.Count == 0) continue;

                // Write the batch — retry on transient errors
                try
                {
                    var result = await _writer.WriteBatchAsync(batch, cancellationToken);
                    _scheduler.ReportWriteResult(result);

                    Interlocked.Add(ref _totalRecordsWritten, result.RecordsWritten);
                    Interlocked.Exchange(ref _totalRuConsumed, _totalRuConsumed + result.CostUnitsConsumed);

                    _logger.LogDebug(
                        "Wrote {Count} records ({RU:F0} RU, {Ms:F0}ms). Pending: {Pending}",
                        result.RecordsWritten, result.CostUnitsConsumed,
                        result.Duration.TotalMilliseconds, _buffer.PendingCount);
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    _logger.LogError(ex, "Batch write failed for {Count} records, will retry next cycle", batch.Count);
                    // Re-enqueue failed items
                    foreach (var record in batch)
                    {
                        await _buffer.EnqueueAsync(record, cancellationToken);
                    }
                    await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation("Drain loop stopping (cancellation requested)");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Drain loop fatal error");
        }
    }

    private int _disposed;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        _drainCts.Cancel();
        try { await _drainTask; } catch (OperationCanceledException) { }
        _drainCts.Dispose();
        GC.SuppressFinalize(this);
    }

    private static QueryDefinition BuildSearchQueryDefinition(SearchOptions options, float[] queryEmbedding)
    {
        var sql = new StringBuilder(
            """
            SELECT TOP @topK c.id, c.documentId, c.chunkIndex, c.text, c.metadata,
                   (1 - VectorDistance(c.embedding, @queryVector)) AS score
            FROM c
            """);

        var filters = new List<string>();
        if (!string.IsNullOrWhiteSpace(options.PartitionKey))
        {
            filters.Add("c.userId = @partitionKey");
        }

        if (!string.IsNullOrWhiteSpace(options.DocumentId))
        {
            filters.Add("c.documentId = @documentId");
        }

        if (filters.Count > 0)
        {
            sql.AppendLine().Append("WHERE ").AppendJoin(" AND ", filters);
        }

        sql.AppendLine().Append("ORDER BY VectorDistance(c.embedding, @queryVector)");

        var query = new QueryDefinition(sql.ToString())
            .WithParameter("@topK", options.TopK)
            .WithParameter("@queryVector", queryEmbedding);

        if (!string.IsNullOrWhiteSpace(options.PartitionKey))
        {
            query = query.WithParameter("@partitionKey", options.PartitionKey);
        }

        if (!string.IsNullOrWhiteSpace(options.DocumentId))
        {
            query = query.WithParameter("@documentId", options.DocumentId);
        }

        return query;
    }

    private static Dictionary<string, object?> ConvertMetadata(Dictionary<string, JsonElement>? metadata)
    {
        if (metadata is null || metadata.Count == 0)
        {
            return [];
        }

        var converted = new Dictionary<string, object?>(metadata.Count);
        foreach (var item in metadata)
        {
            converted[item.Key] = ConvertJsonElement(item.Value);
        }

        return converted;
    }

    private static object? ConvertJsonElement(JsonElement value) =>
        value.ValueKind switch
        {
            JsonValueKind.Object => value.EnumerateObject()
                .ToDictionary(property => property.Name, property => ConvertJsonElement(property.Value)),
            JsonValueKind.Array => value.EnumerateArray()
                .Select(ConvertJsonElement)
                .ToList(),
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number when value.TryGetInt32(out var intValue) => intValue,
            JsonValueKind.Number when value.TryGetInt64(out var longValue) => longValue,
            JsonValueKind.Number when value.TryGetDecimal(out var decimalValue) => decimalValue,
            JsonValueKind.Number => value.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null or JsonValueKind.Undefined => null,
            _ => value.GetRawText()
        };

    private sealed class SearchQueryPage
    {
        [JsonPropertyName("Documents")]
        public List<SearchQueryRecord>? Documents { get; set; }
    }

    private sealed class SearchQueryRecord
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } = default!;

        [JsonPropertyName("documentId")]
        public string DocumentId { get; set; } = default!;

        [JsonPropertyName("text")]
        public string Text { get; set; } = default!;

        [JsonPropertyName("chunkIndex")]
        public int ChunkIndex { get; set; }

        [JsonPropertyName("score")]
        public double Score { get; set; }

        [JsonPropertyName("metadata")]
        public Dictionary<string, JsonElement>? Metadata { get; set; }
    }
}

public record VectorFlowStats(long TotalIngested, long TotalWritten, int PendingInBuffer, double TotalRuConsumed);
