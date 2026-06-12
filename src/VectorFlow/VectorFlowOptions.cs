using Azure.Core;

namespace VectorFlow;

/// <summary>
/// Configuration for the VectorFlow SDK.
/// </summary>
public class VectorFlowOptions
{
    /// <summary>
    /// Cosmos DB account endpoint (e.g., https://your-account.documents.azure.com:443/)
    /// </summary>
    public string CosmosEndpoint { get; set; } = default!;

    /// <summary>
    /// Cosmos DB database name.
    /// </summary>
    public string CosmosDatabase { get; set; } = default!;

    /// <summary>
    /// Cosmos DB container name for vector records.
    /// </summary>
    public string CosmosContainer { get; set; } = default!;

    /// <summary>
    /// Azure OpenAI endpoint (e.g., https://your-resource.openai.azure.com/)
    /// </summary>
    public string OpenAIEndpoint { get; set; } = default!;

    /// <summary>
    /// Azure OpenAI embedding model deployment name.
    /// </summary>
    public string OpenAIDeployment { get; set; } = default!;

    /// <summary>
    /// Embedding dimensions (default 1536 for text-embedding-ada-002).
    /// </summary>
    public int EmbeddingDimensions { get; set; } = 1536;

    /// <summary>
    /// Azure credential used for both Cosmos DB and Azure OpenAI.
    /// Uses DefaultAzureCredential if not specified.
    /// Ignored if API keys are provided.
    /// </summary>
    public TokenCredential? Credential { get; set; }

    /// <summary>
    /// Cosmos DB account key. If set, key-based auth is used instead of TokenCredential.
    /// </summary>
    public string? CosmosKey { get; set; }

    /// <summary>
    /// Azure OpenAI API key. If set, key-based auth is used instead of TokenCredential.
    /// </summary>
    public string? OpenAIKey { get; set; }

    /// <summary>
    /// Maximum RU/s budget the SDK is allowed to use for writes.
    /// The scheduler uses this to calculate safe batch sizes.
    /// </summary>
    public double WriteRuBudgetPerSecond { get; set; } = 400;

    /// <summary>
    /// Estimated RU cost per single chunk upsert (depends on document size).
    /// </summary>
    public double EstimatedRuPerWrite { get; set; } = 30;

    /// <summary>
    /// Maximum number of chunks to buffer in memory before applying backpressure.
    /// </summary>
    public int BufferCapacity { get; set; } = 10_000;

    /// <summary>
    /// Maximum number of texts per embedding API call.
    /// </summary>
    public int MaxEmbeddingBatchSize { get; set; } = 100;

    /// <summary>
    /// Maximum concurrent embedding API calls.
    /// </summary>
    public int MaxConcurrentEmbeddingCalls { get; set; } = 3;

    /// <summary>
    /// Minimum interval between write flushes (floor for the scheduler).
    /// </summary>
    public TimeSpan MinFlushInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Maximum interval between write flushes (ceiling for the scheduler).
    /// </summary>
    public TimeSpan MaxFlushInterval { get; set; } = TimeSpan.FromSeconds(10);

    // --- Optional: Redis Embedding Cache ---

    /// <summary>
    /// Azure Cache for Redis connection string. If set, embeddings are cached
    /// to avoid redundant Azure OpenAI calls for identical text.
    /// </summary>
    public string? RedisConnectionString { get; set; }

    /// <summary>
    /// TTL for cached embeddings. Default: 24 hours.
    /// </summary>
    public TimeSpan RedisCacheTtl { get; set; } = TimeSpan.FromHours(24);
}
