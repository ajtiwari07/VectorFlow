using Microsoft.Extensions.Logging;

namespace VectorFlow.Embedding;

/// <summary>
/// Batches embedding requests to maximize token packing and respect rate limits.
/// </summary>
internal class EmbeddingBatcher
{
    private readonly IEmbeddingService _embeddingService;
    private readonly int _maxBatchSize;
    private readonly SemaphoreSlim _rateLimiter;
    private readonly ILogger<EmbeddingBatcher> _logger;

    public EmbeddingBatcher(
        IEmbeddingService embeddingService,
        int maxBatchSize = 100,
        int maxConcurrentCalls = 3,
        ILogger<EmbeddingBatcher>? logger = null)
    {
        _embeddingService = embeddingService;
        _maxBatchSize = maxBatchSize;
        _rateLimiter = new SemaphoreSlim(maxConcurrentCalls);
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<EmbeddingBatcher>.Instance;
    }

    public async Task<IReadOnlyList<float[]>> BatchEmbedAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default)
    {
        var allEmbeddings = new float[texts.Count][];
        var batches = texts
            .Select((text, index) => (text, index))
            .Chunk(_maxBatchSize)
            .ToList();

        _logger.LogInformation("Processing {TotalTexts} texts in {BatchCount} batches", texts.Count, batches.Count);

        foreach (var batch in batches)
        {
            await _rateLimiter.WaitAsync(cancellationToken);
            try
            {
                var batchTexts = batch.Select(b => b.text).ToList();
                var embeddings = await _embeddingService.GenerateEmbeddingsAsync(batchTexts, cancellationToken);

                for (int i = 0; i < batch.Length; i++)
                {
                    allEmbeddings[batch[i].index] = embeddings[i];
                }
            }
            finally
            {
                _rateLimiter.Release();
            }
        }

        return allEmbeddings;
    }
}
