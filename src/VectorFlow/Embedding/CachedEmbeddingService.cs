using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace VectorFlow.Embedding;

/// <summary>
/// Decorator that caches embeddings in Redis to avoid redundant OpenAI calls.
/// Falls through to the inner service on cache miss or Redis failure.
/// </summary>
internal class CachedEmbeddingService : IEmbeddingService
{
    private readonly IEmbeddingService _inner;
    private readonly IDatabase _redis;
    private readonly TimeSpan _ttl;
    private readonly ILogger<CachedEmbeddingService> _logger;

    public CachedEmbeddingService(
        IEmbeddingService inner,
        IConnectionMultiplexer redis,
        TimeSpan ttl,
        ILogger<CachedEmbeddingService>? logger = null)
    {
        _inner = inner;
        _redis = redis.GetDatabase();
        _ttl = ttl;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<CachedEmbeddingService>.Instance;
    }

    public async Task<IReadOnlyList<float[]>> GenerateEmbeddingsAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default)
    {
        if (texts.Count == 0) return [];

        var results = new float[texts.Count][];
        var cacheMisses = new List<(int Index, string Text)>();

        // Check cache for each text
        for (int i = 0; i < texts.Count; i++)
        {
            var cached = await TryGetCachedAsync(texts[i]);
            if (cached is not null)
            {
                results[i] = cached;
            }
            else
            {
                cacheMisses.Add((i, texts[i]));
            }
        }

        if (cacheMisses.Count > 0)
        {
            _logger.LogDebug("Embedding cache: {Hits} hits, {Misses} misses",
                texts.Count - cacheMisses.Count, cacheMisses.Count);

            // Generate embeddings for misses only
            var missTexts = cacheMisses.Select(m => m.Text).ToList();
            var generated = await _inner.GenerateEmbeddingsAsync(missTexts, cancellationToken);

            // Store results and cache them
            for (int i = 0; i < cacheMisses.Count; i++)
            {
                results[cacheMisses[i].Index] = generated[i];
                await TryCacheAsync(cacheMisses[i].Text, generated[i]);
            }
        }
        else
        {
            _logger.LogDebug("Embedding cache: all {Count} texts served from cache", texts.Count);
        }

        return results;
    }

    private async Task<float[]?> TryGetCachedAsync(string text)
    {
        try
        {
            var key = BuildKey(text);
            var cached = await _redis.StringGetAsync(key);
            if (cached.IsNullOrEmpty) return null;
            return JsonSerializer.Deserialize<float[]>(cached.ToString());
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Redis cache read failed, skipping");
            return null;
        }
    }

    private async Task TryCacheAsync(string text, float[] embedding)
    {
        try
        {
            var key = BuildKey(text);
            var json = JsonSerializer.Serialize(embedding);
            await _redis.StringSetAsync(key, json, _ttl);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Redis cache write failed, skipping");
        }
    }

    private static string BuildKey(string text)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(text.ToLowerInvariant().Trim()));
        return $"vectorflow:emb:{Convert.ToHexString(hash)[..16]}";
    }
}
