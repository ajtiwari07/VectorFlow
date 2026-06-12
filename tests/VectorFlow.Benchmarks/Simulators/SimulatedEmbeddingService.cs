using VectorFlow.Embedding;

namespace VectorFlow.Benchmarks.Simulators;

/// <summary>
/// Simulates Azure OpenAI embedding API with realistic latency.
/// Latency is per-batch (not per-text), simulating real batched API behavior.
/// </summary>
internal class SimulatedEmbeddingService : IEmbeddingService
{
    private readonly SimulatorOptions _options;
    private int _callCount;

    public int CallCount => _callCount;

    public SimulatedEmbeddingService(SimulatorOptions options)
    {
        _options = options;
    }

    public async Task<IReadOnlyList<float[]>> GenerateEmbeddingsAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _callCount);

        // Simulate API latency (one round trip regardless of batch size)
        await Task.Delay(_options.EmbeddingLatency, cancellationToken);

        // Generate deterministic fake embeddings
        var results = new float[texts.Count][];
        for (int i = 0; i < texts.Count; i++)
        {
            results[i] = new float[_options.EmbeddingDimensions];
            results[i][0] = i * 0.01f;
        }
        return results;
    }
}
