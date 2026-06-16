using VectorFlow.Embedding;

namespace VectorFlow.Tests.Simulators;

/// <summary>
/// Thread-safe simulated embedding service for tests.
/// Tracks call counts and supports injecting failures.
/// </summary>
internal class TestEmbeddingService : IEmbeddingService
{
    private int _callCount;
    private int _failAfter = int.MaxValue;
    private readonly int _dimensions;
    private readonly TimeSpan _latency;

    public int CallCount => Volatile.Read(ref _callCount);

    public TestEmbeddingService(int dimensions = 8, TimeSpan? latency = null)
    {
        _dimensions = dimensions;
        _latency = latency ?? TimeSpan.FromMilliseconds(5);
    }

    public void FailAfterCalls(int count) => _failAfter = count;

    public async Task<IReadOnlyList<float[]>> GenerateEmbeddingsAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default)
    {
        var call = Interlocked.Increment(ref _callCount);
        if (call > _failAfter)
            throw new InvalidOperationException($"Simulated embedding failure on call {call}");

        await Task.Delay(_latency, cancellationToken);

        var results = new float[texts.Count][];
        for (int i = 0; i < texts.Count; i++)
        {
            results[i] = new float[_dimensions];
            // Deterministic embedding based on text hash for consistency
            var hash = texts[i].GetHashCode();
            for (int d = 0; d < _dimensions; d++)
                results[i][d] = ((hash + d) % 100) / 100f;
        }
        return results;
    }
}
