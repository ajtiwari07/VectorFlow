namespace VectorFlow.Benchmarks.Simulators;

/// <summary>
/// Configuration for simulated Azure service latencies and costs.
/// Values based on real-world Azure performance characteristics.
/// </summary>
public class SimulatorOptions
{
    /// <summary>Simulated latency per embedding API call (batch).</summary>
    public TimeSpan EmbeddingLatency { get; set; } = TimeSpan.FromMilliseconds(150);

    /// <summary>Simulated latency per single Cosmos DB write.</summary>
    public TimeSpan CosmosWriteLatency { get; set; } = TimeSpan.FromMilliseconds(10);

    /// <summary>Simulated RU cost per upsert (vector doc ~1536 dims).</summary>
    public double RuPerWrite { get; set; } = 30;

    /// <summary>Embedding dimensions.</summary>
    public int EmbeddingDimensions { get; set; } = 1536;
}
