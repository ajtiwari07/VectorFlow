using System.Diagnostics;
using VectorFlow;
using VectorFlow.Writing;
using VectorFlow.Benchmarks.Simulators;

namespace VectorFlow.Benchmarks;

/// <summary>
/// Compares naive sequential ingestion vs VectorFlow SDK adaptive batching.
/// Uses simulated services with realistic latency and RU costs.
/// </summary>
public class IngestionBenchmark
{
    public static async Task<BenchmarkResult> RunNaiveAsync(int chunkCount, SimulatorOptions simOpts)
    {
        var chunks = GenerateChunks(chunkCount);
        var embeddingService = new SimulatedEmbeddingService(simOpts);
        var cosmosWriter = new SimulatedCosmosWriter(simOpts);

        var sw = Stopwatch.StartNew();
        double totalRu = 0;
        int embeddingCalls = 0;

        // Naive: embed one-by-one, write one-by-one
        for (int i = 0; i < chunks.Count; i++)
        {
            var embeddings = await embeddingService.GenerateEmbeddingsAsync([chunks[i]], default);
            embeddingCalls++;

            var record = new VectorRecord
            {
                UserId = "user-1",
                DocumentId = "doc-1",
                ChunkIndex = i,
                Text = chunks[i],
                Embedding = embeddings[0]
            };

            var result = await cosmosWriter.WriteBatchAsync([record], default);
            totalRu += result.CostUnitsConsumed;
        }

        sw.Stop();

        return new BenchmarkResult
        {
            Method = "Naive (Sequential)",
            ChunkCount = chunkCount,
            TotalDuration = sw.Elapsed,
            TotalRuConsumed = totalRu,
            EmbeddingApiCalls = embeddingCalls,
            AvgLatencyPerChunk = sw.Elapsed / chunkCount,
            PeakRuPerSecond = CalculatePeakRu(totalRu, sw.Elapsed),
            WriteBatches = chunkCount
        };
    }

    public static async Task<BenchmarkResult> RunVectorFlowAsync(int chunkCount, SimulatorOptions simOpts, VectorFlowOptions? flowOpts = null)
    {
        var chunks = GenerateChunks(chunkCount);
        var embeddingService = new SimulatedEmbeddingService(simOpts);
        var cosmosWriter = new SimulatedCosmosWriter(simOpts);

        flowOpts ??= new VectorFlowOptions
        {
            WriteRuBudgetPerSecond = 400,
            EstimatedRuPerWrite = simOpts.RuPerWrite,
            BufferCapacity = 10_000,
            MaxEmbeddingBatchSize = 100,
            MaxConcurrentEmbeddingCalls = 3,
            MinFlushInterval = TimeSpan.FromMilliseconds(500),
            MaxFlushInterval = TimeSpan.FromSeconds(10)
        };

        // Use internal constructor via reflection for testing with simulated services
        var buffer = new VectorFlow.Buffering.ChannelBuffer<VectorRecord>(flowOpts.BufferCapacity);
        var batcher = new VectorFlow.Embedding.EmbeddingBatcher(
            embeddingService,
            flowOpts.MaxEmbeddingBatchSize,
            flowOpts.MaxConcurrentEmbeddingCalls);
        var scheduler = new VectorFlow.Scheduling.AdaptiveWriteScheduler(flowOpts, null);

        var client = new VectorFlowClient(batcher, buffer, cosmosWriter, scheduler);

        var sw = Stopwatch.StartNew();

        await client.IngestAsync("doc-1", "user-1", chunks);
        await client.FlushAsync();

        sw.Stop();

        var stats = client.GetStats();
        await client.DisposeAsync();

        return new BenchmarkResult
        {
            Method = "VectorFlow SDK",
            ChunkCount = chunkCount,
            TotalDuration = sw.Elapsed,
            TotalRuConsumed = stats.TotalRuConsumed,
            EmbeddingApiCalls = embeddingService.CallCount,
            AvgLatencyPerChunk = sw.Elapsed / chunkCount,
            PeakRuPerSecond = CalculatePeakRu(stats.TotalRuConsumed, sw.Elapsed),
            WriteBatches = cosmosWriter.BatchCount
        };
    }

    private static List<string> GenerateChunks(int count)
    {
        var chunks = new List<string>(count);
        for (int i = 0; i < count; i++)
        {
            chunks.Add($"This is chunk {i} of a large document. It contains meaningful text that would typically be between 200-500 tokens for a real document chunk used in RAG scenarios. The chunk discusses topic {i % 10} in detail.");
        }
        return chunks;
    }

    private static double CalculatePeakRu(double totalRu, TimeSpan duration)
    {
        if (duration.TotalSeconds == 0) return 0;
        return totalRu / duration.TotalSeconds;
    }
}

public class BenchmarkResult
{
    public string Method { get; set; } = "";
    public int ChunkCount { get; set; }
    public TimeSpan TotalDuration { get; set; }
    public double TotalRuConsumed { get; set; }
    public int EmbeddingApiCalls { get; set; }
    public TimeSpan AvgLatencyPerChunk { get; set; }
    public double PeakRuPerSecond { get; set; }
    public int WriteBatches { get; set; }
}
