using VectorFlow.Benchmarks;
using VectorFlow.Benchmarks.Simulators;

Console.WriteLine("═══════════════════════════════════════════════════════════════════");
Console.WriteLine("  VectorFlow SDK — Ingestion Benchmark");
Console.WriteLine("  Comparing Naive Sequential vs VectorFlow Adaptive Batching");
Console.WriteLine("═══════════════════════════════════════════════════════════════════");
Console.WriteLine();

var simOpts = new SimulatorOptions
{
    EmbeddingLatency = TimeSpan.FromMilliseconds(150),  // Realistic Azure OpenAI latency
    CosmosWriteLatency = TimeSpan.FromMilliseconds(10), // Realistic Cosmos DB write latency
    RuPerWrite = 30,                                     // Typical RU for vector upsert
    EmbeddingDimensions = 1536
};

int[] scenarios = [50, 200, 500, 1000];

Console.WriteLine($"Simulation parameters:");
Console.WriteLine($"  Embedding API latency: {simOpts.EmbeddingLatency.TotalMilliseconds}ms/batch");
Console.WriteLine($"  Cosmos write latency:  {simOpts.CosmosWriteLatency.TotalMilliseconds}ms/record");
Console.WriteLine($"  RU per write:          {simOpts.RuPerWrite} RU");
Console.WriteLine($"  Embedding dimensions:  {simOpts.EmbeddingDimensions}");
Console.WriteLine();

var allResults = new List<(BenchmarkResult Naive, BenchmarkResult VectorFlow)>();

foreach (var chunkCount in scenarios)
{
    Console.WriteLine($"─── Scenario: {chunkCount} chunks (≈ {chunkCount * 400} tokens) ───");
    Console.WriteLine();

    var naive = await IngestionBenchmark.RunNaiveAsync(chunkCount, simOpts);
    var vectorflow = await IngestionBenchmark.RunVectorFlowAsync(chunkCount, simOpts);

    allResults.Add((naive, vectorflow));

    PrintResult(naive);
    PrintResult(vectorflow);

    var speedup = naive.TotalDuration / vectorflow.TotalDuration;
    var embeddingSavings = (1.0 - (double)vectorflow.EmbeddingApiCalls / naive.EmbeddingApiCalls) * 100;
    var ruReduction = vectorflow.PeakRuPerSecond < naive.PeakRuPerSecond
        ? (1.0 - vectorflow.PeakRuPerSecond / naive.PeakRuPerSecond) * 100
        : 0;

    Console.WriteLine($"  ⚡ Speedup:                {speedup:F1}x faster");
    Console.WriteLine($"  📉 Embedding API calls:    {naive.EmbeddingApiCalls} → {vectorflow.EmbeddingApiCalls} ({embeddingSavings:F0}% fewer)");
    Console.WriteLine($"  💰 Peak RU/s:              {naive.PeakRuPerSecond:F0} → {vectorflow.PeakRuPerSecond:F0} RU/s ({ruReduction:F0}% lower peak)");
    Console.WriteLine($"  📦 Write batches:          {naive.WriteBatches} → {vectorflow.WriteBatches}");
    Console.WriteLine();
}

// Summary table
Console.WriteLine("═══════════════════════════════════════════════════════════════════");
Console.WriteLine("  SUMMARY TABLE");
Console.WriteLine("═══════════════════════════════════════════════════════════════════");
Console.WriteLine();
Console.WriteLine($"{"Chunks",-8} {"Naive",-12} {"VectorFlow",-12} {"Speedup",-10} {"API Calls",-18} {"Peak RU/s",-20}");
Console.WriteLine($"{"------",-8} {"-----",-12} {"----------",-12} {"-------",-10} {"---------",-18} {"---------",-20}");

foreach (var (naive, vf) in allResults)
{
    var speedup = naive.TotalDuration / vf.TotalDuration;
    Console.WriteLine($"{naive.ChunkCount,-8} {naive.TotalDuration.TotalSeconds:F2}s{"",-6} {vf.TotalDuration.TotalSeconds:F2}s{"",-6} {speedup:F1}x{"",-6} {naive.EmbeddingApiCalls} → {vf.EmbeddingApiCalls,-8} {naive.PeakRuPerSecond:F0} → {vf.PeakRuPerSecond:F0}");
}

Console.WriteLine();
Console.WriteLine("═══════════════════════════════════════════════════════════════════");

void PrintResult(BenchmarkResult r)
{
    Console.WriteLine($"  [{r.Method}]");
    Console.WriteLine($"    Duration:            {r.TotalDuration.TotalSeconds:F2}s");
    Console.WriteLine($"    Total RU consumed:   {r.TotalRuConsumed:F0} RU");
    Console.WriteLine($"    Embedding API calls: {r.EmbeddingApiCalls}");
    Console.WriteLine($"    Avg latency/chunk:   {r.AvgLatencyPerChunk.TotalMilliseconds:F1}ms");
    Console.WriteLine($"    Peak RU/s:           {r.PeakRuPerSecond:F0} RU/s");
    Console.WriteLine($"    Write batches:       {r.WriteBatches}");
    Console.WriteLine();
}
