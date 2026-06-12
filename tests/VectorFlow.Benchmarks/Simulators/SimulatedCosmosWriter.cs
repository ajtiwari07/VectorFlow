using System.Diagnostics;
using VectorFlow.Writing;

namespace VectorFlow.Benchmarks.Simulators;

/// <summary>
/// Simulates Cosmos DB writes with realistic latency and RU reporting.
/// Tracks batch count and total writes for benchmark comparison.
/// </summary>
internal class SimulatedCosmosWriter : IBatchWriter
{
    private readonly SimulatorOptions _options;
    private int _batchCount;
    private int _totalWrites;

    public int BatchCount => _batchCount;
    public int TotalWrites => _totalWrites;

    public SimulatedCosmosWriter(SimulatorOptions options)
    {
        _options = options;
    }

    public async Task<WriteBatchResult> WriteBatchAsync(
        IReadOnlyList<VectorRecord> records,
        CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _batchCount);
        Interlocked.Add(ref _totalWrites, records.Count);

        var sw = Stopwatch.StartNew();

        // Simulate write latency — parallel writes within a batch amortize
        var perRecordLatency = _options.CosmosWriteLatency;
        var batchLatency = TimeSpan.FromMilliseconds(
            perRecordLatency.TotalMilliseconds * Math.Min(records.Count, 4)); // max 4 concurrent
        await Task.Delay(batchLatency, cancellationToken);

        sw.Stop();

        var totalRu = records.Count * _options.RuPerWrite;
        return new WriteBatchResult(records.Count, totalRu, sw.Elapsed);
    }
}
