using System.Collections.Concurrent;
using System.Diagnostics;
using VectorFlow.Writing;

namespace VectorFlow.Tests.Simulators;

/// <summary>
/// Thread-safe simulated writer that records all writes for verification.
/// Supports injecting transient failures to test retry logic.
/// </summary>
internal class TestBatchWriter : IBatchWriter
{
    private readonly ConcurrentBag<List<VectorRecord>> _batches = new();
    private readonly TimeSpan _latency;
    private readonly double _ruPerRecord;
    private int _batchCount;
    private int _totalWrites;
    private int _failNextN;

    public int BatchCount => Volatile.Read(ref _batchCount);
    public int TotalWrites => Volatile.Read(ref _totalWrites);
    public IReadOnlyList<List<VectorRecord>> Batches => _batches.ToList();
    public IReadOnlyList<VectorRecord> AllRecords => _batches.SelectMany(b => b).ToList();

    public TestBatchWriter(TimeSpan? latency = null, double ruPerRecord = 10)
    {
        _latency = latency ?? TimeSpan.FromMilliseconds(5);
        _ruPerRecord = ruPerRecord;
    }

    public void FailNextBatches(int count) => Interlocked.Exchange(ref _failNextN, count);

    public async Task<WriteBatchResult> WriteBatchAsync(
        IReadOnlyList<VectorRecord> records,
        CancellationToken cancellationToken = default)
    {
        if (Interlocked.Decrement(ref _failNextN) >= 0)
        {
            throw new InvalidOperationException("Simulated transient write failure");
        }

        var sw = Stopwatch.StartNew();
        await Task.Delay(_latency, cancellationToken);
        sw.Stop();

        _batches.Add(records.ToList());
        Interlocked.Increment(ref _batchCount);
        Interlocked.Add(ref _totalWrites, records.Count);

        return new WriteBatchResult(records.Count, records.Count * _ruPerRecord, sw.Elapsed);
    }
}
