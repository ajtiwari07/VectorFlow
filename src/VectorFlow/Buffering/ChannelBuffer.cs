using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace VectorFlow.Buffering;

/// <summary>
/// In-memory buffer using .NET Channels for zero-allocation async buffering with backpressure.
/// </summary>
internal class ChannelBuffer<T> : IIngestionBuffer<T>
{
    private readonly Channel<T> _channel;

    public ChannelBuffer(int capacity = 10_000)
    {
        _channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        });
    }

    public int PendingCount => _channel.Reader.Count;

    public ValueTask EnqueueAsync(T item, CancellationToken cancellationToken = default)
        => _channel.Writer.WriteAsync(item, cancellationToken);

    public async ValueTask EnqueueManyAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        foreach (var item in items)
        {
            await _channel.Writer.WriteAsync(item, cancellationToken);
        }
    }

    public async IAsyncEnumerable<IReadOnlyList<T>> ConsumeBatchesAsync(
        int batchSize,
        TimeSpan flushInterval,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var batch = new List<T>(batchSize);
        using var timer = new PeriodicTimer(flushInterval);

        while (!cancellationToken.IsCancellationRequested)
        {
            var shouldFlush = false;

            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

                var readTask = _channel.Reader.ReadAsync(cts.Token).AsTask();
                var timerTask = timer.WaitForNextTickAsync(cts.Token).AsTask();

                var completed = await Task.WhenAny(readTask, timerTask);

                if (completed == readTask && readTask.IsCompletedSuccessfully)
                {
                    batch.Add(await readTask);

                    // Drain as many as available up to batch size
                    while (batch.Count < batchSize && _channel.Reader.TryRead(out var item))
                    {
                        batch.Add(item);
                    }
                }

                shouldFlush = batch.Count >= batchSize || (completed == timerTask && batch.Count > 0);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            if (shouldFlush)
            {
                var flushed = batch.ToList();
                batch.Clear();
                yield return flushed;
            }
        }

        // Flush remaining on shutdown
        while (_channel.Reader.TryRead(out var remaining))
        {
            batch.Add(remaining);
        }

        if (batch.Count > 0)
        {
            yield return batch.ToList();
        }
    }
}
