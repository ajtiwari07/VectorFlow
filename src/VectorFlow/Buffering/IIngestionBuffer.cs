namespace VectorFlow.Buffering;

/// <summary>
/// Async buffer for vector chunks with backpressure support.
/// </summary>
internal interface IIngestionBuffer<T>
{
    /// <summary>
    /// Enqueue an item. Will wait (backpressure) if buffer is at capacity.
    /// </summary>
    ValueTask EnqueueAsync(T item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enqueue multiple items.
    /// </summary>
    ValueTask EnqueueManyAsync(IEnumerable<T> items, CancellationToken cancellationToken = default);

    /// <summary>
    /// Drain the buffer in batches. Yields when batch is full or flush interval elapses.
    /// </summary>
    IAsyncEnumerable<IReadOnlyList<T>> ConsumeBatchesAsync(
        int batchSize,
        TimeSpan flushInterval,
        CancellationToken cancellationToken);

    /// <summary>
    /// Try to read up to maxCount items without blocking.
    /// Returns items immediately available in the buffer.
    /// </summary>
    IReadOnlyList<T> TryReadBatch(int maxCount);

    /// <summary>
    /// Current number of items waiting in the buffer.
    /// </summary>
    int PendingCount { get; }
}
