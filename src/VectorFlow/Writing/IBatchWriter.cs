namespace VectorFlow.Writing;

/// <summary>
/// Writes batches of vector records to a database.
/// </summary>
internal interface IBatchWriter
{
    /// <summary>
    /// Write a batch of records. Returns total cost units consumed (e.g., RUs for Cosmos).
    /// </summary>
    Task<WriteBatchResult> WriteBatchAsync(IReadOnlyList<VectorRecord> records, CancellationToken cancellationToken = default);
}

internal record WriteBatchResult(int RecordsWritten, double CostUnitsConsumed, TimeSpan Duration);
