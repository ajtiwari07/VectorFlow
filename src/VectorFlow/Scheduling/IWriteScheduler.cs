using VectorFlow.Writing;

namespace VectorFlow.Scheduling;

/// <summary>
/// Determines optimal batch size and flush timing to keep writes
/// within budget and avoid impacting live read queries.
/// </summary>
internal interface IWriteScheduler
{
    /// <summary>
    /// Get the next recommended batch size and delay before flushing.
    /// </summary>
    WriteSchedule GetNextSchedule();

    /// <summary>
    /// Report the result of a write batch so the scheduler can adapt.
    /// </summary>
    void ReportWriteResult(WriteBatchResult result);
}

/// <summary>
/// The scheduler's recommendation for the next write batch.
/// </summary>
internal record WriteSchedule(int BatchSize, TimeSpan DelayBeforeFlush);
