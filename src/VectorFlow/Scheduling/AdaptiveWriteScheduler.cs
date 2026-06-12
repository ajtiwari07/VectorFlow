using Microsoft.Extensions.Logging;
using VectorFlow.Writing;

namespace VectorFlow.Scheduling;

/// <summary>
/// Adaptive write scheduler that calculates optimal micro-batch sizes and intervals
/// based on a configured RU budget. Self-adjusts when actual costs deviate from estimates.
///
/// Math:
///   safe_writes_per_second = write_ru_budget / ru_per_write
///   batch_size = safe_writes_per_second * flush_interval_seconds
///
/// If observed RU > estimated, the scheduler reduces batch size.
/// If observed RU < estimated, it gradually increases batch size.
/// </summary>
internal class AdaptiveWriteScheduler : IWriteScheduler
{
    private readonly VectorFlowOptions _options;
    private readonly ILogger<AdaptiveWriteScheduler> _logger;

    private double _currentRuPerWrite;
    private double _ewmaRuPerWrite; // exponentially weighted moving average
    private const double EwmaAlpha = 0.3; // responsiveness to new observations
    private int _consecutiveSuccesses;

    public AdaptiveWriteScheduler(VectorFlowOptions options, ILogger<AdaptiveWriteScheduler>? logger = null)
    {
        _options = options;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<AdaptiveWriteScheduler>.Instance;
        _currentRuPerWrite = options.EstimatedRuPerWrite;
        _ewmaRuPerWrite = options.EstimatedRuPerWrite;
    }

    public WriteSchedule GetNextSchedule()
    {
        // Calculate safe writes per second within budget
        var safeWritesPerSecond = _options.WriteRuBudgetPerSecond / _currentRuPerWrite;

        // Calculate flush interval — aim for batches of 10-100 items
        // Interval = batch_size / writes_per_second
        var targetBatchSize = Math.Clamp((int)(safeWritesPerSecond * _options.MinFlushInterval.TotalSeconds), 1, 200);

        // If target batch would be too small, increase interval
        var interval = TimeSpan.FromSeconds(targetBatchSize / safeWritesPerSecond);
        interval = Clamp(interval, _options.MinFlushInterval, _options.MaxFlushInterval);

        // Recalculate batch size from clamped interval
        var batchSize = Math.Max(1, (int)(safeWritesPerSecond * interval.TotalSeconds));

        _logger.LogDebug(
            "Schedule: batch={BatchSize}, interval={Interval:F1}s, ruPerWrite={RuPerWrite:F1}, budget={Budget}RU/s",
            batchSize, interval.TotalSeconds, _currentRuPerWrite, _options.WriteRuBudgetPerSecond);

        return new WriteSchedule(batchSize, interval);
    }

    public void ReportWriteResult(WriteBatchResult result)
    {
        if (result.RecordsWritten == 0) return;

        var observedRuPerWrite = result.CostUnitsConsumed / result.RecordsWritten;

        // Update EWMA
        _ewmaRuPerWrite = (EwmaAlpha * observedRuPerWrite) + ((1 - EwmaAlpha) * _ewmaRuPerWrite);
        _currentRuPerWrite = _ewmaRuPerWrite;

        // Track success streak for gradual scale-up
        if (observedRuPerWrite <= _options.EstimatedRuPerWrite * 1.2)
        {
            _consecutiveSuccesses++;
        }
        else
        {
            _consecutiveSuccesses = 0;
        }

        _logger.LogDebug(
            "Write feedback: observed={Observed:F1} RU/write, ewma={Ewma:F1}, streak={Streak}",
            observedRuPerWrite, _ewmaRuPerWrite, _consecutiveSuccesses);
    }

    private static TimeSpan Clamp(TimeSpan value, TimeSpan min, TimeSpan max)
        => value < min ? min : value > max ? max : value;
}
