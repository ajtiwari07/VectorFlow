using System.Diagnostics;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

namespace VectorFlow.Writing;

/// <summary>
/// Writes vector records to Azure Cosmos DB with bulk execution support.
/// </summary>
internal class CosmosBatchWriter : IBatchWriter
{
    private readonly Container _container;
    private readonly ILogger<CosmosBatchWriter> _logger;

    public CosmosBatchWriter(Container container, ILogger<CosmosBatchWriter>? logger = null)
    {
        _container = container;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<CosmosBatchWriter>.Instance;
    }

    private const int MaxConcurrency = 4; // Small enough to avoid SDK auto-batch body size limits

    public async Task<WriteBatchResult> WriteBatchAsync(IReadOnlyList<VectorRecord> records, CancellationToken cancellationToken = default)
    {
        if (records.Count == 0)
            return new WriteBatchResult(0, 0, TimeSpan.Zero);

        _logger.LogDebug("Writing batch of {Count} records to Cosmos DB", records.Count);
        var sw = Stopwatch.StartNew();

        double totalRUs = 0;
        object ruLock = new();
        var semaphore = new SemaphoreSlim(MaxConcurrency);

        var tasks = records.Select(async record =>
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                var response = await _container.UpsertItemAsync(
                    record, new PartitionKey(record.UserId),
                    new ItemRequestOptions { EnableContentResponseOnWrite = false },
                    cancellationToken);
                lock (ruLock) { totalRUs += response.RequestCharge; }
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
            {
                _logger.LogWarning("Throttled on write, backing off for {Delay}ms", ex.RetryAfter?.TotalMilliseconds ?? 1000);
                await Task.Delay(ex.RetryAfter ?? TimeSpan.FromSeconds(1), cancellationToken);
                var response = await _container.UpsertItemAsync(
                    record, new PartitionKey(record.UserId),
                    new ItemRequestOptions { EnableContentResponseOnWrite = false },
                    cancellationToken);
                lock (ruLock) { totalRUs += response.RequestCharge; }
            }
            finally
            {
                semaphore.Release();
            }
        });

        await Task.WhenAll(tasks);
        sw.Stop();

        _logger.LogInformation(
            "Batch write complete: {Count} records, {RUs:F1} RUs, {Duration:F0}ms",
            records.Count, totalRUs, sw.Elapsed.TotalMilliseconds);

        return new WriteBatchResult(records.Count, totalRUs, sw.Elapsed);
    }
}
