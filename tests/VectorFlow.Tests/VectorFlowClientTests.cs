using FluentAssertions;
using VectorFlow.Buffering;
using VectorFlow.Embedding;
using VectorFlow.Scheduling;
using VectorFlow.Tests.Simulators;
using VectorFlow.Writing;

namespace VectorFlow.Tests;

public class VectorFlowClientTests
{
    private static VectorFlowClient CreateClient(
        TestEmbeddingService? embedding = null,
        TestBatchWriter? writer = null,
        VectorFlowOptions? options = null)
    {
        options ??= new VectorFlowOptions
        {
            CosmosEndpoint = "https://fake.documents.azure.com:443/",
            CosmosDatabase = "testdb",
            CosmosContainer = "vectors",
            OpenAIEndpoint = "https://fake.openai.azure.com/",
            OpenAIDeployment = "text-embedding-test",
            EmbeddingDimensions = 8,
            WriteRuBudgetPerSecond = 4000, // high budget for fast tests
            EstimatedRuPerWrite = 10,
            MinFlushInterval = TimeSpan.FromMilliseconds(10),
            MaxFlushInterval = TimeSpan.FromMilliseconds(50),
            BufferCapacity = 10_000,
            MaxEmbeddingBatchSize = 50,
            MaxConcurrentEmbeddingCalls = 3,
            EnableSearch = false // no real Cosmos container in tests
        };

        embedding ??= new TestEmbeddingService(options.EmbeddingDimensions);
        writer ??= new TestBatchWriter();

        var batcher = new EmbeddingBatcher(embedding, options.MaxEmbeddingBatchSize, options.MaxConcurrentEmbeddingCalls);
        var buffer = new ChannelBuffer<VectorRecord>(options.BufferCapacity);
        var scheduler = new AdaptiveWriteScheduler(options, null);

        return new VectorFlowClient(batcher, buffer, writer, scheduler);
    }

    // ──────────────────────────────────────────────────────────────
    // Basic Ingestion
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task IngestAsync_small_document_writes_all_chunks()
    {
        var writer = new TestBatchWriter();
        await using var client = CreateClient(writer: writer);

        var texts = Enumerable.Range(0, 10).Select(i => $"Chunk {i} content").ToList();
        await client.IngestAsync("doc-1", "user-1", texts);
        await client.FlushAsync();

        writer.TotalWrites.Should().Be(10);
        writer.AllRecords.Select(r => r.DocumentId).Should().AllBe("doc-1");
        writer.AllRecords.Select(r => r.UserId).Should().AllBe("user-1");
    }

    [Fact]
    public async Task IngestAsync_preserves_chunk_ordering()
    {
        var writer = new TestBatchWriter();
        await using var client = CreateClient(writer: writer);

        var texts = Enumerable.Range(0, 20).Select(i => $"Chunk-{i:D3}").ToList();
        await client.IngestAsync("doc-order", "user-1", texts);
        await client.FlushAsync();

        var indices = writer.AllRecords.OrderBy(r => r.ChunkIndex).Select(r => r.ChunkIndex).ToList();
        indices.Should().BeEquivalentTo(Enumerable.Range(0, 20));
    }

    [Fact]
    public async Task IngestAsync_generates_embeddings_for_all_chunks()
    {
        var embedding = new TestEmbeddingService(dimensions: 8);
        var writer = new TestBatchWriter();
        await using var client = CreateClient(embedding: embedding, writer: writer);

        var texts = Enumerable.Range(0, 25).Select(i => $"Text {i}").ToList();
        await client.IngestAsync("doc-embed", "user-1", texts);
        await client.FlushAsync();

        writer.AllRecords.Should().AllSatisfy(r =>
        {
            r.Embedding.Should().NotBeNull();
            r.Embedding.Should().HaveCount(8);
        });
    }

    // ──────────────────────────────────────────────────────────────
    // Bulk Ingestion
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task BulkIngest_1000_chunks_completes_without_data_loss()
    {
        var writer = new TestBatchWriter();
        await using var client = CreateClient(writer: writer);

        var texts = Enumerable.Range(0, 1000).Select(i => $"Bulk chunk {i} with enough content to be realistic").ToList();
        await client.IngestAsync("doc-bulk", "user-bulk", texts);
        await client.FlushAsync();

        writer.TotalWrites.Should().Be(1000, "all 1000 chunks must be written");
        writer.BatchCount.Should().BeGreaterThan(1, "should use multiple batches");
    }

    [Fact]
    public async Task BulkIngest_multiple_documents_sequentially()
    {
        var writer = new TestBatchWriter();
        await using var client = CreateClient(writer: writer);

        for (int doc = 0; doc < 5; doc++)
        {
            var texts = Enumerable.Range(0, 200).Select(i => $"Doc{doc} chunk {i}").ToList();
            await client.IngestAsync($"doc-{doc}", "user-1", texts);
        }
        await client.FlushAsync();

        writer.TotalWrites.Should().Be(1000);
        var byDoc = writer.AllRecords.GroupBy(r => r.DocumentId).ToList();
        byDoc.Should().HaveCount(5);
        byDoc.Should().AllSatisfy(g => g.Count().Should().Be(200));
    }

    // ──────────────────────────────────────────────────────────────
    // Concurrent Client Simulation (multiple users ingesting at once)
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentIngestion_multiple_users_no_data_loss()
    {
        var writer = new TestBatchWriter();
        await using var client = CreateClient(writer: writer);

        var tasks = Enumerable.Range(0, 10).Select(userId =>
            Task.Run(async () =>
            {
                var texts = Enumerable.Range(0, 100).Select(i => $"User{userId} chunk {i}").ToList();
                await client.IngestAsync($"doc-user{userId}", $"user-{userId}", texts);
            })).ToArray();

        await Task.WhenAll(tasks);
        await client.FlushAsync();

        writer.TotalWrites.Should().Be(1000, "10 users × 100 chunks = 1000 total");
        var byUser = writer.AllRecords.GroupBy(r => r.UserId).ToList();
        byUser.Should().HaveCount(10);
        byUser.Should().AllSatisfy(g => g.Count().Should().Be(100));
    }

    [Fact]
    public async Task ConcurrentIngestion_high_volume_stress_test()
    {
        var writer = new TestBatchWriter(latency: TimeSpan.FromMilliseconds(2));
        var embedding = new TestEmbeddingService(dimensions: 8, latency: TimeSpan.FromMilliseconds(2));
        await using var client = CreateClient(embedding: embedding, writer: writer);

        // 20 concurrent "clients" each ingesting 50 chunks
        var tasks = Enumerable.Range(0, 20).Select(clientId =>
            Task.Run(async () =>
            {
                var texts = Enumerable.Range(0, 50).Select(i => $"Client{clientId} text {i} payload data").ToList();
                await client.IngestAsync($"doc-{clientId}", $"client-{clientId}", texts);
            })).ToArray();

        await Task.WhenAll(tasks);
        await client.FlushAsync();

        writer.TotalWrites.Should().Be(1000);
    }

    [Fact]
    public async Task ConcurrentIngestion_interleaved_with_flush()
    {
        var writer = new TestBatchWriter();
        await using var client = CreateClient(writer: writer);

        // Simulate real-world: multiple users uploading while system is flushing
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var producerTasks = Enumerable.Range(0, 5).Select(userId =>
            Task.Run(async () =>
            {
                for (int batch = 0; batch < 3; batch++)
                {
                    var texts = Enumerable.Range(0, 30).Select(i => $"U{userId}B{batch}C{i}").ToList();
                    await client.IngestAsync($"doc-{userId}-{batch}", $"user-{userId}", texts);
                    await Task.Delay(10); // slight stagger
                }
            })).ToArray();

        await Task.WhenAll(producerTasks);
        await client.FlushAsync(cts.Token);

        writer.TotalWrites.Should().Be(450, "5 users × 3 batches × 30 chunks");
    }

    // ──────────────────────────────────────────────────────────────
    // Backpressure
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Backpressure_does_not_lose_data_with_small_buffer()
    {
        var writer = new TestBatchWriter();
        var options = new VectorFlowOptions
        {
            CosmosEndpoint = "https://fake.documents.azure.com:443/",
            CosmosDatabase = "testdb",
            CosmosContainer = "vectors",
            OpenAIEndpoint = "https://fake.openai.azure.com/",
            OpenAIDeployment = "test",
            EmbeddingDimensions = 8,
            BufferCapacity = 50, // very small buffer
            WriteRuBudgetPerSecond = 4000, // high budget for fast drain
            EstimatedRuPerWrite = 10,
            MinFlushInterval = TimeSpan.FromMilliseconds(10),
            MaxFlushInterval = TimeSpan.FromMilliseconds(30),
            MaxEmbeddingBatchSize = 50,
            MaxConcurrentEmbeddingCalls = 3,
            EnableSearch = false
        };
        await using var client = CreateClient(writer: writer, options: options);

        // Ingest more than buffer capacity — should not lose data
        var texts = Enumerable.Range(0, 200).Select(i => $"Backpressure chunk {i}").ToList();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await client.IngestAsync("doc-bp", "user-bp", texts);
        await client.FlushAsync(cts.Token);

        writer.TotalWrites.Should().Be(200, "backpressure should slow down but not lose data");
    }

    // ──────────────────────────────────────────────────────────────
    // Error Recovery (Transient Failures)
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task TransientWriteFailure_retries_and_eventually_writes()
    {
        var writer = new TestBatchWriter();
        writer.FailNextBatches(2); // first 2 batches will fail
        await using var client = CreateClient(writer: writer);

        var texts = Enumerable.Range(0, 20).Select(i => $"Retry chunk {i}").ToList();
        await client.IngestAsync("doc-retry", "user-1", texts);

        // Wait longer to account for retries (2s delay per retry)
        var timeout = TimeSpan.FromSeconds(15);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (writer.TotalWrites < 20 && sw.Elapsed < timeout)
        {
            await Task.Delay(100);
        }

        writer.TotalWrites.Should().Be(20, "all records should eventually be written after retries");
    }

    // ──────────────────────────────────────────────────────────────
    // Stats & Telemetry
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task GetStats_reports_accurate_counts()
    {
        var writer = new TestBatchWriter();
        await using var client = CreateClient(writer: writer);

        var texts = Enumerable.Range(0, 50).Select(i => $"Stats chunk {i}").ToList();
        await client.IngestAsync("doc-stats", "user-1", texts);

        var beforeFlush = client.GetStats();
        beforeFlush.TotalIngested.Should().Be(50);

        await client.FlushAsync();

        var afterFlush = client.GetStats();
        afterFlush.TotalWritten.Should().Be(50);
        afterFlush.PendingInBuffer.Should().Be(0);
        afterFlush.TotalRuConsumed.Should().BeGreaterThan(0);
    }

    // ──────────────────────────────────────────────────────────────
    // Cancellation
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Cancellation_stops_ingestion_gracefully()
    {
        var embedding = new TestEmbeddingService(dimensions: 8, latency: TimeSpan.FromMilliseconds(500));
        await using var client = CreateClient(embedding: embedding);

        var cts = new CancellationTokenSource();
        var texts = Enumerable.Range(0, 100).Select(i => $"Cancel chunk {i}").ToList();

        cts.CancelAfter(TimeSpan.FromMilliseconds(50));

        var act = () => client.IngestAsync("doc-cancel", "user-1", texts, cancellationToken: cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    // ──────────────────────────────────────────────────────────────
    // Dispose Safety
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task DisposeAsync_completes_cleanly_with_pending_items()
    {
        var writer = new TestBatchWriter(latency: TimeSpan.FromMilliseconds(50));
        var client = CreateClient(writer: writer);

        var texts = Enumerable.Range(0, 30).Select(i => $"Dispose chunk {i}").ToList();
        await client.IngestAsync("doc-dispose", "user-1", texts);

        // Dispose without waiting for flush — should not throw
        var act = () => client.DisposeAsync().AsTask();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task DisposeAsync_is_idempotent()
    {
        var client = CreateClient();

        await client.DisposeAsync();
        var act = () => client.DisposeAsync().AsTask();
        await act.Should().NotThrowAsync();
    }

    // ──────────────────────────────────────────────────────────────
    // IngestRecordsAsync (pre-embedded)
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task IngestRecordsAsync_writes_preembedded_records()
    {
        var writer = new TestBatchWriter();
        await using var client = CreateClient(writer: writer);

        var records = Enumerable.Range(0, 15).Select(i => new VectorRecord
        {
            UserId = "user-pre",
            DocumentId = "doc-pre",
            ChunkIndex = i,
            Text = $"Pre-embedded chunk {i}",
            Embedding = new float[] { 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f }
        }).ToList();

        await client.IngestRecordsAsync(records);
        await client.FlushAsync();

        writer.TotalWrites.Should().Be(15);
        writer.AllRecords.Should().AllSatisfy(r => r.Embedding[0].Should().Be(1f));
    }

    // ──────────────────────────────────────────────────────────────
    // Embedding Batching Efficiency
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task EmbeddingBatching_reduces_api_calls()
    {
        var embedding = new TestEmbeddingService(dimensions: 8);
        var writer = new TestBatchWriter();
        var options = new VectorFlowOptions
        {
            CosmosEndpoint = "https://fake.documents.azure.com:443/",
            CosmosDatabase = "testdb",
            CosmosContainer = "vectors",
            OpenAIEndpoint = "https://fake.openai.azure.com/",
            OpenAIDeployment = "test",
            EmbeddingDimensions = 8,
            MaxEmbeddingBatchSize = 50, // batch up to 50 per call
            MaxConcurrentEmbeddingCalls = 3,
            WriteRuBudgetPerSecond = 400,
            EstimatedRuPerWrite = 10,
            MinFlushInterval = TimeSpan.FromMilliseconds(30),
            MaxFlushInterval = TimeSpan.FromMilliseconds(100),
            EnableSearch = false
        };
        await using var client = CreateClient(embedding: embedding, writer: writer, options: options);

        // 200 texts with batch size 50 → should need only 4 API calls
        var texts = Enumerable.Range(0, 200).Select(i => $"Batch text {i}").ToList();
        await client.IngestAsync("doc-batch", "user-1", texts);
        await client.FlushAsync();

        embedding.CallCount.Should().BeLessThanOrEqualTo(8, "200 texts / 50 batch = 4 calls (with concurrency, up to 8 max)");
        embedding.CallCount.Should().BeGreaterThanOrEqualTo(4, "need at least 4 calls for 200 texts");
        writer.TotalWrites.Should().Be(200);
    }

    // ──────────────────────────────────────────────────────────────
    // Multi-Client Isolation (simulate different apps sharing one SDK config)
    // ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task MultipleClientInstances_operate_independently()
    {
        var writer1 = new TestBatchWriter();
        var writer2 = new TestBatchWriter();

        await using var client1 = CreateClient(writer: writer1);
        await using var client2 = CreateClient(writer: writer2);

        var task1 = Task.Run(async () =>
        {
            var texts = Enumerable.Range(0, 100).Select(i => $"App1 chunk {i}").ToList();
            await client1.IngestAsync("app1-doc", "app1-user", texts);
            await client1.FlushAsync();
        });

        var task2 = Task.Run(async () =>
        {
            var texts = Enumerable.Range(0, 100).Select(i => $"App2 chunk {i}").ToList();
            await client2.IngestAsync("app2-doc", "app2-user", texts);
            await client2.FlushAsync();
        });

        await Task.WhenAll(task1, task2);

        writer1.TotalWrites.Should().Be(100);
        writer2.TotalWrites.Should().Be(100);
        writer1.AllRecords.Should().AllSatisfy(r => r.UserId.Should().Be("app1-user"));
        writer2.AllRecords.Should().AllSatisfy(r => r.UserId.Should().Be("app2-user"));
    }
}
