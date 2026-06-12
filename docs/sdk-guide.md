# VectorFlow SDK — Developer Guide

## Overview

**VectorFlow** is an SDK that ingests text, generates embeddings via Azure OpenAI, and writes vector records to Cosmos DB in adaptive micro-batches — ensuring live user read queries are never degraded by write pressure.

### Problem Statement

Inserting vector embeddings into Cosmos DB triggers index updates that consume high RUs. Naïve bulk writes can starve concurrent read queries (searches). VectorFlow solves this by:

1. Buffering records in a memory-backed channel (non-blocking for callers)
2. Scheduling writes using an adaptive algorithm that respects a configurable RU budget
3. Auto-adjusting batch size based on observed RU costs
4. Optionally caching embeddings in Redis to avoid redundant OpenAI calls

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       VectorFlowClient                           │
│                                                                  │
│  IngestAsync(texts)                                              │
│       │                                                          │
│       ▼                                                          │
│  ┌──────────────────────────────────────────────┐                │
│  │        CachedEmbeddingService (optional)      │                │
│  │  Redis cache: text hash → float[] embedding   │                │
│  │  Cache hit → skip OpenAI    Miss → call below │                │
│  └────────────────────┬─────────────────────────┘                │
│                       ▼                                          │
│  ┌──────────────────┐                                            │
│  │ EmbeddingBatcher │  Batches texts → Azure OpenAI              │
│  │                  │  (rate-limited, concurrent calls)           │
│  └────────┬─────────┘                                            │
│           ▼                                                      │
│  ┌──────────────────┐                                            │
│  │  ChannelBuffer   │  Bounded async queue (backpressure)        │
│  │                  │  Capacity: configurable (default 10K)       │
│  └────────┬─────────┘                                            │
│           ▼                                                      │
│  ┌──────────────────────┐                                        │
│  │ AdaptiveScheduler    │  Calculates batch size + timing        │
│  │                      │  based on RU budget & EWMA costs       │
│  └────────┬─────────────┘                                        │
│           ▼                                                      │
│  ┌──────────────────┐                                            │
│  │ CosmosBatchWriter│  Writes micro-batches to Cosmos DB         │
│  │                  │  Reports actual RU back to scheduler       │
│  └──────────────────┘                                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### 1. Register VectorFlow (DI setup)

```csharp
using VectorFlow;

services.AddVectorFlow(options =>
{
    // Connection — Cosmos DB
    options.CosmosEndpoint = "https://your-account.documents.azure.com:443/";
    options.CosmosDatabase = "synapseai";
    options.CosmosContainer = "chunks";

    // Connection — Azure OpenAI
    options.OpenAIEndpoint = "https://your-openai.openai.azure.com/";
    options.OpenAIDeployment = "text-embedding-ada-002";
    options.EmbeddingDimensions = 1536;

    // Authentication (optional — defaults to DefaultAzureCredential)
    options.Credential = new DefaultAzureCredential();

    // Optional — Redis embedding cache (reduces OpenAI calls)
    options.RedisConnectionString = "your-redis.redis.cache.windows.net:6380,password=...,ssl=True";
    options.RedisCacheTtl = TimeSpan.FromHours(24);

    // Tuning (all optional — sensible defaults provided)
    options.WriteRuBudgetPerSecond = 400;
    options.BufferCapacity = 10_000;
});
```

The SDK handles all internal connections to Cosmos DB, Azure OpenAI, and Redis. No internal interfaces or implementations are exposed — just provide your endpoints and credentials.

### Authentication Options

The SDK supports three auth patterns:

**Managed Identity (recommended for production):**
```csharp
options.Credential = new DefaultAzureCredential();
// or omit — DefaultAzureCredential is used automatically
```

**API Keys (for local dev or simple setups):**
```csharp
options.CosmosKey = "your-cosmos-account-key";
options.OpenAIKey = "your-openai-api-key";
```

**Mixed (key for one, managed identity for the other):**
```csharp
options.Credential = new DefaultAzureCredential(); // used for Cosmos
options.OpenAIKey = "sk-..."; // key takes priority for OpenAI
```

> **Priority rule:** If an API key is provided for a service, it takes precedence over `TokenCredential` for that service.

### 2. Ingest documents

```csharp
// Inject VectorFlowClient
public class DocumentProcessor(VectorFlowClient vectorFlow)
{
    public async Task ProcessAsync(string documentId, string userId, List<string> chunks)
    {
        // Non-blocking: buffers and returns immediately after embedding
        await vectorFlow.IngestAsync(
            documentId: documentId,
            partitionKey: userId,
            texts: chunks);
    }
}
```

### 3. Ingest pre-embedded records (skip embedding)

```csharp
var records = chunks.Select((text, i) => new VectorRecord
{
    UserId = userId,
    DocumentId = documentId,
    ChunkIndex = i,
    Text = text,
    Embedding = preComputedEmbeddings[i]
}).ToList();

await vectorFlow.IngestRecordsAsync(records);
```

### 4. Wait for all writes to complete

```csharp
await vectorFlow.FlushAsync();
```

### 5. Monitor telemetry

```csharp
var stats = vectorFlow.GetStats();
// stats.TotalIngested, stats.TotalWritten, stats.PendingInBuffer, stats.TotalRuConsumed
```

---

## Configuration Reference

### Connection & Auth

| Option | Required | Description |
|--------|----------|-------------|
| `CosmosEndpoint` | ✅ | Cosmos DB account endpoint |
| `CosmosDatabase` | ✅ | Cosmos DB database name |
| `CosmosContainer` | ✅ | Cosmos DB container for vector records |
| `CosmosKey` | ❌ | Cosmos account key (overrides Credential for Cosmos) |
| `OpenAIEndpoint` | ✅ | Azure OpenAI endpoint |
| `OpenAIDeployment` | ✅ | Embedding model deployment name |
| `OpenAIKey` | ❌ | OpenAI API key (overrides Credential for OpenAI) |
| `EmbeddingDimensions` | ❌ | Embedding vector size (default: 1536) |
| `Credential` | ❌ | `TokenCredential` for both services (default: `DefaultAzureCredential`) |

### Redis Embedding Cache (optional)

| Option | Default | Description |
|--------|---------|-------------|
| `RedisConnectionString` | `null` | Azure Cache for Redis connection string. If set, enables caching. |
| `RedisCacheTtl` | 24 hours | How long cached embeddings live before expiry |

**How the cache works:**
- Key: `vectorflow:emb:<SHA256-hash-of-text>` (first 16 hex chars)
- Value: JSON-serialized `float[]` embedding
- On cache hit → skips the OpenAI call entirely (saves cost + latency)
- On cache miss → calls OpenAI, then stores the result for next time
- Redis failures are non-fatal (logged as warnings, falls through to OpenAI)

### Tuning

| Option | Default | Description |
|--------|---------|-------------|
| `WriteRuBudgetPerSecond` | 400 | Max RU/s the SDK can consume for writes |
| `EstimatedRuPerWrite` | 30 | Initial RU estimate per upsert (auto-adjusts) |
| `BufferCapacity` | 10,000 | Max records held in memory before backpressure |
| `MaxEmbeddingBatchSize` | 100 | Texts per embedding API call |
| `MaxConcurrentEmbeddingCalls` | 3 | Parallel embedding API requests |
| `MinFlushInterval` | 500ms | Floor for write scheduling |
| `MaxFlushInterval` | 10s | Ceiling for write scheduling |

---

## Adaptive Scheduling Algorithm

The scheduler uses an EWMA (Exponentially Weighted Moving Average) to track actual RU cost per write:

```
safe_writes_per_second = WriteRuBudgetPerSecond / currentRuPerWrite
batch_size = safe_writes_per_second × flush_interval
```

- If observed RU > estimate → scheduler **reduces** batch size
- If observed RU < estimate → scheduler **gradually increases** batch size
- EWMA alpha: `0.3` (responsive to recent observations without overreacting)

This ensures writes stay within budget even as document sizes vary.

---

## Internal Architecture (for SDK contributors)

All core components are interface-based internally, but **opaque to consumers**:

| Internal Interface | Purpose | Default Implementation |
|-----------|---------|----------------------|
| `IEmbeddingService` | Generate embeddings from text | `AzureOpenAIEmbeddingService` |
| `CachedEmbeddingService` | Decorator: Redis cache around embedding | Wraps `IEmbeddingService` |
| `IBatchWriter` | Write records to database | `CosmosBatchWriter` |
| `IWriteScheduler` | Calculate batch timing | `AdaptiveWriteScheduler` |
| `IIngestionBuffer<T>` | In-memory queue | `ChannelBuffer<T>` |

These interfaces are `internal` — consumers interact only with `VectorFlowClient` and `VectorFlowOptions`. To add a new database backend (e.g., Azure PostgreSQL pgvector), implement `IBatchWriter` inside the SDK and expose it via a new option:

```csharp
// Example: adding PostgreSQL support as an SDK contributor
internal class PostgresBatchWriter : IBatchWriter
{
    public async Task<WriteBatchResult> WriteBatchAsync(
        IReadOnlyList<VectorRecord> records,
        CancellationToken cancellationToken = default)
    {
        // Batch upsert logic
        return new WriteBatchResult(records.Count, costUnits, elapsed);
    }
}

// Then expose via VectorFlowOptions:
// options.DatabaseProvider = VectorFlowDatabaseProvider.PostgreSQL;
```

---

## Error Handling

- **Transient write failures**: Records are re-enqueued into the buffer and retried on the next cycle (with a 2s backoff).
- **Embedding failures**: Exceptions propagate to the caller of `IngestAsync`.
- **Redis cache failures**: Non-fatal — logged as warnings, SDK falls through to OpenAI directly.
- **Buffer full (backpressure)**: `EnqueueAsync` awaits until space is available — callers naturally slow down.
- **Graceful shutdown**: `DisposeAsync()` cancels the drain loop. Call `FlushAsync()` before disposal to ensure all pending records are written.

---

## Usage in SynapseAI

In SynapseAI, `VectorFlowClient` is consumed by the `SynapseAI.Worker` background service:

1. User uploads a document via the frontend
2. The API stores the file in Blob Storage and enqueues a processing message
3. The Worker chunks the document and calls `VectorFlowClient.IngestAsync()`
4. VectorFlow handles embedding (with Redis cache) + batched writes in the background
5. The document becomes searchable as chunks are written to Cosmos DB

This architecture keeps uploads fast and searches unaffected by ingestion load.
