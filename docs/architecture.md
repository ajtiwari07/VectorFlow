# VectorFlow architecture

![VectorFlow SDK architecture](vectorflow-architecture.svg)

## Data flows

### Ingestion

1. Batch and optionally cache embeddings.
2. Buffer vector records with bounded backpressure.
3. Write adaptive micro-batches to Cosmos.
4. Feed consumed RUs back into the next write schedule.

### Semantic search

VectorFlow embeds the query, runs a filtered Cosmos `VectorDistance` query, and
returns ranked results above the configured score threshold.

## Boundaries

- **SDK:** embedding, buffering, adaptive writing, and search behind `VectorFlowClient`.
- **Providers:** Pluggable model providers generate embeddings or perform inference.
- **Storage:** Cosmos stores and searches vectors.
- **Optional caching:** A cache avoids regenerating vectors for duplicate text.
