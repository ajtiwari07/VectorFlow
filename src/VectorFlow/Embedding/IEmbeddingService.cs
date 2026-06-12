namespace VectorFlow.Embedding;

internal interface IEmbeddingService
{
    Task<IReadOnlyList<float[]>> GenerateEmbeddingsAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default);
}
