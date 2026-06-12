using Azure.AI.OpenAI;
using Microsoft.Extensions.Logging;
using OpenAI.Embeddings;

namespace VectorFlow.Embedding;

internal class AzureOpenAIEmbeddingService : IEmbeddingService
{
    private readonly EmbeddingClient _client;
    private readonly int _dimensions;
    private readonly ILogger<AzureOpenAIEmbeddingService> _logger;

    public AzureOpenAIEmbeddingService(
        EmbeddingClient client,
        int dimensions,
        ILogger<AzureOpenAIEmbeddingService>? logger = null)
    {
        _client = client;
        _dimensions = dimensions;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<AzureOpenAIEmbeddingService>.Instance;
    }

    public AzureOpenAIEmbeddingService(
        string endpoint,
        string apiKey,
        string deploymentName,
        int dimensions = 1536,
        ILogger<AzureOpenAIEmbeddingService>? logger = null)
    {
        var azureClient = new AzureOpenAIClient(
            new Uri(endpoint),
            new System.ClientModel.ApiKeyCredential(apiKey));
        _client = azureClient.GetEmbeddingClient(deploymentName);
        _dimensions = dimensions;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<AzureOpenAIEmbeddingService>.Instance;
    }

    public async Task<IReadOnlyList<float[]>> GenerateEmbeddingsAsync(
        IReadOnlyList<string> texts,
        CancellationToken cancellationToken = default)
    {
        if (texts.Count == 0) return [];

        _logger.LogDebug("Generating embeddings for {Count} texts", texts.Count);

        var embeddingOptions = new EmbeddingGenerationOptions
        {
            Dimensions = _dimensions
        };

        var response = await _client.GenerateEmbeddingsAsync(texts, embeddingOptions, cancellationToken);

        return response.Value
            .OrderBy(e => e.Index)
            .Select(e => e.ToFloats().ToArray())
            .ToList();
    }
}
