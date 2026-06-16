namespace VectorFlow.Search;

/// <summary>
/// A ranked semantic search result.
/// </summary>
public class SearchResult
{
    public string Id { get; set; } = default!;

    public string DocumentId { get; set; } = default!;

    public string Text { get; set; } = default!;

    public int ChunkIndex { get; set; }

    public double Score { get; set; }

    public Dictionary<string, object?> Metadata { get; set; } = [];
}
