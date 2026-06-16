namespace VectorFlow.Search;

/// <summary>
/// Options for semantic vector search.
/// </summary>
public class SearchOptions
{
    /// <summary>
    /// Maximum number of ranked results to return.
    /// </summary>
    public int TopK { get; set; } = 10;

    /// <summary>
    /// Minimum similarity score required for a result to be returned.
    /// </summary>
    public double ScoreThreshold { get; set; } = 0.0;

    /// <summary>
    /// Optional partition key filter (userId).
    /// </summary>
    public string? PartitionKey { get; set; }

    /// <summary>
    /// Optional document identifier filter.
    /// </summary>
    public string? DocumentId { get; set; }
}
