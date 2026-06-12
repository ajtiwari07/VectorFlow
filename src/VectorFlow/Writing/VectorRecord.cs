namespace VectorFlow.Writing;

/// <summary>
/// Represents a vector record ready to be written to the database.
/// </summary>
public class VectorRecord
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string UserId { get; set; } = default!;
    public string DocumentId { get; set; } = default!;
    public int ChunkIndex { get; set; }
    public string Text { get; set; } = default!;
    public int TokenCount { get; set; }
    public float[] Embedding { get; set; } = [];
    public Dictionary<string, object?> Metadata { get; set; } = [];
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
