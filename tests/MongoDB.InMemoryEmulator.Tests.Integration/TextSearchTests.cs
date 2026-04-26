using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for text search ($text filter), $meta textScore, $search/$vectorSearch stubs.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/text-search/
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/query/text/
/// </remarks>
public class TextSearchTests
{
    private static IMongoCollection<BsonDocument> CreateCollection()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("testdb");
        return db.GetCollection<BsonDocument>("articles");
    }

    private static void SeedArticles(IMongoCollection<BsonDocument> col)
    {
        col.InsertMany(new[]
        {
            new BsonDocument { { "_id", 1 }, { "title", "MongoDB Tutorial" }, { "body", "An introduction to MongoDB and its features" } },
            new BsonDocument { { "_id", 2 }, { "title", "Redis Guide" }, { "body", "How to use Redis for caching and messaging" } },
            new BsonDocument { { "_id", 3 }, { "title", "MongoDB Advanced Topics" }, { "body", "Aggregation pipelines and indexing in MongoDB" } },
            new BsonDocument { { "_id", 4 }, { "title", "SQL Basics" }, { "body", "Introduction to SQL databases and queries" } },
            new BsonDocument { { "_id", 5 }, { "title", "NoSQL Overview" }, { "body", "Comparing MongoDB, Redis, and Cassandra" } }
        });
    }

    [Fact]
    public void Text_search_single_word()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/text/
        var col = CreateCollection();
        SeedArticles(col);

        var filter = Builders<BsonDocument>.Filter.Text("MongoDB");
        var results = col.Find(filter).ToList();

        Assert.Equal(3, results.Count); // Articles 1, 3, 5
        Assert.All(results, r =>
        {
            var text = r["title"].AsString + " " + r["body"].AsString;
            Assert.Contains("MongoDB", text, StringComparison.OrdinalIgnoreCase);
        });
    }

    [Fact]
    public void Text_search_multiple_words_OR()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/text/
        //   "If the $search string consists of multiple terms, $text performs a logical OR search."
        var col = CreateCollection();
        SeedArticles(col);

        var filter = Builders<BsonDocument>.Filter.Text("Redis caching");
        var results = col.Find(filter).ToList();

        Assert.True(results.Count >= 2); // At least Redis Guide and NoSQL Overview
    }

    [Fact]
    public void Text_search_phrase()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/text/#phrases
        //   "To match the exact phrase, enclose the phrase in escaped quotes."
        var col = CreateCollection();
        SeedArticles(col);

        var filter = Builders<BsonDocument>.Filter.Text("\"MongoDB Tutorial\"");
        var results = col.Find(filter).ToList();

        Assert.Single(results);
        Assert.Equal(1, results[0]["_id"].AsInt32);
    }

    [Fact]
    public void Text_search_negation()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/text/#negations
        //   "Prefixing a word with a hyphen-minus excludes documents that contain the negated term."
        var col = CreateCollection();
        SeedArticles(col);

        // Search for MongoDB but exclude "Advanced"
        var filter = Builders<BsonDocument>.Filter.Text("MongoDB -Advanced");
        var results = col.Find(filter).ToList();

        Assert.DoesNotContain(results, r => r["_id"] == 3);
        Assert.True(results.Count >= 1);
    }

    [Fact]
    public void Text_search_case_insensitive_default()
    {
        var col = CreateCollection();
        SeedArticles(col);

        var filter = Builders<BsonDocument>.Filter.Text("mongodb");
        var results = col.Find(filter).ToList();

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public void Text_search_case_sensitive()
    {
        var col = CreateCollection();
        SeedArticles(col);

        // Case-sensitive search — only matches exact case
        var filter = new BsonDocument("$text", new BsonDocument
        {
            { "$search", "mongodb" },
            { "$caseSensitive", true }
        });

        var results = col.Find(filter).ToList();
        // "mongodb" lowercase doesn't appear in any title/body (they use "MongoDB")
        Assert.Empty(results);
    }

    [Fact]
    public void Text_index_created_without_error()
    {
        // Ref: https://www.mongodb.com/docs/manual/core/index-text/#create-a-text-index
        var col = CreateCollection();

        var indexName = col.Indexes.CreateOne(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Text("title")));

        Assert.False(string.IsNullOrEmpty(indexName));
    }

    [Fact]
    public void Text_index_listed()
    {
        var col = CreateCollection();
        col.Indexes.CreateOne(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Text("title")));

        var indexes = col.Indexes.List().ToList();
        Assert.True(indexes.Count >= 2); // _id_ + text index
    }

    [Fact]
    public void Search_aggregation_stub_matches_substring()
    {
        // Ref: https://www.mongodb.com/docs/atlas/atlas-search/query-syntax/
        var col = CreateCollection();
        SeedArticles(col);

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$search", new BsonDocument("text", new BsonDocument
            {
                { "query", "MongoDB" },
                { "path", "title" }
            })));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Equal(2, results.Count); // "MongoDB Tutorial" and "MongoDB Advanced Topics"
        Assert.All(results, r => Assert.True(r.Contains("score")));
    }

    [Fact]
    public void SearchMeta_aggregation_returns_count()
    {
        var col = CreateCollection();
        SeedArticles(col);

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$searchMeta", new BsonDocument("text", new BsonDocument
            {
                { "query", "anything" },
                { "path", "title" }
            })));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Single(results);
        Assert.True(results[0].Contains("count"));
    }

    [Fact]
    public void VectorSearch_returns_sorted_by_similarity()
    {
        // Ref: https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-stage/
        var col = CreateCollection();
        col.InsertMany(new[]
        {
            new BsonDocument { { "_id", 1 }, { "name", "A" }, { "embedding", new BsonArray { 1.0, 0.0, 0.0 } } },
            new BsonDocument { { "_id", 2 }, { "name", "B" }, { "embedding", new BsonArray { 0.0, 1.0, 0.0 } } },
            new BsonDocument { { "_id", 3 }, { "name", "C" }, { "embedding", new BsonArray { 0.9, 0.1, 0.0 } } }
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$vectorSearch", new BsonDocument
            {
                { "queryVector", new BsonArray { 1.0, 0.0, 0.0 } },
                { "path", "embedding" },
                { "numCandidates", 10 },
                { "limit", 3 },
                { "index", "vector_index" }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Equal(3, results.Count);
        // Most similar to [1,0,0] should be A, then C, then B
        Assert.Equal("A", results[0]["name"].AsString);
        Assert.Equal("C", results[1]["name"].AsString);
        Assert.Equal("B", results[2]["name"].AsString);
    }

    [Fact]
    public void VectorSearch_respects_limit()
    {
        var col = CreateCollection();
        col.InsertMany(Enumerable.Range(1, 10).Select(i =>
            new BsonDocument { { "_id", i }, { "vec", new BsonArray { (double)i, 0.0 } } }));

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$vectorSearch", new BsonDocument
            {
                { "queryVector", new BsonArray { 5.0, 0.0 } },
                { "path", "vec" },
                { "numCandidates", 10 },
                { "limit", 3 }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Equal(3, results.Count);
    }
}
