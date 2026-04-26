using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for GridFS file storage. GridFSBucket uses IMongoDatabase.GetCollection()
/// internally, so it should work with our InMemoryMongoDatabase.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/core/gridfs/
///   "GridFS is a specification for storing and retrieving files that exceed the BSON-document
///    size limit of 16 MB."
/// Ref: https://www.mongodb.com/docs/drivers/csharp/current/fundamentals/gridfs/
/// </remarks>
public class GridFSTests
{
    private static (IMongoDatabase db, InMemoryGridFSBucket bucket) CreateBucket(string bucketName = "fs")
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("testdb");
        var bucket = new InMemoryGridFSBucket(db, new GridFSBucketOptions { BucketName = bucketName });
        return (db, bucket);
    }

    [Fact]
    public void Upload_and_download_bytes()
    {
        var (_, bucket) = CreateBucket();
        var data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        var fileId = bucket.UploadFromBytes("test.bin", data);

        var downloaded = bucket.DownloadAsBytes(fileId);
        Assert.Equal(data, downloaded);
    }

    [Fact]
    public void Upload_and_download_stream()
    {
        var (_, bucket) = CreateBucket();
        var data = System.Text.Encoding.UTF8.GetBytes("Hello, GridFS!");

        ObjectId fileId;
        using (var stream = new MemoryStream(data))
        {
            fileId = bucket.UploadFromStream("hello.txt", stream);
        }

        using var outStream = new MemoryStream();
        bucket.DownloadToStream(fileId, outStream);
        Assert.Equal(data, outStream.ToArray());
    }

    [Fact]
    public async Task Upload_and_download_async()
    {
        var (_, bucket) = CreateBucket();
        var data = new byte[] { 42, 43, 44 };

        var fileId = await bucket.UploadFromBytesAsync("async.bin", data);

        var downloaded = await bucket.DownloadAsBytesAsync(fileId);
        Assert.Equal(data, downloaded);
    }

    [Fact]
    public void Upload_large_file_multiple_chunks()
    {
        var (db, bucket) = CreateBucket();
        // Default chunk size is 255KB; create a file larger than that
        var data = new byte[300_000]; // ~300KB
        Random.Shared.NextBytes(data);

        var fileId = bucket.UploadFromBytes("large.bin", data);

        // Verify chunks were created
        var chunks = db.GetCollection<BsonDocument>("fs.chunks")
            .Find(Builders<BsonDocument>.Filter.Eq("files_id", fileId)).ToList();
        Assert.True(chunks.Count > 1, $"Expected multiple chunks, got {chunks.Count}");

        // Download and verify
        var downloaded = bucket.DownloadAsBytes(fileId);
        Assert.Equal(data, downloaded);
    }

    [Fact]
    public void Find_files_by_filename()
    {
        var (_, bucket) = CreateBucket();
        bucket.UploadFromBytes("report.pdf", new byte[] { 1, 2, 3 });
        bucket.UploadFromBytes("image.png", new byte[] { 4, 5, 6 });
        bucket.UploadFromBytes("report.pdf", new byte[] { 7, 8, 9 }); // Duplicate filename

        var filter = Builders<GridFSFileInfo>.Filter.Eq("filename", "report.pdf");
        var files = bucket.Find(filter).ToList();

        Assert.Equal(2, files.Count);
        Assert.All(files, f => Assert.Equal("report.pdf", f.Filename));
    }

    [Fact]
    public void Delete_removes_file_and_chunks()
    {
        var (db, bucket) = CreateBucket();
        var fileId = bucket.UploadFromBytes("delete-me.bin", new byte[] { 1, 2, 3 });

        bucket.Delete(fileId);

        var files = db.GetCollection<BsonDocument>("fs.files")
            .Find(Builders<BsonDocument>.Filter.Eq("_id", fileId)).ToList();
        Assert.Empty(files);

        var chunks = db.GetCollection<BsonDocument>("fs.chunks")
            .Find(Builders<BsonDocument>.Filter.Eq("files_id", fileId)).ToList();
        Assert.Empty(chunks);
    }

    [Fact]
    public void Rename_file()
    {
        var (_, bucket) = CreateBucket();
        var fileId = bucket.UploadFromBytes("old-name.txt", new byte[] { 1, 2, 3 });

        bucket.Rename(fileId, "new-name.txt");

        var filter = Builders<GridFSFileInfo>.Filter.Eq("_id", fileId);
        var files = bucket.Find(filter).ToList();
        Assert.Single(files);
        Assert.Equal("new-name.txt", files[0].Filename);
    }

    [Fact]
    public void Custom_bucket_name()
    {
        var (db, bucket) = CreateBucket("custom_files");
        bucket.UploadFromBytes("test.bin", new byte[] { 1, 2, 3 });

        // Verify collections use custom prefix
        var files = db.GetCollection<BsonDocument>("custom_files.files")
            .Find(FilterDefinition<BsonDocument>.Empty).ToList();
        Assert.Single(files);
    }

    [Fact]
    public void Upload_with_metadata()
    {
        var (_, bucket) = CreateBucket();
        var options = new GridFSUploadOptions
        {
            Metadata = new BsonDocument { { "author", "test" }, { "version", 1 } }
        };
        var fileId = bucket.UploadFromBytes("doc.txt", new byte[] { 1 }, options);

        var filter = Builders<GridFSFileInfo>.Filter.Eq("_id", fileId);
        var files = bucket.Find(filter).ToList();
        Assert.Single(files);
        Assert.Equal("test", files[0].Metadata["author"].AsString);
    }

    [Fact]
    public void Download_nonexistent_file_throws()
    {
        var (_, bucket) = CreateBucket();

        Assert.Throws<GridFSFileNotFoundException>(() =>
            bucket.DownloadAsBytes(ObjectId.GenerateNewId()));
    }

    [Fact]
    public void Upload_and_download_preserves_content()
    {
        var (_, bucket) = CreateBucket();
        var text = "The quick brown fox jumps over the lazy dog. " +
                   "Pack my box with five dozen liquor jugs.";
        var data = System.Text.Encoding.UTF8.GetBytes(text);

        var fileId = bucket.UploadFromBytes("text.txt", data);
        var downloaded = bucket.DownloadAsBytes(fileId);

        Assert.Equal(text, System.Text.Encoding.UTF8.GetString(downloaded));
    }
}
