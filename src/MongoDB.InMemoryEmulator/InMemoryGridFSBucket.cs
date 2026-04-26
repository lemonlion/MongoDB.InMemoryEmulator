using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of GridFS bucket for testing.
/// Stores files and chunks in the backing InMemoryMongoDatabase collections.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/core/gridfs/
///   "GridFS is a specification for storing and retrieving files that exceed the BSON-document
///    size limit of 16 MB. GridFS stores files in two collections: fs.files and fs.chunks."
/// </remarks>
public class InMemoryGridFSBucket
{
    private readonly IMongoDatabase _database;
    private readonly string _bucketName;
    private readonly int _chunkSizeBytes;
    private IMongoCollection<BsonDocument> FilesCollection => _database.GetCollection<BsonDocument>($"{_bucketName}.files");
    private IMongoCollection<BsonDocument> ChunksCollection => _database.GetCollection<BsonDocument>($"{_bucketName}.chunks");

    public InMemoryGridFSBucket(IMongoDatabase database, GridFSBucketOptions? options = null)
    {
        _database = database;
        _bucketName = options?.BucketName ?? "fs";
        _chunkSizeBytes = options?.ChunkSizeBytes ?? 255 * 1024; // 255KB default
    }

    public IMongoDatabase Database => _database;
    public ImmutableGridFSBucketOptions Options => new ImmutableGridFSBucketOptions(
        new GridFSBucketOptions { BucketName = _bucketName, ChunkSizeBytes = _chunkSizeBytes });

    #region Upload

    // Ref: https://www.mongodb.com/docs/manual/core/gridfs/#gridfs-chunks
    //   "Each chunk is identified by its unique ObjectId _id field."
    public ObjectId UploadFromBytes(string filename, byte[] source, GridFSUploadOptions? options = null, CancellationToken cancellationToken = default)
    {
        var fileId = ObjectId.GenerateNewId();
        UploadInternal(fileId, filename, source, options);
        return fileId;
    }

    public Task<ObjectId> UploadFromBytesAsync(string filename, byte[] source, GridFSUploadOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(UploadFromBytes(filename, source, options, cancellationToken));

    public ObjectId UploadFromStream(string filename, Stream source, GridFSUploadOptions? options = null, CancellationToken cancellationToken = default)
    {
        using var ms = new MemoryStream();
        source.CopyTo(ms);
        return UploadFromBytes(filename, ms.ToArray(), options, cancellationToken);
    }

    public Task<ObjectId> UploadFromStreamAsync(string filename, Stream source, GridFSUploadOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(UploadFromStream(filename, source, options, cancellationToken));

    private void UploadInternal(ObjectId fileId, string filename, byte[] data, GridFSUploadOptions? options)
    {
        var chunkSize = options?.ChunkSizeBytes ?? _chunkSizeBytes;
        var metadata = options?.Metadata;

        // Write chunks
        var n = 0;
        for (int offset = 0; offset < data.Length || n == 0; offset += chunkSize)
        {
            var remaining = Math.Min(chunkSize, data.Length - offset);
            var chunkData = remaining > 0 ? new byte[remaining] : Array.Empty<byte>();
            if (remaining > 0) Array.Copy(data, offset, chunkData, 0, remaining);

            ChunksCollection.InsertOne(new BsonDocument
            {
                { "_id", ObjectId.GenerateNewId() },
                { "files_id", fileId },
                { "n", n },
                { "data", new BsonBinaryData(chunkData) }
            });
            n++;
            if (remaining <= 0) break;
        }

        // Write file document
        var fileDoc = new BsonDocument
        {
            { "_id", fileId },
            { "length", (long)data.Length },
            { "chunkSize", chunkSize },
            { "uploadDate", new BsonDateTime(DateTime.UtcNow) },
            { "filename", filename }
        };
        if (metadata != null) fileDoc["metadata"] = metadata;

        FilesCollection.InsertOne(fileDoc);
    }

    #endregion

    #region Download

    public byte[] DownloadAsBytes(ObjectId id, GridFSDownloadOptions? options = null, CancellationToken cancellationToken = default)
    {
        var fileDoc = FilesCollection.Find(Builders<BsonDocument>.Filter.Eq("_id", id)).FirstOrDefault();
        if (fileDoc == null) throw new GridFSFileNotFoundException(id);

        var length = fileDoc["length"].ToInt64();
        var chunks = ChunksCollection.Find(Builders<BsonDocument>.Filter.Eq("files_id", id))
            .Sort(Builders<BsonDocument>.Sort.Ascending("n"))
            .ToList();

        using var ms = new MemoryStream((int)length);
        foreach (var chunk in chunks)
        {
            var data = chunk["data"].AsByteArray;
            ms.Write(data, 0, data.Length);
        }
        return ms.ToArray();
    }

    public Task<byte[]> DownloadAsBytesAsync(ObjectId id, GridFSDownloadOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(DownloadAsBytes(id, options, cancellationToken));

    public void DownloadToStream(ObjectId id, Stream destination, GridFSDownloadOptions? options = null, CancellationToken cancellationToken = default)
    {
        var bytes = DownloadAsBytes(id, options, cancellationToken);
        destination.Write(bytes, 0, bytes.Length);
    }

    public Task DownloadToStreamAsync(ObjectId id, Stream destination, GridFSDownloadOptions? options = null, CancellationToken cancellationToken = default)
    {
        DownloadToStream(id, destination, options, cancellationToken);
        return Task.CompletedTask;
    }

    #endregion

    #region Delete / Rename

    // Ref: https://www.mongodb.com/docs/manual/core/gridfs/#delete-files
    //   "The delete command removes the file document from fs.files and also removes its chunks."
    public void Delete(ObjectId id, CancellationToken cancellationToken = default)
    {
        FilesCollection.DeleteOne(Builders<BsonDocument>.Filter.Eq("_id", id));
        ChunksCollection.DeleteMany(Builders<BsonDocument>.Filter.Eq("files_id", id));
    }

    public Task DeleteAsync(ObjectId id, CancellationToken cancellationToken = default)
    {
        Delete(id, cancellationToken);
        return Task.CompletedTask;
    }

    public void Rename(ObjectId id, string newFilename, CancellationToken cancellationToken = default)
    {
        FilesCollection.UpdateOne(
            Builders<BsonDocument>.Filter.Eq("_id", id),
            Builders<BsonDocument>.Update.Set("filename", newFilename));
    }

    public Task RenameAsync(ObjectId id, string newFilename, CancellationToken cancellationToken = default)
    {
        Rename(id, newFilename, cancellationToken);
        return Task.CompletedTask;
    }

    #endregion

    #region Find

    public IAsyncCursor<GridFSFileInfo> Find(FilterDefinition<GridFSFileInfo> filter, GridFSFindOptions? options = null, CancellationToken cancellationToken = default)
    {
        // Convert GridFSFileInfo filter to BsonDocument filter for our collection
        var serializer = BsonSerializer.LookupSerializer<GridFSFileInfo>();
        var renderedFilter = filter.Render(new RenderArgs<GridFSFileInfo>(serializer, BsonSerializer.SerializerRegistry));
        var docs = FilesCollection.Find(new BsonDocumentFilterDefinition<BsonDocument>(renderedFilter)).ToList();

        var fileInfos = docs.Select(d => DeserializeFileInfo(d)).ToList();
        return new InMemoryAsyncCursor<GridFSFileInfo>(fileInfos);
    }

    public Task<IAsyncCursor<GridFSFileInfo>> FindAsync(FilterDefinition<GridFSFileInfo> filter, GridFSFindOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Find(filter, options, cancellationToken));

    private static GridFSFileInfo DeserializeFileInfo(BsonDocument doc)
    {
        return BsonSerializer.Deserialize<GridFSFileInfo>(doc);
    }

    #endregion

    #region Open Stream

    public GridFSUploadStream OpenUploadStream(string filename, GridFSUploadOptions? options = null, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("OpenUploadStream is not supported in the in-memory emulator. Use UploadFromBytes or UploadFromStream instead.");
    }

    public GridFSUploadStream OpenUploadStream(ObjectId id, string filename, GridFSUploadOptions? options = null, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("OpenUploadStream is not supported in the in-memory emulator.");
    }

    public GridFSDownloadStream OpenDownloadStream(ObjectId id, GridFSDownloadOptions? options = null, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("OpenDownloadStream is not supported in the in-memory emulator. Use DownloadAsBytes or DownloadToStream instead.");
    }

    public GridFSDownloadStream OpenDownloadStreamByName(string filename, GridFSDownloadByNameOptions? options = null, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("OpenDownloadStreamByName is not supported in the in-memory emulator.");
    }

    #endregion

    #region Upload by Id

    public void UploadFromBytes(ObjectId id, string filename, byte[] source, GridFSUploadOptions? options = null, CancellationToken cancellationToken = default)
    {
        UploadInternal(id, filename, source, options);
    }

    public Task UploadFromBytesAsync(ObjectId id, string filename, byte[] source, GridFSUploadOptions? options = null, CancellationToken cancellationToken = default)
    {
        UploadFromBytes(id, filename, source, options, cancellationToken);
        return Task.CompletedTask;
    }

    public void UploadFromStream(ObjectId id, string filename, Stream source, GridFSUploadOptions? options = null, CancellationToken cancellationToken = default)
    {
        using var ms = new MemoryStream();
        source.CopyTo(ms);
        UploadInternal(id, filename, ms.ToArray(), options);
    }

    public Task UploadFromStreamAsync(ObjectId id, string filename, Stream source, GridFSUploadOptions? options = null, CancellationToken cancellationToken = default)
    {
        UploadFromStream(id, filename, source, options, cancellationToken);
        return Task.CompletedTask;
    }

    #endregion

    #region Download By Name

    public byte[] DownloadAsBytesByName(string filename, GridFSDownloadByNameOptions? options = null, CancellationToken cancellationToken = default)
    {
        var fileId = FindFileIdByName(filename);
        return DownloadAsBytes(fileId, null, cancellationToken);
    }

    public Task<byte[]> DownloadAsBytesByNameAsync(string filename, GridFSDownloadByNameOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(DownloadAsBytesByName(filename, options, cancellationToken));

    public void DownloadToStreamByName(string filename, Stream destination, GridFSDownloadByNameOptions? options = null, CancellationToken cancellationToken = default)
    {
        var fileId = FindFileIdByName(filename);
        DownloadToStream(fileId, destination, null, cancellationToken);
    }

    public Task DownloadToStreamByNameAsync(string filename, Stream destination, GridFSDownloadByNameOptions? options = null, CancellationToken cancellationToken = default)
    {
        DownloadToStreamByName(filename, destination, options, cancellationToken);
        return Task.CompletedTask;
    }

    private ObjectId FindFileIdByName(string filename)
    {
        var fileDoc = FilesCollection.Find(Builders<BsonDocument>.Filter.Eq("filename", filename))
            .Sort(Builders<BsonDocument>.Sort.Descending("uploadDate"))
            .FirstOrDefault();
        if (fileDoc == null) throw new GridFSFileNotFoundException(new BsonString(filename));
        return fileDoc["_id"].AsObjectId;
    }

    #endregion

    #region Drop

    public void Drop(CancellationToken cancellationToken = default)
    {
        _database.DropCollection($"{_bucketName}.files", cancellationToken);
        _database.DropCollection($"{_bucketName}.chunks", cancellationToken);
    }

    public Task DropAsync(CancellationToken cancellationToken = default)
    {
        Drop(cancellationToken);
        return Task.CompletedTask;
    }

    #endregion
}
