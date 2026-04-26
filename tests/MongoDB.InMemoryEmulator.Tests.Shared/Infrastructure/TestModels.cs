using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// General-purpose test document with common field types.
/// </summary>
public class TestDoc
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string? Id { get; set; }

    public string? Name { get; set; }
    public int Value { get; set; }
    public bool IsActive { get; set; }
    public List<string> Tags { get; set; } = [];

    public NestedDoc? Nested { get; set; }
}

/// <summary>
/// Nested sub-document for testing dot notation and embedded objects.
/// </summary>
public class NestedDoc
{
    public string? Description { get; set; }
    public double Score { get; set; }
}

/// <summary>
/// Test document for order-like scenarios (numeric fields, dates).
/// </summary>
public class OrderDoc
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string? Id { get; set; }

    public string? CustomerId { get; set; }
    public decimal Total { get; set; }
    public int Quantity { get; set; }
    public string? Status { get; set; }
    public DateTime CreatedAt { get; set; }
    public List<OrderItem> Items { get; set; } = [];
}

public class OrderItem
{
    public string? ProductName { get; set; }
    public decimal Price { get; set; }
    public int Quantity { get; set; }
}

/// <summary>
/// Test document for user-like scenarios (unique fields, nested address).
/// </summary>
public class UserDoc
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string? Id { get; set; }

    public string? Username { get; set; }
    public string? Email { get; set; }
    public int Age { get; set; }
    public Address? Address { get; set; }
}

public class Address
{
    public string? Street { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? ZipCode { get; set; }
}
