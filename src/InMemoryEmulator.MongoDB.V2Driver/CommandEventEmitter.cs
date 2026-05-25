using System.Diagnostics;
using System.Net;
using global::MongoDB.Bson;
using global::MongoDB.Driver;
using global::MongoDB.Driver.Core.Clusters;
using global::MongoDB.Driver.Core.Connections;
using global::MongoDB.Driver.Core.Events;
using global::MongoDB.Driver.Core.Servers;

namespace InMemoryEmulator.MongoDB;

/// <summary>
/// Emits synthetic MongoDB driver command monitoring events (CommandStartedEvent,
/// CommandSucceededEvent, CommandFailedEvent) for operations on in-memory collections.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/drivers/csharp/current/fundamentals/logging/
///   "The driver raises CommandStartedEvent, CommandSucceededEvent, and CommandFailedEvent
///    for each command sent to the server."
///
/// This mirrors how the real MongoDB driver raises events through the ClusterConfigurator
/// event subscription mechanism.
/// </remarks>
internal sealed class CommandEventEmitter
{
    private readonly Action<CommandStartedEvent>? _startedHandler;
    private readonly Action<CommandSucceededEvent>? _succeededHandler;
    private readonly Action<CommandFailedEvent>? _failedHandler;
    private readonly ConnectionId _connectionId;
    private int _requestId;

    public CommandEventEmitter(Action<CommandEventSubscriptionBuilder> configurator)
    {
        var builder = new CommandEventSubscriptionBuilder();
        configurator(builder);

        _startedHandler = builder.GetHandler<CommandStartedEvent>();
        _succeededHandler = builder.GetHandler<CommandSucceededEvent>();
        _failedHandler = builder.GetHandler<CommandFailedEvent>();

        // Create a synthetic ConnectionId for all events from this emulator instance
        // Ref: https://www.mongodb.com/docs/manual/reference/command/
        //   ConnectionId identifies the source of a command — we use a synthetic one for in-memory.
        var clusterId = new ClusterId(1);
        var serverId = new ServerId(clusterId, new DnsEndPoint("localhost", 27017));
        _connectionId = new ConnectionId(serverId);
    }

    public bool HasSubscribers => _startedHandler != null || _succeededHandler != null || _failedHandler != null;

    /// <summary>
    /// Emits command events around an operation. Fires Started before, Succeeded/Failed after.
    /// </summary>
    public void EmitCommandEvents(
        string commandName,
        DatabaseNamespace databaseNamespace,
        Func<BsonDocument> commandBuilder,
        Action operation,
        Func<BsonDocument>? replyBuilder = null)
    {
        var requestId = Interlocked.Increment(ref _requestId);
        var command = commandBuilder();

        _startedHandler?.Invoke(new CommandStartedEvent(
            commandName,
            command,
            databaseNamespace,
            operationId: 0,
            requestId,
            _connectionId));

        var sw = Stopwatch.StartNew();
        try
        {
            operation();
            sw.Stop();

            var reply = replyBuilder?.Invoke() ?? new BsonDocument("ok", 1);

            _succeededHandler?.Invoke(new CommandSucceededEvent(
                commandName,
                reply,
                databaseNamespace,
                operationId: 0,
                requestId,
                _connectionId,
                sw.Elapsed));
        }
        catch (Exception ex)
        {
            sw.Stop();

            _failedHandler?.Invoke(new CommandFailedEvent(
                commandName,
                databaseNamespace,
                ex,
                operationId: 0,
                requestId,
                _connectionId,
                sw.Elapsed));

            throw;
        }
    }

    /// <summary>
    /// Emits command events around an operation that returns a value.
    /// </summary>
    public T EmitCommandEvents<T>(
        string commandName,
        DatabaseNamespace databaseNamespace,
        Func<BsonDocument> commandBuilder,
        Func<T> operation,
        Func<T, BsonDocument>? replyBuilder = null)
    {
        var requestId = Interlocked.Increment(ref _requestId);
        var command = commandBuilder();

        _startedHandler?.Invoke(new CommandStartedEvent(
            commandName,
            command,
            databaseNamespace,
            operationId: 0,
            requestId,
            _connectionId));

        var sw = Stopwatch.StartNew();
        try
        {
            var result = operation();
            sw.Stop();

            var reply = replyBuilder?.Invoke(result) ?? new BsonDocument("ok", 1);

            _succeededHandler?.Invoke(new CommandSucceededEvent(
                commandName,
                reply,
                databaseNamespace,
                operationId: 0,
                requestId,
                _connectionId,
                sw.Elapsed));

            return result;
        }
        catch (Exception ex)
        {
            sw.Stop();

            _failedHandler?.Invoke(new CommandFailedEvent(
                commandName,
                databaseNamespace,
                ex,
                operationId: 0,
                requestId,
                _connectionId,
                sw.Elapsed));

            throw;
        }
    }
}

/// <summary>
/// Builder for subscribing to command monitoring events.
/// Mirrors the Subscribe API of <see cref="ClusterBuilder"/> for familiarity.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/drivers/csharp/current/fundamentals/logging/
///   "Use MongoClientSettings.ClusterConfigurator to subscribe to command events."
///
/// This provides the same Subscribe&lt;TEvent&gt; and Subscribe(IEventSubscriber)
/// patterns that MongoDB developers are already familiar with.
/// </remarks>
public sealed class CommandEventSubscriptionBuilder
{
    private readonly List<IEventSubscriber> _subscribers = new();
    private readonly Dictionary<Type, Delegate> _handlers = new();

    /// <summary>
    /// Subscribes to events of type <typeparamref name="TEvent"/>.
    /// </summary>
    public CommandEventSubscriptionBuilder Subscribe<TEvent>(Action<TEvent> handler)
    {
        if (_handlers.TryGetValue(typeof(TEvent), out var existing))
        {
            var combined = (Action<TEvent>)Delegate.Combine((Action<TEvent>)existing, handler)!;
            _handlers[typeof(TEvent)] = combined;
        }
        else
        {
            _handlers[typeof(TEvent)] = handler;
        }
        return this;
    }

    /// <summary>
    /// Subscribes the specified event subscriber.
    /// </summary>
    public CommandEventSubscriptionBuilder Subscribe(IEventSubscriber subscriber)
    {
        _subscribers.Add(subscriber);
        return this;
    }

    internal Action<TEvent>? GetHandler<TEvent>()
    {
        Action<TEvent>? result = null;

        if (_handlers.TryGetValue(typeof(TEvent), out var handler))
        {
            result = (Action<TEvent>)handler;
        }

        foreach (var subscriber in _subscribers)
        {
            if (subscriber.TryGetEventHandler<TEvent>(out var subscriberHandler))
            {
                result = result == null
                    ? subscriberHandler
                    : (Action<TEvent>)Delegate.Combine(result, subscriberHandler)!;
            }
        }

        return result;
    }
}
