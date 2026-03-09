# .NET/C# Kafka Producer Guide

Complete guide for creating Kafka producers in .NET/C# using Confluent.Kafka with Schema Registry and Avro serialization.

## Installation

### Using NuGet Package Manager

```bash
dotnet add package Confluent.Kafka
dotnet add package Confluent.SchemaRegistry.Serdes.Avro
dotnet add package DotNetEnv
```

### Package References (.csproj)

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Confluent.Kafka" Version="2.3.0" />
    <PackageReference Include="Confluent.SchemaRegistry.Serdes.Avro" Version="2.3.0" />
    <PackageReference Include="DotNetEnv" Version="3.0.0" />
  </ItemGroup>
</Project>
```

---

## Avro Schema and Code Generation

### Define Schema (User.avsc)

```json
{
  "type": "record",
  "name": "User",
  "namespace": "Com.Example.Models",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "createdAt", "type": "long", "logicalType": "timestamp-millis"}
  ]
}
```

### Generate C# Classes

Use Apache Avro Tools:
```bash
dotnet tool install --global Apache.Avro.Tools
avrogen -s User.avsc .
```

Or use the Avro.msbuild package for automatic generation during build.

---

## Producer Implementation

### Configuration Loader

```csharp
using DotNetEnv;
using Confluent.Kafka;
using Confluent.SchemaRegistry;

public class ProducerConfig
{
    public enum OptimizationProfile
    {
        Throughput,
        Latency,
        Durability,
        Availability,
        Freight,
        Balanced
    }

    public static ProducerConfig<string, User> GetProducerConfig(OptimizationProfile profile)
    {
        Env.Load();

        var config = new ProducerConfig
        {
            BootstrapServers = Environment.GetEnvironmentVariable("BOOTSTRAP_SERVERS"),
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = Environment.GetEnvironmentVariable("SASL_USERNAME"),
            SaslPassword = Environment.GetEnvironmentVariable("SASL_PASSWORD"),
            ClientId = Environment.GetEnvironmentVariable("CLIENT_ID") ?? "dotnet-producer",
        };

        ApplyOptimizationProfile(config, profile);

        return new ProducerConfig<string, User>
        {
            // Copy all properties from base config
            BootstrapServers = config.BootstrapServers,
            SecurityProtocol = config.SecurityProtocol,
            SaslMechanism = config.SaslMechanism,
            SaslUsername = config.SaslUsername,
            SaslPassword = config.SaslPassword,
            ClientId = config.ClientId,
            Acks = config.Acks,
            // ... other properties
        };
    }

    public static SchemaRegistryConfig GetSchemaRegistryConfig()
    {
        return new SchemaRegistryConfig
        {
            Url = Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL"),
            BasicAuthUserInfo = $"{Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_API_KEY")}:{Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_API_SECRET")}"
        };
    }

    private static void ApplyOptimizationProfile(ProducerConfig config, OptimizationProfile profile)
    {
        switch (profile)
        {
            case OptimizationProfile.Throughput:
                config.BatchSize = 65536;
                config.LingerMs = 100;
                config.CompressionType = CompressionType.Lz4;
                config.Acks = Acks.Leader;
                break;

            case OptimizationProfile.Latency:
                config.LingerMs = 0;
                config.Acks = Acks.Leader;
                config.CompressionType = CompressionType.None;
                config.BatchSize = 16384;
                break;

            case OptimizationProfile.Durability:
                config.Acks = Acks.All;
                config.EnableIdempotence = true;
                config.MessageSendMaxRetries = int.MaxValue;
                config.MaxInFlight = 5;
                break;

            case OptimizationProfile.Freight:
                config.EnableIdempotence = false;
                config.LingerMs = 100;
                config.BatchSize = 1048576;
                break;

            case OptimizationProfile.Balanced:
            default:
                config.Acks = Acks.All;
                config.EnableIdempotence = true;
                config.BatchSize = 16384;
                config.LingerMs = 10;
                config.CompressionType = CompressionType.Lz4;
                break;
        }
    }
}
```

### Complete Producer Class

```csharp
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Com.Example.Models;
using System;
using System.Threading.Tasks;

public class UserProducer : IDisposable
{
    private readonly IProducer<string, User> _producer;
    private readonly string _topicName;
    private long _successCount = 0;
    private long _failureCount = 0;

    public UserProducer(ProducerConfig.OptimizationProfile profile)
    {
        var producerConfig = ProducerConfig.GetProducerConfig(profile);
        var schemaRegistryConfig = ProducerConfig.GetSchemaRegistryConfig();

        var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);

        _producer = new ProducerBuilder<string, User>(producerConfig)
            .SetValueSerializer(new AvroSerializer<User>(schemaRegistryClient))
            .Build();

        _topicName = Environment.GetEnvironmentVariable("TOPIC_NAME") ?? "users";

        Console.WriteLine($"UserProducer initialized with profile: {profile}");
    }

    public async Task<DeliveryResult<string, User>> ProduceAsync(User user)
    {
        try
        {
            var message = new Message<string, User>
            {
                Key = user.id,
                Value = user
            };

            var deliveryResult = await _producer.ProduceAsync(_topicName, message);

            Interlocked.Increment(ref _successCount);

            Console.WriteLine($"✅ Message delivered: topic={deliveryResult.Topic}, " +
                            $"partition={deliveryResult.Partition}, offset={deliveryResult.Offset}");

            return deliveryResult;
        }
        catch (ProduceException<string, User> ex)
        {
            Interlocked.Increment(ref _failureCount);
            Console.WriteLine($"❌ Failed to produce message: {ex.Error.Reason}");
            throw;
        }
    }

    public void ProduceWithCallback(User user, Action<DeliveryReport<string, User>>? callback = null)
    {
        var message = new Message<string, User>
        {
            Key = user.id,
            Value = user
        };

        _producer.Produce(_topicName, message, deliveryReport =>
        {
            if (deliveryReport.Error.IsError)
            {
                Interlocked.Increment(ref _failureCount);
                Console.WriteLine($"❌ Delivery failed: {deliveryReport.Error.Reason}");
            }
            else
            {
                Interlocked.Increment(ref _successCount);
                Console.WriteLine($"✅ Message delivered: partition={deliveryReport.Partition}, " +
                                $"offset={deliveryReport.Offset}");
            }

            callback?.Invoke(deliveryReport);
        });
    }

    public void Flush(TimeSpan? timeout = null)
    {
        Console.WriteLine("Flushing pending messages...");
        _producer.Flush(timeout ?? TimeSpan.FromSeconds(30));
        Console.WriteLine($"Flush complete. Success: {_successCount}, Failures: {_failureCount}");
    }

    public (long Success, long Failures) GetMetrics()
    {
        return (_successCount, _failureCount);
    }

    public void Dispose()
    {
        Console.WriteLine("Closing producer...");
        Flush();
        _producer.Dispose();
        Console.WriteLine($"Producer closed. Final stats - Success: {_successCount}, Failures: {_failureCount}");
    }
}
```

### Example Application

```csharp
using Com.Example.Models;
using System;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        using var producer = new UserProducer(ProducerConfig.OptimizationProfile.Durability);

        try
        {
            var user1 = new User
            {
                id = "user-001",
                email = "alice@example.com",
                createdAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            var user2 = new User
            {
                id = "user-002",
                email = "bob@example.com",
                createdAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            // Async produce
            await producer.ProduceAsync(user1);
            await producer.ProduceAsync(user2);

            // Or with callback
            // producer.ProduceWithCallback(user1, report => {
            //     Console.WriteLine($"Callback: {report.Status}");
            // });

            producer.Flush();

            var (success, failures) = producer.GetMetrics();
            Console.WriteLine($"Final metrics - Success: {success}, Failures: {failures}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            Environment.Exit(1);
        }
    }
}
```

---

## Best Practices

1. **Use `using` statements**: Ensure proper disposal
2. **Flush before shutdown**: Prevents message loss
3. **Handle delivery reports**: Monitor success/failure
4. **Use async/await**: For better performance with async operations
5. **Configure timeouts**: Balance retries and responsiveness
6. **Secure credentials**: Use environment variables or Azure Key Vault
7. **Monitor metrics**: Track success/failure rates
8. **Implement retry logic**: With exponential backoff for transient errors

---

## Common Errors

- **Authentication failed**: Check SASL credentials
- **Schema incompatible**: Verify Schema Registry compatibility mode
- **Connection timeout**: Check bootstrap servers and network
- **Buffer full**: Increase buffer.memory or call Flush() more frequently
