# Zerobus vs. Kafka-Style Architecture comparison

## Architecture

| Aspect | Zerobus | Kafka-Style Architecture |
|---|---|---|
| Ingestion path | Producer → Zerobus → Delta | Producer → Broker → Consumer → Delta |
| Intermediate broker | Not required | Required |
| Streaming consumer | Not required for direct ingestion | Required |
| Infrastructure complexity | Lower | Higher |
| Producer-consumer coupling | More directly tied to the destination | Loosely coupled through the broker |
| Event buffering | No separate Kafka-style broker buffer | Built-in broker buffering |
| Event retention and replay | Requires separate recovery/replay strategy | Supported through broker retention and offsets |
| Multiple consumers | Requires additional downstream mechanisms | Native consumer groups and independent subscriptions |
| Independent scaling | More limited separation of ingestion components | Producers, brokers, and consumers scale independently |
| Latency | Fewer ingestion stages | Additional broker and consumer stages |
| Fault recovery | Producer retries and Zerobus ingestion recovery | Broker retention, consumer offsets, and retries |
| Idempotency | Requires stable event IDs and idempotent destination processing | Also requires idempotent destination processing |
| Operational overhead | Lower | Higher |
| Infrastructure cost | No separately managed message broker | Broker infrastructure and consumer compute costs |

## Cost and Operational Trade-offs

### Zerobus

**Pros:**
- Eliminates the need to provision and maintain a separate message broker.
- Reduces the number of infrastructure components.
- Removes the need for a dedicated streaming consumer just to transfer events into Delta.
- Simplifies deployment, monitoring, and troubleshooting.
- Can be cost-effective when Databricks is already the primary data platform and events are destined directly for Delta.

**Cons:**
- Creates a stronger dependency on Databricks as the ingestion destination.
- Does not provide the same general-purpose buffering, retention, and replay capabilities as Kafka.
- Multiple independent consumers require additional architecture.
- Requires appropriate Databricks connectivity, authentication, and Unity Catalog permissions.
- Ingestion throughput and pricing are subject to Zerobus service limits and pricing.

### Kafka-Style Architecture

**Pros:**
- Provides a general-purpose event distribution layer independent of the destination storage system.
- Supports multiple independent consumers and downstream applications.
- Allows consumers to recover from failures by reading retained events.
- Supports buffering during temporary downstream outages or processing slowdowns.
- Enables different applications to process the same events independently.

**Cons:**
- Requires additional infrastructure and configuration.
- Introduces broker management, monitoring, and maintenance responsibilities.
- Requires a consumer application to transfer events into Delta.
- Increases the number of potential failure points.
- Can introduce additional latency and infrastructure costs.

### When to choose each

**Zerobus is better when:**
- Databricks is the primary destination.
- Events are ingested directly into the Databricks platform.
- The architecture does not require multiple consumers.
- Simplicity and reduced operational overhead are priorities.

**Kafka is better when:**
- Events need to be distributed to multiple applications or platforms.
- Producers and consumers must remain independently deployable.
- Long-lived buffering and replay are important requirements.
- The system needs to support different consumers.
