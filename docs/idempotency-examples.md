# Iceberg REST API Idempotency Examples

This document provides examples of how to use the idempotency feature in the Apache Iceberg REST API.

## Overview

Idempotency keys allow clients to safely retry mutation operations without risk of side effects. When an idempotency key is provided, the server ensures that multiple requests with the same key have identical effects.

## Server-Side Implementation

### Basic Idempotency Handler Usage

```java
import org.apache.iceberg.rest.IdempotencyHandler;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;

public class TableService {
    private final IdempotencyHandler idempotencyHandler = new IdempotencyHandler();
    
    public LoadTableResponse updateTable(
            String idempotencyKey,
            String tablePath,
            UpdateTableRequest request) {
        
        String operationType = "updateTable";
        String resourcePath = tablePath;
        String requestHash = IdempotencyHandler.RequestHashGenerator.generateHash(request);
        
        return idempotencyHandler.handleOperation(
            idempotencyKey, operationType, resourcePath, requestHash,
            () -> performTableUpdate(request));
    }
    
    private LoadTableResponse performTableUpdate(UpdateTableRequest request) {
        // Your actual table update logic here
        // This method is only called once per unique idempotency key
        return LoadTableResponse.builder()
            .withTableMetadata(/* updated metadata */)
            .build();
    }
}
```

### REST Endpoint Integration

```java
import org.apache.iceberg.rest.IdempotencyHandler;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}")
public class TableResource {
    
    private final IdempotencyHandler idempotencyHandler = new IdempotencyHandler();
    
    @POST
    public LoadTableResponse updateTable(
            @PathParam("prefix") String prefix,
            @PathParam("namespace") String namespace,
            @PathParam("table") String table,
            @HeaderParam("Idempotency-Key") String idempotencyKey,
            UpdateTableRequest request) {
        
        String resourcePath = String.format("/v1/%s/namespaces/%s/tables/%s", 
            prefix, namespace, table);
        String requestHash = IdempotencyHandler.RequestHashGenerator.generateHash(request);
        
        return idempotencyHandler.handleOperation(
            idempotencyKey, "updateTable", resourcePath, requestHash,
            () -> tableService.updateTable(namespace, table, request));
    }
}
```

## Client-Side Usage

### Basic Client Usage

```java
import org.apache.iceberg.rest.IdempotencyAwareRESTClient;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;

public class IcebergClient {
    
    public void updateTableExample() {
        // Create an idempotency-aware client
        RESTClient baseClient = HTTPClient.builder()
            .uri("https://catalog.example.com")
            .build();
        IdempotencyAwareRESTClient client = new IdempotencyAwareRESTClient(baseClient);
        
        UpdateTableRequest request = UpdateTableRequest.builder()
            .withUpdate(/* your updates */)
            .build();
        
        // Option 1: Automatic idempotency key generation
        LoadTableResponse response = client.post(
            "/v1/warehouse/namespaces/sales/tables/orders",
            request,
            LoadTableResponse.class,
            Map.of("Authorization", "Bearer " + token),
            ErrorHandlers.tableErrorHandler());
        
        // Option 2: Specify your own idempotency key
        String idempotencyKey = UUID.randomUUID().toString();
        LoadTableResponse response2 = client.postWithIdempotencyKey(
            "/v1/warehouse/namespaces/sales/tables/orders",
            request,
            LoadTableResponse.class,
            Map.of("Authorization", "Bearer " + token),
            ErrorHandlers.tableErrorHandler(),
            idempotencyKey);
    }
}
```

### Retry Scenarios

```java
import org.apache.iceberg.rest.IdempotencyAwareRESTClient;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import java.util.concurrent.TimeUnit;

public class RetryExample {
    
    public LoadTableResponse updateTableWithRetry(
            IdempotencyAwareRESTClient client,
            UpdateTableRequest request) {
        
        // Create an idempotent operation context
        IdempotencyAwareRESTClient.IdempotentOperationContext context = 
            client.createIdempotentOperation();
        
        int maxRetries = 3;
        int retryDelayMs = 1000;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                // All retries use the same idempotency key
                return context.post(
                    "/v1/warehouse/namespaces/sales/tables/orders",
                    request,
                    LoadTableResponse.class,
                    Map.of("Authorization", "Bearer " + token),
                    ErrorHandlers.tableErrorHandler());
                    
            } catch (ServiceUnavailableException e) {
                if (attempt == maxRetries) {
                    throw e; // Give up after max retries
                }
                
                System.out.println("Attempt " + attempt + " failed, retrying with same key: " + 
                    context.getIdempotencyKey());
                
                try {
                    Thread.sleep(retryDelayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
        
        throw new RuntimeException("Should not reach here");
    }
}
```

### Pipeline Integration

```java
import org.apache.iceberg.rest.IdempotencyAwareRESTClient;

public class DataPipeline {
    
    public void processDataWithIdempotency() {
        IdempotencyAwareRESTClient client = createClient();
        
        // Step 1: Create table (if needed)
        String tableCreationKey = generateBusinessKey("create-orders-table", "2024-01-15");
        try {
            CreateTableResponse tableResponse = client.postWithIdempotencyKey(
                "/v1/warehouse/namespaces/sales/tables",
                createTableRequest(),
                CreateTableResponse.class,
                headers(),
                ErrorHandlers.tableErrorHandler(),
                tableCreationKey);
        } catch (AlreadyExistsException e) {
            // Table already exists, continue
        }
        
        // Step 2: Update table with new data
        String updateKey = generateBusinessKey("update-orders-table", "2024-01-15", "batch-123");
        LoadTableResponse updateResponse = client.postWithIdempotencyKey(
            "/v1/warehouse/namespaces/sales/tables/orders",
            updateTableRequest(),
            LoadTableResponse.class,
            headers(),
            ErrorHandlers.tableErrorHandler(),
            updateKey);
        
        // Step 3: Report metrics
        String metricsKey = generateBusinessKey("report-metrics", "2024-01-15", "batch-123");
        client.postWithIdempotencyKey(
            "/v1/warehouse/namespaces/sales/tables/orders/metrics",
            metricsRequest(),
            ReportMetricsResponse.class,
            headers(),
            ErrorHandlers.defaultErrorHandler(),
            metricsKey);
    }
    
    private String generateBusinessKey(String operation, String date, String... params) {
        // Generate business-meaningful idempotency keys
        String combined = operation + "-" + date + "-" + String.join("-", params);
        return UUID.nameUUIDFromBytes(combined.getBytes()).toString();
    }
}
```

## Error Handling

### Handling Idempotency Key Conflicts

```java
import org.apache.iceberg.exceptions.IdempotencyKeyConflictException;

public class ConflictHandlingExample {
    
    public LoadTableResponse updateTableSafely(
            IdempotencyAwareRESTClient client,
            UpdateTableRequest request) {
        
        String idempotencyKey = UUID.randomUUID().toString();
        
        try {
            return client.postWithIdempotencyKey(
                "/v1/warehouse/namespaces/sales/tables/orders",
                request,
                LoadTableResponse.class,
                headers(),
                ErrorHandlers.tableErrorHandler(),
                idempotencyKey);
                
        } catch (IdempotencyKeyConflictException e) {
            // This key was used for a different operation
            // Generate a new key and retry
            System.out.println("Key conflict detected, generating new key");
            
            String newKey = UUID.randomUUID().toString();
            return client.postWithIdempotencyKey(
                "/v1/warehouse/namespaces/sales/tables/orders",
                request,
                LoadTableResponse.class,
                headers(),
                ErrorHandlers.tableErrorHandler(),
                newKey);
        }
    }
}
```

## Best Practices

### 1. Key Generation Strategies

```java
// Good: Use UUIDs for unique operations
String key = UUID.randomUUID().toString();

// Good: Use business-meaningful keys for deterministic operations
String businessKey = UUID.nameUUIDFromBytes(
    ("update-" + tableId + "-" + date + "-" + batchId).getBytes()).toString();

// Good: Use timestamp-based keys for time-series operations
String timeKey = "operation-" + Instant.now().getEpochSecond();

// Bad: Predictable or reused keys
String badKey = "fixed-key"; // Don't reuse the same key
```

### 2. Request Hashing

```java
// Use the built-in request hash generator
String requestHash = IdempotencyHandler.RequestHashGenerator.generateHash(request);

// Or implement custom hashing for sensitive data
public class SecureRequestHashGenerator {
    public static String generateHash(Object request) {
        // Implement secure hashing that excludes sensitive fields
        // and focuses on semantically important fields
        return DigestUtils.sha256Hex(serializeRequest(request));
    }
}
```

### 3. Configuration

```java
// Configure expiration time based on your use case
IdempotencyHandler handler = new IdempotencyHandler(
    Duration.ofHours(24).toMillis()); // 24 hour expiration

// For high-volume systems, consider shorter expiration
IdempotencyHandler highVolumeHandler = new IdempotencyHandler(
    Duration.ofHours(1).toMillis()); // 1 hour expiration
```

### 4. Monitoring

```java
public class IdempotencyMetrics {
    private final IdempotencyHandler handler;
    private final MetricRegistry metrics;
    
    public void recordMetrics() {
        // Monitor key store size
        metrics.gauge("idempotency.keys.stored", () -> handler.getStoredKeyCount());
        
        // Monitor key conflicts (implement in your error handler)
        metrics.counter("idempotency.conflicts").increment();
        
        // Monitor cache hit rate
        metrics.counter("idempotency.cache.hits").increment();
    }
}
```

## Testing Idempotency

```java
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class IdempotencyTest {
    
    @Test
    void testIdempotentTableUpdate() {
        IdempotencyHandler handler = new IdempotencyHandler();
        String key = "test-key-123";
        
        AtomicInteger callCount = new AtomicInteger(0);
        
        // First call
        String result1 = handler.handleOperation(
            key, "updateTable", "/v1/tables/test", "hash123",
            () -> {
                callCount.incrementAndGet();
                return "result-1";
            });
        
        // Second call should return cached result
        String result2 = handler.handleOperation(
            key, "updateTable", "/v1/tables/test", "hash123",
            () -> {
                callCount.incrementAndGet();
                return "result-2";
            });
        
        assertThat(result1).isEqualTo("result-1");
        assertThat(result2).isEqualTo("result-1"); // Same as first
        assertThat(callCount.get()).isEqualTo(1); // Only called once
    }
}
```

## HTTP Examples

### Successful Idempotent Request

```http
POST /v1/warehouse/namespaces/sales/tables/orders HTTP/1.1
Host: catalog.example.com
Authorization: Bearer token123
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000
Content-Type: application/json

{
  "requirements": [
    {
      "type": "assert-current-schema-id",
      "current-schema-id": 1
    }
  ],
  "updates": [
    {
      "action": "add-schema",
      "schema": {
        "type": "struct",
        "fields": [...]
      }
    }
  ]
}
```

### Response (First Time)

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "metadata-location": "s3://bucket/warehouse/sales/orders/metadata/v2.json",
  "metadata": {
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    ...
  }
}
```

### Retry with Same Key

```http
POST /v1/warehouse/namespaces/sales/tables/orders HTTP/1.1
Host: catalog.example.com
Authorization: Bearer token123
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000
Content-Type: application/json

{
  "requirements": [
    {
      "type": "assert-current-schema-id", 
      "current-schema-id": 1
    }
  ],
  "updates": [
    {
      "action": "add-schema",
      "schema": {
        "type": "struct",
        "fields": [...]
      }
    }
  ]
}
```

### Response (Cached)

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "metadata-location": "s3://bucket/warehouse/sales/orders/metadata/v2.json",
  "metadata": {
    "format-version": 2,
    "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
    ...
  }
}
```

### Conflict Example

```http
POST /v1/warehouse/namespaces/sales/tables/orders HTTP/1.1
Host: catalog.example.com
Authorization: Bearer token123
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000
Content-Type: application/json

{
  "requirements": [
    {
      "type": "assert-current-schema-id",
      "current-schema-id": 2
    }
  ],
  "updates": [
    {
      "action": "drop-schema",
      "schema-id": 1
    }
  ]
}
```

### Conflict Response

```http
HTTP/1.1 409 Conflict
Content-Type: application/json

{
  "error": {
    "message": "Idempotency key '550e8400-e29b-41d4-a716-446655440000' was already used for a different operation",
    "type": "IdempotencyKeyConflictException",
    "code": 409
  }
}
```