/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.exceptions.IdempotencyKeyConflictException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test demonstrating end-to-end idempotency behavior. This test simulates the
 * interaction between client and server components.
 */
public class TestIdempotencyIntegration {

  private IdempotencyHandler serverHandler;
  private IdempotencyAwareRESTClient.SequentialIdempotencyKeyGenerator keyGenerator;
  private AtomicInteger operationCounter;
  private TableMetadata testTableMetadata;

  @BeforeEach
  void before() {
    serverHandler = new IdempotencyHandler();
    keyGenerator = new IdempotencyAwareRESTClient.SequentialIdempotencyKeyGenerator("integration-");
    operationCounter = new AtomicInteger(0);

    // Create a simple test schema and table metadata
    Schema testSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    testTableMetadata =
        TableMetadata.newTableMetadata(
            testSchema,
            org.apache.iceberg.PartitionSpec.unpartitioned(),
            "s3://test-bucket/test-table",
            org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap.of());
  }

  @Test
  void testCompleteIdempotencyFlow() {
    String idempotencyKey = keyGenerator.generate();
    String operationType = "updateTable";
    String resourcePath = "/v1/warehouse/namespaces/sales/tables/orders";

    // Simulate table update request
    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(new UpdateRequirement.AssertTableUUID(testTableMetadata.uuid())),
            ImmutableList.of(new MetadataUpdate.SetCurrentSchema(-1)));

    String requestHash = IdempotencyHandler.RequestHashGenerator.generateHash(request);

    // First request - should execute operation
    LoadTableResponse response1 =
        serverHandler.handleOperation(
            idempotencyKey,
            operationType,
            resourcePath,
            requestHash,
            () -> simulateTableUpdate(request));

    // Second request with same key - should return cached result
    LoadTableResponse response2 =
        serverHandler.handleOperation(
            idempotencyKey,
            operationType,
            resourcePath,
            requestHash,
            () -> simulateTableUpdate(request));

    // Third request with same key - should still return cached result
    LoadTableResponse response3 =
        serverHandler.handleOperation(
            idempotencyKey,
            operationType,
            resourcePath,
            requestHash,
            () -> simulateTableUpdate(request));

    // Verify all responses are identical
    assertThat(response1).isEqualTo(response2);
    assertThat(response2).isEqualTo(response3);

    // Verify operation was only executed once
    assertThat(operationCounter.get()).isEqualTo(1);
  }

  @Test
  void testConflictingOperations() {
    String idempotencyKey = keyGenerator.generate();
    String operationType = "updateTable";
    String resourcePath = "/v1/warehouse/namespaces/sales/tables/orders";

    // First operation
    UpdateTableRequest request1 =
        new UpdateTableRequest(
            ImmutableList.of(new UpdateRequirement.AssertTableUUID(testTableMetadata.uuid())),
            ImmutableList.of(new MetadataUpdate.SetCurrentSchema(-1)));

    String requestHash1 = IdempotencyHandler.RequestHashGenerator.generateHash(request1);

    // Execute first operation
    LoadTableResponse response1 =
        serverHandler.handleOperation(
            idempotencyKey,
            operationType,
            resourcePath,
            requestHash1,
            () -> simulateTableUpdate(request1));

    // Second operation with different request but same key
    UpdateTableRequest request2 =
        new UpdateTableRequest(
            ImmutableList.of(new UpdateRequirement.AssertTableUUID(testTableMetadata.uuid())),
            ImmutableList.of(new MetadataUpdate.SetDefaultPartitionSpec(-1)));

    String requestHash2 = IdempotencyHandler.RequestHashGenerator.generateHash(request2);

    // Should throw conflict exception
    assertThatThrownBy(
            () ->
                serverHandler.handleOperation(
                    idempotencyKey,
                    operationType,
                    resourcePath,
                    requestHash2,
                    () -> simulateTableUpdate(request2)))
        .isInstanceOf(IdempotencyKeyConflictException.class)
        .hasMessageContaining("was already used for a different operation");

    // First operation should still return cached result
    LoadTableResponse response1Again =
        serverHandler.handleOperation(
            idempotencyKey,
            operationType,
            resourcePath,
            requestHash1,
            () -> simulateTableUpdate(request1));

    assertThat(response1Again).isEqualTo(response1);
    assertThat(operationCounter.get()).isEqualTo(1); // Only first operation executed
  }

  @Test
  void testRetryScenario() {
    String idempotencyKey = keyGenerator.generate();
    String operationType = "updateTable";
    String resourcePath = "/v1/warehouse/namespaces/sales/tables/orders";

    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(new UpdateRequirement.AssertTableUUID(testTableMetadata.uuid())),
            ImmutableList.of(new MetadataUpdate.SetCurrentSchema(-1)));

    String requestHash = IdempotencyHandler.RequestHashGenerator.generateHash(request);

    // Simulate network timeout scenario - operation succeeds but client doesn't get response
    LoadTableResponse originalResponse =
        serverHandler.handleOperation(
            idempotencyKey,
            operationType,
            resourcePath,
            requestHash,
            () -> simulateTableUpdate(request));

    // Client retries with same idempotency key (as it should)
    LoadTableResponse retryResponse =
        serverHandler.handleOperation(
            idempotencyKey,
            operationType,
            resourcePath,
            requestHash,
            () -> simulateTableUpdate(request));

    // Should get the same response without re-executing the operation
    assertThat(retryResponse).isEqualTo(originalResponse);
    assertThat(operationCounter.get()).isEqualTo(1);
  }

  @Test
  void testDifferentResourcePaths() {
    String idempotencyKey = keyGenerator.generate();
    String operationType = "updateTable";

    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(new UpdateRequirement.AssertTableUUID(testTableMetadata.uuid())),
            ImmutableList.of(new MetadataUpdate.SetCurrentSchema(-1)));

    String requestHash = IdempotencyHandler.RequestHashGenerator.generateHash(request);

    // Same key can be used for different resources
    LoadTableResponse response1 =
        serverHandler.handleOperation(
            idempotencyKey,
            operationType,
            "/v1/warehouse/namespaces/sales/tables/orders",
            requestHash,
            () -> simulateTableUpdate(request));

    LoadTableResponse response2 =
        serverHandler.handleOperation(
            idempotencyKey,
            operationType,
            "/v1/warehouse/namespaces/sales/tables/customers",
            requestHash,
            () -> simulateTableUpdate(request));

    // Should be different responses from different operations
    assertThat(response1).isNotEqualTo(response2);
    assertThat(operationCounter.get()).isEqualTo(2); // Both operations executed
  }

  @Test
  void testDifferentOperationTypes() {
    String idempotencyKey = keyGenerator.generate();
    String resourcePath = "/v1/warehouse/namespaces/sales/tables/orders";

    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(new UpdateRequirement.AssertTableUUID(testTableMetadata.uuid())),
            ImmutableList.of(new MetadataUpdate.SetCurrentSchema(-1)));

    String requestHash = IdempotencyHandler.RequestHashGenerator.generateHash(request);

    // Same key can be used for different operation types
    LoadTableResponse response1 =
        serverHandler.handleOperation(
            idempotencyKey,
            "updateTable",
            resourcePath,
            requestHash,
            () -> simulateTableUpdate(request));

    LoadTableResponse response2 =
        serverHandler.handleOperation(
            idempotencyKey,
            "createTable",
            resourcePath,
            requestHash,
            () -> simulateTableUpdate(request));

    // Should be different responses from different operations
    assertThat(response1).isNotEqualTo(response2);
    assertThat(operationCounter.get()).isEqualTo(2); // Both operations executed
  }

  @Test
  void testFailedOperationRetry() {
    String idempotencyKey = keyGenerator.generate();
    String operationType = "updateTable";
    String resourcePath = "/v1/warehouse/namespaces/sales/tables/orders";

    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(new UpdateRequirement.AssertTableUUID(testTableMetadata.uuid())),
            ImmutableList.of(new MetadataUpdate.SetCurrentSchema(-1)));

    String requestHash = IdempotencyHandler.RequestHashGenerator.generateHash(request);

    // First attempt fails
    assertThatThrownBy(
            () ->
                serverHandler.handleOperation(
                    idempotencyKey,
                    operationType,
                    resourcePath,
                    requestHash,
                    () -> {
                      operationCounter.incrementAndGet();
                      throw new RuntimeException("Temporary failure");
                    }))
        .hasMessage("Temporary failure");

    // Retry should work (failures are not cached)
    LoadTableResponse response =
        serverHandler.handleOperation(
            idempotencyKey,
            operationType,
            resourcePath,
            requestHash,
            () -> simulateTableUpdate(request));

    assertThat(response).isNotNull();
    assertThat(operationCounter.get()).isEqualTo(2); // Both attempts executed
  }

  /** Simulates a table update operation that would normally interact with metadata store. */
  private LoadTableResponse simulateTableUpdate(UpdateTableRequest request) {
    int operationId = operationCounter.incrementAndGet();

    // Simulate some processing time
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    // Return a mock response with operation-specific data
    return LoadTableResponse.builder().withTableMetadata(testTableMetadata).build();
  }
}
