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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.exceptions.IdempotencyKeyConflictException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIdempotencyHandler {

  private IdempotencyHandler handler;

  @BeforeEach
  void before() {
    handler = new IdempotencyHandler();
  }

  @Test
  void testBasicIdempotency() {
    String key = UUID.randomUUID().toString();
    String operationType = "updateTable";
    String resourcePath = "/v1/warehouse/namespaces/sales/tables/orders";
    String requestHash = "hash123";

    AtomicInteger callCount = new AtomicInteger(0);

    // First call
    String result1 =
        handler.handleOperation(
            key,
            operationType,
            resourcePath,
            requestHash,
            () -> {
              callCount.incrementAndGet();
              return "result1";
            });

    // Second call with same key should return cached result
    String result2 =
        handler.handleOperation(
            key,
            operationType,
            resourcePath,
            requestHash,
            () -> {
              callCount.incrementAndGet();
              return "result2";
            });

    assertThat(result1).isEqualTo("result1");
    assertThat(result2).isEqualTo("result1"); // Same as first result
    assertThat(callCount.get()).isEqualTo(1); // Operation only called once
  }

  @Test
  void testNoIdempotencyKey() {
    AtomicInteger callCount = new AtomicInteger(0);

    // First call without key
    String result1 =
        handler.handleOperation(
            null,
            "updateTable",
            "/path",
            "hash123",
            () -> {
              callCount.incrementAndGet();
              return "result1";
            });

    // Second call without key
    String result2 =
        handler.handleOperation(
            null,
            "updateTable",
            "/path",
            "hash123",
            () -> {
              callCount.incrementAndGet();
              return "result2";
            });

    assertThat(result1).isEqualTo("result1");
    assertThat(result2).isEqualTo("result2");
    assertThat(callCount.get()).isEqualTo(2); // Both operations called
  }

  @Test
  void testDuplicateIgnoresPayloadDifferencesInKeyOnlyMode() {
    String key = UUID.randomUUID().toString();
    String operationType = "updateTable";
    String resourcePath = "/v1/warehouse/namespaces/sales/tables/orders";

    String first =
        handler.handleOperation(key, operationType, resourcePath, "hash1", () -> "result1");

    String second =
        handler.handleOperation(key, operationType, resourcePath, "hash2", () -> "result2");

    assertThat(first).isEqualTo("result1");
    assertThat(second).isEqualTo("result1");
  }

  @Test
  void testScopedKeys() {
    String key = UUID.randomUUID().toString();
    String requestHash = "hash123";

    // Same key can be used for different operations or resources
    String result1 =
        handler.handleOperation(key, "updateTable", "/path1", requestHash, () -> "result1");

    String result2 =
        handler.handleOperation(key, "updateTable", "/path2", requestHash, () -> "result2");

    String result3 =
        handler.handleOperation(key, "createTable", "/path1", requestHash, () -> "result3");

    assertThat(result1).isEqualTo("result1");
    assertThat(result2).isEqualTo("result2");
    assertThat(result3).isEqualTo("result3");
  }

  @Test
  void testFailedOperationNotCached() {
    String key = UUID.randomUUID().toString();
    String operationType = "updateTable";
    String resourcePath = "/path";
    String requestHash = "hash123";

    AtomicInteger callCount = new AtomicInteger(0);

    // First call that fails
    assertThatThrownBy(
            () ->
                handler.handleOperation(
                    key,
                    operationType,
                    resourcePath,
                    requestHash,
                    () -> {
                      callCount.incrementAndGet();
                      throw new RuntimeException("Operation failed");
                    }))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Operation failed");

    // Second call should retry, not return cached failure
    String result =
        handler.handleOperation(
            key,
            operationType,
            resourcePath,
            requestHash,
            () -> {
              callCount.incrementAndGet();
              return "success";
            });

    assertThat(result).isEqualTo("success");
    assertThat(callCount.get()).isEqualTo(2); // Both operations called
  }

  @Test
  void testKeyValidation() {
    assertThatThrownBy(
            () -> handler.handleOperation("", "updateTable", "/path", "hash123", () -> "result"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Idempotency key length must be between");

    // Too long key
    String longKey = "a".repeat(256);
    assertThatThrownBy(
            () ->
                handler.handleOperation(longKey, "updateTable", "/path", "hash123", () -> "result"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Idempotency key length must be between");

    // Invalid characters
    assertThatThrownBy(
            () ->
                handler.handleOperation(
                    "invalid key with spaces", "updateTable", "/path", "hash123", () -> "result"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Idempotency key must match pattern");
  }

  @Test
  void testValidKeys() {
    // These should all be valid
    String[] validKeys = {
      "abc123", "a-b-c", "a_b_c", "a.b.c", "abc-123_def.456", UUID.randomUUID().toString()
    };

    for (String key : validKeys) {
      String result =
          handler.handleOperation(key, "updateTable", "/path/" + key, "hash123", () -> "result");
      assertThat(result).isEqualTo("result");
    }
  }

  @Test
  void testConcurrentAccess() throws InterruptedException {
    String key = UUID.randomUUID().toString();
    String operationType = "updateTable";
    String resourcePath = "/path";
    String requestHash = "hash123";

    int threadCount = 10;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completeLatch = new CountDownLatch(threadCount);

    AtomicInteger callCount = new AtomicInteger(0);
    AtomicReference<String> firstResult = new AtomicReference<>();
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    // Submit multiple concurrent requests with same idempotency key
    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executor.submit(
          () -> {
            try {
              startLatch.await();

              String result =
                  handler.handleOperation(
                      key,
                      operationType,
                      resourcePath,
                      requestHash,
                      () -> {
                        callCount.incrementAndGet();
                        // Add small delay to make race conditions more likely
                        try {
                          Thread.sleep(10);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new RuntimeException(e);
                        }
                        return "result-" + threadId;
                      });

              // Store the first result we see
              firstResult.compareAndSet(null, result);

              // All threads should get the same result
              assertThat(result).isEqualTo(firstResult.get());

            } catch (Exception e) {
              throw new RuntimeException(e);
            } finally {
              completeLatch.countDown();
            }
          });
    }

    // Start all threads at once
    startLatch.countDown();

    // Wait for all to complete
    assertThat(completeLatch.await(5, TimeUnit.SECONDS)).isTrue();

    // Only one operation should have been executed
    assertThat(callCount.get()).isEqualTo(1);

    executor.shutdown();
  }

  @Test
  void testKeyExpiration() throws InterruptedException {
    // Use short expiration for testing
    IdempotencyHandler shortExpirationHandler = new IdempotencyHandler(100); // 100ms

    String key = UUID.randomUUID().toString();
    String operationType = "updateTable";
    String resourcePath = "/path";
    String requestHash = "hash123";

    AtomicInteger callCount = new AtomicInteger(0);

    // First call
    String result1 =
        shortExpirationHandler.handleOperation(
            key,
            operationType,
            resourcePath,
            requestHash,
            () -> {
              callCount.incrementAndGet();
              return "result1";
            });

    // Wait for expiration
    Thread.sleep(200);

    // Second call should execute again due to expiration
    String result2 =
        shortExpirationHandler.handleOperation(
            key,
            operationType,
            resourcePath,
            requestHash,
            () -> {
              callCount.incrementAndGet();
              return "result2";
            });

    assertThat(result1).isEqualTo("result1");
    assertThat(result2).isEqualTo("result2");
    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  void testStoredKeyCount() {
    assertThat(handler.getStoredKeyCount()).isEqualTo(0);

    handler.handleOperation("key1", "updateTable", "/path1", "hash1", () -> "result1");
    assertThat(handler.getStoredKeyCount()).isEqualTo(1);

    handler.handleOperation("key2", "updateTable", "/path2", "hash2", () -> "result2");
    assertThat(handler.getStoredKeyCount()).isEqualTo(2);

    // Same key, different scope
    handler.handleOperation("key1", "createTable", "/path1", "hash3", () -> "result3");
    assertThat(handler.getStoredKeyCount()).isEqualTo(3);
  }

  @Test
  void testClear() {
    handler.handleOperation("key1", "updateTable", "/path1", "hash1", () -> "result1");
    handler.handleOperation("key2", "updateTable", "/path2", "hash2", () -> "result2");

    assertThat(handler.getStoredKeyCount()).isEqualTo(2);

    handler.clear();

    assertThat(handler.getStoredKeyCount()).isEqualTo(0);
  }
}
