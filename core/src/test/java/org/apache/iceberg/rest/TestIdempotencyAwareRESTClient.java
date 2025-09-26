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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestIdempotencyAwareRESTClient {

  private RESTClient mockDelegate;
  private IdempotencyAwareRESTClient client;
  private IdempotencyAwareRESTClient.IdempotencyKeyGenerator mockKeyGenerator;

  @BeforeEach
  void before() {
    mockDelegate = mock(RESTClient.class);
    mockKeyGenerator = mock(IdempotencyAwareRESTClient.IdempotencyKeyGenerator.class);
    client = new IdempotencyAwareRESTClient(mockDelegate, mockKeyGenerator);
  }

  @Test
  void testPostWithAutomaticIdempotencyKey() {
    // Setup
    String expectedKey = "test-key-123";
    when(mockKeyGenerator.generate()).thenReturn(expectedKey);

    RESTRequest request = mock(RESTRequest.class);
    RESTResponse response = mock(RESTResponse.class);
    when(mockDelegate.post(
            any(), eq(request), eq(RESTResponse.class), any(Map.class), any(Consumer.class)))
        .thenReturn(response);

    // Execute
    RESTResponse result =
        client.post("/v1/tables", request, RESTResponse.class, Map.of(), mock(Consumer.class));

    // Verify
    assertThat(result).isEqualTo(response);

    ArgumentCaptor<Map<String, String>> headersCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockDelegate)
        .post(
            eq("/v1/tables"), eq(request), eq(RESTResponse.class), headersCaptor.capture(), any());

    Map<String, String> capturedHeaders = headersCaptor.getValue();
    assertThat(capturedHeaders).containsEntry("Idempotency-Key", expectedKey);
  }

  @Test
  void testPostWithSpecificIdempotencyKey() {
    // Setup
    String specificKey = "my-specific-key";
    RESTRequest request = mock(RESTRequest.class);
    RESTResponse response = mock(RESTResponse.class);
    when(mockDelegate.post(
            any(), eq(request), eq(RESTResponse.class), any(Map.class), any(Consumer.class)))
        .thenReturn(response);

    // Execute
    RESTResponse result =
        client.postWithIdempotencyKey(
            "/v1/tables", request, RESTResponse.class, Map.of(), mock(Consumer.class), specificKey);

    // Verify
    assertThat(result).isEqualTo(response);

    ArgumentCaptor<Map<String, String>> headersCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockDelegate)
        .post(
            eq("/v1/tables"), eq(request), eq(RESTResponse.class), headersCaptor.capture(), any());

    Map<String, String> capturedHeaders = headersCaptor.getValue();
    assertThat(capturedHeaders).containsEntry("Idempotency-Key", specificKey);
  }

  @Test
  void testPostWithoutIdempotency() {
    // Setup
    RESTRequest request = mock(RESTRequest.class);
    RESTResponse response = mock(RESTResponse.class);
    Map<String, String> originalHeaders = Map.of("Authorization", "Bearer token");
    when(mockDelegate.post(
            eq("/v1/tables"),
            eq(request),
            eq(RESTResponse.class),
            eq(originalHeaders),
            any(Consumer.class)))
        .thenReturn(response);

    // Execute
    RESTResponse result =
        client.postWithoutIdempotency(
            "/v1/tables", request, RESTResponse.class, originalHeaders, mock(Consumer.class));

    // Verify
    assertThat(result).isEqualTo(response);
    verify(mockDelegate)
        .post(eq("/v1/tables"), eq(request), eq(RESTResponse.class), eq(originalHeaders), any());
  }

  @Test
  void testPostWithNullIdempotencyKey() {
    // Setup
    RESTRequest request = mock(RESTRequest.class);
    RESTResponse response = mock(RESTResponse.class);
    Map<String, String> originalHeaders = Map.of("Authorization", "Bearer token");
    when(mockDelegate.post(
            eq("/v1/tables"),
            eq(request),
            eq(RESTResponse.class),
            eq(originalHeaders),
            any(Consumer.class)))
        .thenReturn(response);

    // Execute
    RESTResponse result =
        client.postWithIdempotencyKey(
            "/v1/tables", request, RESTResponse.class, originalHeaders, mock(Consumer.class), null);

    // Verify
    assertThat(result).isEqualTo(response);
    verify(mockDelegate)
        .post(eq("/v1/tables"), eq(request), eq(RESTResponse.class), eq(originalHeaders), any());
  }

  @Test
  void testIdempotentOperationContext() {
    // Setup
    String contextKey = "context-key-456";
    when(mockKeyGenerator.generate()).thenReturn(contextKey);

    RESTRequest request = mock(RESTRequest.class);
    RESTResponse response = mock(RESTResponse.class);
    when(mockDelegate.post(
            any(), eq(request), eq(RESTResponse.class), any(Map.class), any(Consumer.class)))
        .thenReturn(response);

    // Execute
    IdempotencyAwareRESTClient.IdempotentOperationContext context =
        client.createIdempotentOperation();

    assertThat(context.getIdempotencyKey()).isEqualTo(contextKey);

    // Use context multiple times
    RESTResponse result1 =
        context.post("/v1/tables", request, RESTResponse.class, Map.of(), mock(Consumer.class));
    RESTResponse result2 =
        context.post("/v1/tables", request, RESTResponse.class, Map.of(), mock(Consumer.class));

    // Verify
    assertThat(result1).isEqualTo(response);
    assertThat(result2).isEqualTo(response);

    // Verify that both calls used the same idempotency key
    ArgumentCaptor<Map<String, String>> headersCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockDelegate, times(2))
        .post(
            eq("/v1/tables"), eq(request), eq(RESTResponse.class), headersCaptor.capture(), any());

    headersCaptor
        .getAllValues()
        .forEach(headers -> assertThat(headers).containsEntry("Idempotency-Key", contextKey));
  }

  @Test
  void testHeaderMerging() {
    // Setup
    String idempotencyKey = "test-key";
    RESTRequest request = mock(RESTRequest.class);
    RESTResponse response = mock(RESTResponse.class);
    Map<String, String> originalHeaders =
        Map.of(
            "Authorization", "Bearer token",
            "Content-Type", "application/json");
    when(mockDelegate.post(
            any(), eq(request), eq(RESTResponse.class), any(Map.class), any(Consumer.class)))
        .thenReturn(response);

    // Execute
    client.postWithIdempotencyKey(
        "/v1/tables",
        request,
        RESTResponse.class,
        originalHeaders,
        mock(Consumer.class),
        idempotencyKey);

    // Verify
    ArgumentCaptor<Map<String, String>> headersCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mockDelegate)
        .post(
            eq("/v1/tables"), eq(request), eq(RESTResponse.class), headersCaptor.capture(), any());

    Map<String, String> capturedHeaders = headersCaptor.getValue();
    assertThat(capturedHeaders).containsAllEntriesOf(originalHeaders);
    assertThat(capturedHeaders).containsEntry("Idempotency-Key", idempotencyKey);
    assertThat(capturedHeaders).hasSize(3); // Original 2 + idempotency key
  }

  @Test
  void testDelegateReadOnlyMethods() {
    // Test that read-only methods are delegated without modification

    // HEAD
    client.head("/v1/config", Map.of(), mock(Consumer.class));
    verify(mockDelegate).head(eq("/v1/config"), eq(Map.of()), any());

    // GET
    when(mockDelegate.get(any(), any(), any(), any(Map.class), any(Consumer.class)))
        .thenReturn(mock(RESTResponse.class));
    client.get("/v1/namespaces", Map.of(), RESTResponse.class, Map.of(), mock(Consumer.class));
    verify(mockDelegate)
        .get(eq("/v1/namespaces"), eq(Map.of()), eq(RESTResponse.class), eq(Map.of()), any());

    // DELETE
    when(mockDelegate.delete(any(), any(), any(Map.class), any(Consumer.class)))
        .thenReturn(mock(RESTResponse.class));
    client.delete("/v1/tables/test", RESTResponse.class, Map.of(), mock(Consumer.class));
    verify(mockDelegate).delete(eq("/v1/tables/test"), eq(RESTResponse.class), eq(Map.of()), any());
  }

  @Test
  void testUUIDKeyGenerator() {
    IdempotencyAwareRESTClient.UUIDIdempotencyKeyGenerator generator =
        new IdempotencyAwareRESTClient.UUIDIdempotencyKeyGenerator();

    String key1 = generator.generate();
    String key2 = generator.generate();

    assertThat(key1).isNotNull();
    assertThat(key2).isNotNull();
    assertThat(key1).isNotEqualTo(key2);

    // Basic UUID format check (36 characters with dashes)
    assertThat(key1).hasSize(36);
    assertThat(key2).hasSize(36);
    assertThat(key1).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
  }

  @Test
  void testSequentialKeyGenerator() {
    IdempotencyAwareRESTClient.SequentialIdempotencyKeyGenerator generator =
        new IdempotencyAwareRESTClient.SequentialIdempotencyKeyGenerator("test-");

    String key1 = generator.generate();
    String key2 = generator.generate();
    String key3 = generator.generate();

    assertThat(key1).isEqualTo("test-1");
    assertThat(key2).isEqualTo("test-2");
    assertThat(key3).isEqualTo("test-3");
  }

  @Test
  void testSequentialKeyGeneratorDefaultPrefix() {
    IdempotencyAwareRESTClient.SequentialIdempotencyKeyGenerator generator =
        new IdempotencyAwareRESTClient.SequentialIdempotencyKeyGenerator();

    String key1 = generator.generate();
    String key2 = generator.generate();

    assertThat(key1).isEqualTo("test-key-1");
    assertThat(key2).isEqualTo("test-key-2");
  }
}
