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

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.responses.ErrorResponse;

/**
 * A wrapper around RESTClient that provides idempotency key support for mutation operations.
 *
 * <p>This client automatically adds idempotency keys to requests and provides convenient methods
 * for retry scenarios where the same key should be reused.
 */
public class IdempotencyAwareRESTClient implements RESTClient {

  private static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

  private final RESTClient delegate;
  private final IdempotencyKeyGenerator keyGenerator;

  /**
   * Creates a new idempotency-aware REST client.
   *
   * @param delegate the underlying REST client
   */
  public IdempotencyAwareRESTClient(RESTClient delegate) {
    this(delegate, new UUIDIdempotencyKeyGenerator());
  }

  /**
   * Creates a new idempotency-aware REST client with a custom key generator.
   *
   * @param delegate the underlying REST client
   * @param keyGenerator the key generator to use
   */
  public IdempotencyAwareRESTClient(RESTClient delegate, IdempotencyKeyGenerator keyGenerator) {
    this.delegate = Preconditions.checkNotNull(delegate, "delegate cannot be null");
    this.keyGenerator = Preconditions.checkNotNull(keyGenerator, "keyGenerator cannot be null");
  }

  // Delegate read-only operations directly
  @Override
  public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
    delegate.head(path, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delegate.get(path, queryParams, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delegate.delete(path, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delegate.delete(path, queryParams, responseType, headers, errorHandler);
  }

  // Enhanced POST methods with automatic idempotency key generation

  /**
   * Execute a POST request with automatic idempotency key generation. A new idempotency key is
   * generated for each call.
   */
  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return postWithIdempotencyKey(
        path, body, responseType, headers, errorHandler, keyGenerator.generate());
  }

  /**
   * Execute a POST request with a specific idempotency key. Use this method when you want to
   * control the idempotency key, such as for retries.
   *
   * @param path the request path
   * @param body the request body
   * @param responseType the expected response type
   * @param headers the request headers (idempotency key will be added)
   * @param errorHandler error handler
   * @param idempotencyKey the idempotency key to use, or null to skip idempotency
   * @return the response
   */
  public <T extends RESTResponse> T postWithIdempotencyKey(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      String idempotencyKey) {

    Map<String, String> headersWithIdempotency = addIdempotencyKey(headers, idempotencyKey);
    return delegate.post(path, body, responseType, headersWithIdempotency, errorHandler);
  }

  /**
   * Execute a POST request without idempotency. Use this method when you specifically don't want
   * idempotency for a request.
   */
  public <T extends RESTResponse> T postWithoutIdempotency(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return delegate.post(path, body, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    // Form posts typically don't need idempotency for OAuth flows
    return delegate.postForm(path, formData, responseType, headers, errorHandler);
  }

  /**
   * Create idempotent retry context for an operation. This is useful when you want to retry an
   * operation with the same idempotency key.
   *
   * @return a new retry context
   */
  public IdempotentOperationContext createIdempotentOperation() {
    return new IdempotentOperationContext(keyGenerator.generate());
  }

  /** Add idempotency key to headers if the key is not null. */
  private Map<String, String> addIdempotencyKey(
      Map<String, String> headers, String idempotencyKey) {
    if (idempotencyKey == null) {
      return headers;
    }

    Map<String, String> newHeaders = Maps.newHashMap();
    if (headers != null) {
      newHeaders.putAll(headers);
    }

    newHeaders.put(IDEMPOTENCY_KEY_HEADER, idempotencyKey);
    return newHeaders;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  /** Context for an idempotent operation that can be retried with the same key. */
  public class IdempotentOperationContext {
    private final String idempotencyKey;

    private IdempotentOperationContext(String idempotencyKey) {
      this.idempotencyKey = idempotencyKey;
    }

    /**
     * Execute a POST request using this context's idempotency key. Multiple calls with the same
     * context will use the same idempotency key.
     */
    public <T extends RESTResponse> T post(
        String path,
        RESTRequest body,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      return postWithIdempotencyKey(
          path, body, responseType, headers, errorHandler, idempotencyKey);
    }

    /** Get the idempotency key for this context. */
    public String getIdempotencyKey() {
      return idempotencyKey;
    }
  }

  /** Interface for generating idempotency keys. */
  public interface IdempotencyKeyGenerator {
    String generate();
  }

  /** Default implementation that generates UUID-based idempotency keys. */
  public static class UUIDIdempotencyKeyGenerator implements IdempotencyKeyGenerator {
    @Override
    public String generate() {
      return UUID.randomUUID().toString();
    }
  }

  /** Implementation that generates sequential idempotency keys for testing. */
  public static class SequentialIdempotencyKeyGenerator implements IdempotencyKeyGenerator {
    private long counter = 0;
    private final String prefix;

    public SequentialIdempotencyKeyGenerator() {
      this("test-key-");
    }

    public SequentialIdempotencyKeyGenerator(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public synchronized String generate() {
      return prefix + ++counter;
    }
  }
}
