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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class IdempotencyPOCTests {

  @RegisterExtension
  static RESTServerExtension SERVER =
      new RESTServerExtension(
          java.util.Map.of(RESTCatalogServer.REST_PORT, RESTServerExtension.FREE_PORT));

  private static RESTCatalog catalog;
  private static RESTClient client;

  @BeforeAll
  static void beforeAll() {
    catalog = SERVER.client();
    client =
        HTTPClient.builder(ImmutableMap.of())
            .uri(catalog.properties().get(org.apache.iceberg.CatalogProperties.URI))
            .withAuthSession(org.apache.iceberg.rest.auth.AuthSession.EMPTY)
            .build();
  }

  @AfterAll
  static void afterAll() throws Exception {
    catalog.close();
  }

  @Test
  public void duplicateSamePayloadReturnsOriginal() {
    String key = UUID.randomUUID().toString();
    TableIdentifier ident = TableIdentifier.of("ns", "tbl");

    catalog.createNamespace(ident.namespace());
    Schema schema = new Schema(org.apache.iceberg.types.Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.IntegerType.get()));

    // Use createTable as a valid mutation payload for idempotency tests
    org.apache.iceberg.rest.requests.CreateTableRequest req =
        org.apache.iceberg.rest.requests.CreateTableRequest.builder()
            .withName(ident.name())
            .withSchema(schema)
            .build();

    String path =
        String.format(
            "v1/namespaces/%s/tables",
            RESTUtil.encodeNamespace(ident.namespace()));

    LoadTableResponse first =
        new IdempotencyAwareRESTClient(client)
            .postWithIdempotencyKey(
                path, req, LoadTableResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler(), key);

    LoadTableResponse dup =
        new IdempotencyAwareRESTClient(client)
            .postWithIdempotencyKey(
                path, req, LoadTableResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler(), key);

    assertThat(dup.metadataLocation()).isEqualTo(first.metadataLocation());
  }

  @Test
  public void duplicateDifferentPayloadRaisesConflict() {
    String key = UUID.randomUUID().toString();
    TableIdentifier ident = TableIdentifier.of("ns2", "tbl");
    catalog.createNamespace(ident.namespace());
    Schema schema = new Schema(org.apache.iceberg.types.Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.IntegerType.get()));

    String path =
        String.format(
            "v1/namespaces/%s/tables",
            RESTUtil.encodeNamespace(ident.namespace()));

    org.apache.iceberg.rest.requests.CreateTableRequest req1 =
        org.apache.iceberg.rest.requests.CreateTableRequest.builder()
            .withName(ident.name())
            .withSchema(schema)
            .build();

    org.apache.iceberg.rest.requests.CreateTableRequest req2 =
        org.apache.iceberg.rest.requests.CreateTableRequest.builder()
            .withName(ident.name())
            .withSchema(new Schema(org.apache.iceberg.types.Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.LongType.get())))
            .build();

    new IdempotencyAwareRESTClient(client)
        .postWithIdempotencyKey(
            path, req1, LoadTableResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler(), key);

    assertThatThrownBy(
            () ->
                new IdempotencyAwareRESTClient(client)
                    .postWithIdempotencyKey(
                        path, req2, LoadTableResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler(), key))
        .isInstanceOf(org.apache.iceberg.exceptions.RESTException.class);
  }

  @Test
  public void inFlightDuplicateReturnsConflict() throws Exception {
    String key = UUID.randomUUID().toString();
    TableIdentifier ident = TableIdentifier.of("ns3", "tbl");
    catalog.createNamespace(ident.namespace());
    Schema schema = new Schema(org.apache.iceberg.types.Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.IntegerType.get()));

    org.apache.iceberg.rest.requests.CreateTableRequest req =
        org.apache.iceberg.rest.requests.CreateTableRequest.builder()
            .withName(ident.name())
            .withSchema(schema)
            .build();

    String path =
        String.format(
            "v1/namespaces/%s/tables",
            RESTUtil.encodeNamespace(ident.namespace()));

    // Launch first request asynchronously with a server-side delay so it remains IN_PROGRESS
    CompletableFuture<Void> first =
        CompletableFuture.runAsync(
            () ->
                new IdempotencyAwareRESTClient(client)
                    .postWithIdempotencyKey(
                        path,
                        req,
                        LoadTableResponse.class,
                        Map.of("X-Server-Sleep-Ms", "500"),
                        ErrorHandlers.defaultErrorHandler(),
                        key));

    // Give the server a brief moment to reserve the key
    Thread.sleep(50);

    assertThatThrownBy(
            () ->
                new IdempotencyAwareRESTClient(client)
                    .postWithIdempotencyKey(
                        path,
                        req,
                        LoadTableResponse.class,
                        Map.of(),
                        ErrorHandlers.defaultErrorHandler(),
                        key))
        .isInstanceOf(org.apache.iceberg.exceptions.RESTException.class);

    first.get(10, TimeUnit.SECONDS);
  }

  @Test
  public void reconcileAfterFinalizeFailure() {
    String key = UUID.randomUUID().toString();
    TableIdentifier ident = TableIdentifier.of("ns4", "tbl");
    catalog.createNamespace(ident.namespace());
    Schema schema = new Schema(org.apache.iceberg.types.Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.IntegerType.get()));

    org.apache.iceberg.rest.requests.CreateTableRequest req =
        org.apache.iceberg.rest.requests.CreateTableRequest.builder()
            .withName(ident.name())
            .withSchema(schema)
            .build();

    String path =
        String.format(
            "v1/namespaces/%s/tables",
            RESTUtil.encodeNamespace(ident.namespace()));

    // First attempt: server executes but fails finalization -> client sees server error
    assertThatThrownBy(
            () ->
                new IdempotencyAwareRESTClient(client)
                    .postWithIdempotencyKey(
                        path,
                        req,
                        LoadTableResponse.class,
                        Map.of("X-Server-Fail-Finalize-Once", "true"),
                        ErrorHandlers.defaultErrorHandler(),
                        key))
        .isInstanceOf(org.apache.iceberg.exceptions.RESTException.class);

    // Second attempt with same key+payload, ask server to reconcile the IN_PROGRESS record and finalize
    // Give a tiny delay to ensure the test flag is registered on server before retry
    // (flag is read at the start of idempotency handling)
    try {
      Thread.sleep(25);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    LoadTableResponse response1 =
        new IdempotencyAwareRESTClient(client)
            .postWithIdempotencyKey(
                path,
                req,
                LoadTableResponse.class,
                Map.of("X-Server-Reconcile-On-In-Progress-Once", "true"),
                ErrorHandlers.defaultErrorHandler(),
                key);

    // Third attempt without headers should return cached result
    LoadTableResponse response2 =
        new IdempotencyAwareRESTClient(client)
            .postWithIdempotencyKey(
                path, req, LoadTableResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler(), key);

    assertThat(response1.metadataLocation()).isEqualTo(response2.metadataLocation());
  }

  @Test
  public void canonicalEquivalentPayloadsAreTreatedSame() {
    String key = UUID.randomUUID().toString();
    TableIdentifier ident = TableIdentifier.of("ns5", "tbl");
    catalog.createNamespace(ident.namespace());

    Schema schema =
        new Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "id", org.apache.iceberg.types.Types.IntegerType.get()));

    // Two requests that differ only by insertion order of properties; canonical hashing should match
    CreateTableRequest req1 =
        CreateTableRequest.builder()
            .withName(ident.name())
            .withSchema(schema)
            .setProperty("b", "2")
            .setProperty("a", "1")
            .build();

    CreateTableRequest req2 =
        CreateTableRequest.builder()
            .withName(ident.name())
            .withSchema(schema)
            .setProperty("a", "1")
            .setProperty("b", "2")
            .build();

    String path =
        String.format("v1/namespaces/%s/tables", RESTUtil.encodeNamespace(ident.namespace()));

    LoadTableResponse first =
        new IdempotencyAwareRESTClient(client)
            .postWithIdempotencyKey(
                path, req1, LoadTableResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler(), key);

    LoadTableResponse dup =
        new IdempotencyAwareRESTClient(client)
            .postWithIdempotencyKey(
                path, req2, LoadTableResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler(), key);

    assertThat(dup.metadataLocation()).isEqualTo(first.metadataLocation());
  }

  @Test
  public void modifiedPayloadAfterFinalizeFailureRaisesConflict() {
    String key = UUID.randomUUID().toString();
    TableIdentifier ident = TableIdentifier.of("ns6", "tbl");
    catalog.createNamespace(ident.namespace());

    Schema schemaInt =
        new Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "id", org.apache.iceberg.types.Types.IntegerType.get()));
    Schema schemaLong =
        new Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "id", org.apache.iceberg.types.Types.LongType.get()));

    CreateTableRequest req1 =
        CreateTableRequest.builder().withName(ident.name()).withSchema(schemaInt).build();
    CreateTableRequest req2 =
        CreateTableRequest.builder().withName(ident.name()).withSchema(schemaLong).build();

    String path =
        String.format("v1/namespaces/%s/tables", RESTUtil.encodeNamespace(ident.namespace()));

    // First attempt executes and fails to finalize, leaving IN_PROGRESS record
    assertThatThrownBy(
            () ->
                new IdempotencyAwareRESTClient(client)
                    .postWithIdempotencyKey(
                        path,
                        req1,
                        LoadTableResponse.class,
                        Map.of("X-Server-Fail-Finalize-Once", "true"),
                        ErrorHandlers.defaultErrorHandler(),
                        key))
        .isInstanceOf(org.apache.iceberg.exceptions.RESTException.class);

    // Second attempt changes payload under same key -> conflict (hash mismatch)
    assertThatThrownBy(
            () ->
                new IdempotencyAwareRESTClient(client)
                    .postWithIdempotencyKey(
                        path,
                        req2,
                        LoadTableResponse.class,
                        Map.of(),
                        ErrorHandlers.defaultErrorHandler(),
                        key))
        .isInstanceOf(org.apache.iceberg.exceptions.RESTException.class);
  }
}


