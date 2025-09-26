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
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RESTServerExtension.class)
public class RESTCompatibilityKitIdempotencyTests {

  private static RESTCatalog catalog;
  private static RESTClient http;

  @BeforeAll
  static void beforeAll() {
    catalog = RCKUtils.initCatalogClient();
    http =
        HTTPClient.builder(ImmutableMap.of())
            .uri(catalog.properties().get(org.apache.iceberg.CatalogProperties.URI))
            .withAuthSession(org.apache.iceberg.rest.auth.AuthSession.EMPTY)
            .build();

    // Only run if server advertises idempotency capability
    boolean supported =
        Boolean.parseBoolean(
            catalog
                .properties()
                .getOrDefault("capabilities.idempotency.supported", "false"));
    Assumptions.assumeTrue(supported, "Server does not advertise idempotency support");
  }

  @AfterAll
  static void afterAll() throws Exception {
    if (catalog != null) {
      catalog.close();
    }
  }

  @Test
  public void duplicateSamePayloadReturnsOriginal() {
    String key = UUID.randomUUID().toString();
    TableIdentifier ident = TableIdentifier.of("rck_ns_idem", "tbl1");
    catalog.createNamespace(ident.namespace());

    Schema schema =
        new Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "id", org.apache.iceberg.types.Types.IntegerType.get()));

    CreateTableRequest req =
        CreateTableRequest.builder().withName(ident.name()).withSchema(schema).build();

    String path = String.format("v1/namespaces/%s/tables", RESTUtil.encodeNamespace(ident.namespace()));

    IdempotencyAwareRESTClient client = new IdempotencyAwareRESTClient(http);

    LoadTableResponse first =
        client.postWithIdempotencyKey(
            path, req, LoadTableResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler(), key);

    LoadTableResponse dup =
        client.postWithIdempotencyKey(
            path, req, LoadTableResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler(), key);

    assertThat(dup.metadataLocation()).isEqualTo(first.metadataLocation());
  }

  @Test
  public void duplicateDifferentPayloadReturnsOriginalInKeyOnlyBaseline() {
    String key = UUID.randomUUID().toString();
    TableIdentifier ident = TableIdentifier.of("rck_ns_idem2", "tbl2");
    catalog.createNamespace(ident.namespace());

    Schema schemaA =
        new Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "id", org.apache.iceberg.types.Types.IntegerType.get()));
    Schema schemaB =
        new Schema(
            org.apache.iceberg.types.Types.NestedField.required(
                1, "id", org.apache.iceberg.types.Types.LongType.get()));

    CreateTableRequest reqA =
        CreateTableRequest.builder().withName(ident.name()).withSchema(schemaA).build();
    CreateTableRequest reqB =
        CreateTableRequest.builder().withName(ident.name()).withSchema(schemaB).build();

    String path = String.format("v1/namespaces/%s/tables", RESTUtil.encodeNamespace(ident.namespace()));

    IdempotencyAwareRESTClient client = new IdempotencyAwareRESTClient(http);

    LoadTableResponse first =
        client.postWithIdempotencyKey(
            path, reqA, LoadTableResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler(), key);

    LoadTableResponse dup =
        client.postWithIdempotencyKey(
            path, reqB, LoadTableResponse.class, Map.of(), ErrorHandlers.defaultErrorHandler(), key);

    assertThat(dup.metadataLocation()).isEqualTo(first.metadataLocation());
  }
}


