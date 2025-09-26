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

import java.util.Map;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.rest.RESTCatalogServer.CatalogContext;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.util.PropertyUtil;

class RESTServerCatalogAdapter extends RESTCatalogAdapter {
  private static final String INCLUDE_CREDENTIALS = "include-credentials";

  private final CatalogContext catalogContext;
  private final IdempotencyHandler idempotencyHandler = new IdempotencyHandler();

  RESTServerCatalogAdapter(CatalogContext catalogContext) {
    super(catalogContext.catalog());
    this.catalogContext = catalogContext;
  }

  @Override
  public <T extends RESTResponse> T handleRequest(
      Route route, Map<String, String> vars, Object body, Class<T> responseType) {
    // Advertise idempotency capability via /v1/config so clients/tests can detect support
    if (route == Route.CONFIG) {
      // Allow overriding via server config; default to supported=true and TTL=PT30M
      Map<String, String> serverConf = catalogContext.configuration();
      String supported = serverConf.getOrDefault("capabilities.idempotency.supported", "true");
      String tokenTtl =
          serverConf.getOrDefault("capabilities.idempotency.tokenLifetime", "PT30M");

      org.apache.iceberg.rest.responses.ConfigResponse config =
          org.apache.iceberg.rest.responses.ConfigResponse.builder()
              .withEndpoints(
                  java.util.Arrays.stream(Route.values())
                      .map(r -> org.apache.iceberg.rest.Endpoint.create(r.method().name(), r.resourcePath()))
                      .collect(org.apache.iceberg.relocated.com.google.common.collect.ImmutableList.toImmutableList()))
              // Surface capabilities via overrides so they appear in client properties
              .withOverride("capabilities.idempotency.supported", supported)
              .withOverride("capabilities.idempotency.tokenLifetime", tokenTtl)
              .build();

      return RESTCatalogAdapter.castResponse(responseType, config);
    }

    // Idempotency handling for mutation routes (POST/DELETE) where a request class exists
    boolean isMutation = route.method().name().equals("POST") || route.method().name().equals("DELETE");

    if (isMutation) {
      String idempotencyKey = PropertyUtil.propertyAsString(vars, "Idempotency-Key", null);

      if (idempotencyKey != null && route.requestClass() != null) {
        // In baseline key-only mode, the request hash is not used for conflict decisions.
        String requestHash = "unused";
        String resourcePath = route.resourcePath();
        String operationType = route.name();

        String sleepMsStr = PropertyUtil.propertyAsString(vars, "X-Server-Sleep-Ms", null);
        long sleepMs = 0L;
        if (sleepMsStr != null) {
          try {
            sleepMs = Long.parseLong(sleepMsStr);
          } catch (NumberFormatException e) {
            // ignore bad value
          }
        }

        final long delay = sleepMs;
        // Optional test hook: force a finalize failure once for this request
        boolean failFinalizeOnce =
            Boolean.parseBoolean(PropertyUtil.propertyAsString(vars, "X-Server-Fail-Finalize-Once", null));
        boolean reconcileOnInProgressOnce =
            Boolean.parseBoolean(
                PropertyUtil.propertyAsString(vars, "X-Server-Reconcile-On-In-Progress-Once", null));
        // Set test flags BEFORE entering idempotency handler so IN_PROGRESS branch can see them
        if (failFinalizeOnce) {
          IdempotencyHandler.testFailFinalizeOnce();
        }
        if (reconcileOnInProgressOnce) {
          IdempotencyHandler.testReconcileOnInProgressOnce();
        }

        java.util.function.Supplier<T> operationSupplier =
            () -> {
              if (delay > 0) {
                try {
                  Thread.sleep(delay);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }

              // Special reconciliation behavior for create-table: if the table now exists, load it
              if (reconcileOnInProgressOnce && route == Route.CREATE_TABLE) {
                try {
                  return super.handleRequest(route, vars, body, responseType);
                } catch (AlreadyExistsException e) {
                  // Build the identifier from namespace + request name
                  CreateTableRequest ctr = (CreateTableRequest) body;
                  Namespace ns = RESTUtil.decodeNamespace(vars.get("namespace"));
                  TableIdentifier ident = TableIdentifier.of(ns, ctr.name());
                  return RESTCatalogAdapter.castResponse(
                      responseType, CatalogHandlers.loadTable(catalogContext.catalog(), ident));
                }
              }

              return super.handleRequest(route, vars, body, responseType);
            };

        return idempotencyHandler.handleOperation(
            idempotencyKey,
            operationType,
            resourcePath,
            requestHash,
            operationSupplier);
      }
    }

    T restResponse = super.handleRequest(route, vars, body, responseType);

    if (restResponse instanceof LoadTableResponse) {
      if (PropertyUtil.propertyAsBoolean(
          catalogContext.configuration(), INCLUDE_CREDENTIALS, false)) {
        applyCredentials(
            catalogContext.configuration(), ((LoadTableResponse) restResponse).config());
      }
    }

    return restResponse;
  }

  private void applyCredentials(
      Map<String, String> catalogConfig, Map<String, String> tableConfig) {
    if (catalogConfig.containsKey(S3FileIOProperties.ACCESS_KEY_ID)) {
      tableConfig.put(
          S3FileIOProperties.ACCESS_KEY_ID, catalogConfig.get(S3FileIOProperties.ACCESS_KEY_ID));
    }

    if (catalogConfig.containsKey(S3FileIOProperties.SECRET_ACCESS_KEY)) {
      tableConfig.put(
          S3FileIOProperties.SECRET_ACCESS_KEY,
          catalogConfig.get(S3FileIOProperties.SECRET_ACCESS_KEY));
    }

    if (catalogConfig.containsKey(S3FileIOProperties.SESSION_TOKEN)) {
      tableConfig.put(
          S3FileIOProperties.SESSION_TOKEN, catalogConfig.get(S3FileIOProperties.SESSION_TOKEN));
    }

    if (catalogConfig.containsKey(GCPProperties.GCS_OAUTH2_TOKEN)) {
      tableConfig.put(
          GCPProperties.GCS_OAUTH2_TOKEN, catalogConfig.get(GCPProperties.GCS_OAUTH2_TOKEN));
    }

    catalogConfig.entrySet().stream()
        .filter(
            entry ->
                entry.getKey().startsWith(AzureProperties.ADLS_SAS_TOKEN_PREFIX)
                    || entry.getKey().startsWith(AzureProperties.ADLS_CONNECTION_STRING_PREFIX))
        .forEach(entry -> tableConfig.put(entry.getKey(), entry.getValue()));
  }
}
