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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.Test;

/** Tests the client-side deconfliction fallback in RESTTableOperations. */
public class TestRESTTableOperationsDeconfliction {

  private static RESTTableOperations opsWith(
      RESTClient client,
      TableMetadata current,
      Supplier<Map<String, String>> headers,
      boolean deconflictSuccess) {
    Set<Endpoint> endpoints = new java.util.HashSet<>();
    endpoints.add(Endpoint.V1_UPDATE_TABLE);
    endpoints.add(Endpoint.V1_LOAD_TABLE);
    return new RESTTableOperations(
        client, "/v1/prefix/ns/table", headers, new NoopFileIO(), current, endpoints) {
      @Override
      public EncryptionManager encryption() {
        return null;
      }

      @Override
      public LocationProvider locationProvider() {
        return null;
      }

      @Override
      protected boolean wasIntendedUpdateApplied(TableMetadata intendedMetadata) {
        return deconflictSuccess;
      }
    };
  }

  @Test
  public void testDeconflictTreatsUnknownAsSuccessWhenSnapshotMatches() {
    // Build a minimal valid TableMetadata
    org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
        org.apache.iceberg.types.Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.LongType.get()));
    org.apache.iceberg.PartitionSpec spec = org.apache.iceberg.PartitionSpec.unpartitioned();
    TableMetadata base = TableMetadata.newTableMetadata(schema, spec, "/tmp", Collections.emptyMap());
    TableMetadata intended = TableMetadata.buildFrom(base).build();

    // Mock client: throw CommitStateUnknownException during post
    RESTClient client = mock(RESTClient.class);
    when(client.post(any(), any(), any(), any(Supplier.class), any()))
        .thenThrow(new CommitStateUnknownException(new RuntimeException("unknown")));

    // headers supplier
    Supplier<Map<String, String>> headers = Collections::emptyMap;

    RESTTableOperations ops = opsWith(client, base, headers, true);

    // Execute: commit should catch unknown state, verify via refresh, and not throw
    ops.commit(base, intended);
  }

  @Test
  public void testDeconflictRethrowsWhenSnapshotDoesNotMatch() {
    org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
        org.apache.iceberg.types.Types.NestedField.required(1, "id", org.apache.iceberg.types.Types.LongType.get()));
    org.apache.iceberg.PartitionSpec spec = org.apache.iceberg.PartitionSpec.unpartitioned();
    TableMetadata base = TableMetadata.newTableMetadata(schema, spec, "/tmp", Collections.emptyMap());
    TableMetadata intended = TableMetadata.buildFrom(base).build();

    RESTClient client = mock(RESTClient.class);
    when(client.post(any(), any(), any(), any(Supplier.class), any()))
        .thenThrow(new CommitStateUnknownException(new RuntimeException("unknown")));

    Supplier<Map<String, String>> headers = Collections::emptyMap;
    RESTTableOperations ops = opsWith(client, base, headers, false);

    assertThatThrownBy(() -> ops.commit(base, intended))
        .isInstanceOf(CommitStateUnknownException.class);
  }

  // Minimal FileIO implementation for test
  private static class NoopFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      return null;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return null;
    }

    @Override
    public void deleteFile(String path) {}

    @Override
    public void close() {}
  }
}

