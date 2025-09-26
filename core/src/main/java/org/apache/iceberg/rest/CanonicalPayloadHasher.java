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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.iceberg.exceptions.RESTException;
import java.util.Locale;

/** Utility to compute a canonical SHA-256 hash for REST request payloads. */
public final class CanonicalPayloadHasher {

  private static final ObjectWriter CANONICAL_WRITER =
      RESTObjectMapper.mapper()
          .writer()
          .with(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

  private CanonicalPayloadHasher() {}

  /**
   * Serialize the given object into a canonical JSON form and return its SHA-256 hex digest.
   *
   * <p>Notes:
   * - Map keys are sorted (ORDER_MAP_ENTRIES_BY_KEYS)
   * - Object fields are serialized in a deterministic order by the mapper
   * - No extra whitespace is included
   */
  public static String sha256Of(Object value) {
    try {
      String json = CANONICAL_WRITER.writeValueAsString(value);
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hashed = digest.digest(json.getBytes(StandardCharsets.UTF_8));
      return toHex(hashed);
    } catch (JsonProcessingException e) {
      throw new RESTException(e, "Failed to canonicalize request payload for hashing");
    } catch (NoSuchAlgorithmException e) {
      // Should never happen for SHA-256
      throw new IllegalStateException("SHA-256 algorithm not available", e);
    }
  }

  private static String toHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format(Locale.ROOT, "%02x", b));
    }
    return sb.toString();
  }
}


