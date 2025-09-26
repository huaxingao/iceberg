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

import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.IdempotencyKeyConflictException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for managing idempotency keys in REST API operations.
 *
 * <p>This implementation provides idempotency guarantees for mutation operations by: - Storing
 * operation results associated with idempotency keys - Detecting conflicting usage of the same key
 * for different operations - Ensuring identical results are returned for repeated requests with the
 * same key
 *
 * <p>Key features: - Thread-safe concurrent access - Configurable key expiration - Validation of
 * key format - Scoped keys (operation + resource + key combination)
 */
public class IdempotencyHandler {
  private static final Logger LOG = LoggerFactory.getLogger(IdempotencyHandler.class);

  // Pattern for valid idempotency keys: alphanumeric, hyphens, underscores, periods
  private static final Pattern VALID_KEY_PATTERN = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9_.-]*$");
  private static final int MAX_KEY_LENGTH = 255;
  private static final int MIN_KEY_LENGTH = 1;

  // Default expiration time for idempotency keys (24 hours)
  private static final long DEFAULT_EXPIRATION_MILLIS = 24 * 60 * 60 * 1000L;

  private final ConcurrentMap<String, IdempotencyRecord> keyStore = Maps.newConcurrentMap();
  private final long expirationMillis;

  /** Creates a new idempotency handler with default settings. */
  public IdempotencyHandler() {
    this(DEFAULT_EXPIRATION_MILLIS);
  }

  /**
   * Creates a new idempotency handler with custom expiration time.
   *
   * @param expirationMillis how long to keep idempotency keys before expiring them
   */
  public IdempotencyHandler(long expirationMillis) {
    this.expirationMillis = expirationMillis;
  }

  // Test hook: fail finalization once after a successful operation to simulate commit-success but
  // finalize-failure. This leaves the record in IN_PROGRESS for reconciliation tests.
  private static final ThreadLocal<Boolean> FAIL_FINALIZE_ONCE = new ThreadLocal<>();
  private static final ThreadLocal<Boolean> RECONCILE_ON_IN_PROGRESS = new ThreadLocal<>();

  public static void testFailFinalizeOnce() {
    FAIL_FINALIZE_ONCE.set(Boolean.TRUE);
  }

  private boolean shouldFailFinalizeOnce() {
    Boolean flag = FAIL_FINALIZE_ONCE.get();
    if (flag != null && flag) {
      FAIL_FINALIZE_ONCE.remove();
      return true;
    }
    return false;
  }

  public static void testReconcileOnInProgressOnce() {
    RECONCILE_ON_IN_PROGRESS.set(Boolean.TRUE);
  }

  private boolean shouldReconcileOnInProgressOnce() {
    Boolean flag = RECONCILE_ON_IN_PROGRESS.get();
    if (flag != null && flag) {
      RECONCILE_ON_IN_PROGRESS.remove();
      return true;
    }
    return false;
  }

  /**
   * Execute an operation with idempotency guarantees (key-only baseline).
   *
   * @param idempotencyKey the idempotency key (may be null to skip idempotency)
   * @param operationType the type of operation (e.g. "updateTable", "createNamespace")
   * @param resourcePath the resource path (e.g. "/v1/mywarehouse/namespaces/sales/tables/orders")
   * @param requestHash unused in baseline key-only mode (kept for compatibility)
   * @param operation the operation to execute
   * @param <T> the return type of the operation
   * @return the result of the operation (either fresh or cached)
   */
  public <T> T handleOperation(
      String idempotencyKey,
      String operationType,
      String resourcePath,
      String requestHash,
      Supplier<T> operation) {

    if (idempotencyKey == null) {
      // No idempotency requested, execute directly
      return operation.get();
    }

    validateIdempotencyKey(idempotencyKey);

    String scopedKey = createScopedKey(operationType, resourcePath, idempotencyKey);

    // Clean up expired keys periodically
    cleanupExpiredKeys();

    // Reserve the key immediately to enforce IN_PROGRESS semantics
    IdempotencyRecord existing = keyStore.putIfAbsent(scopedKey, IdempotencyRecord.inProgress());

    if (existing != null) {
      // Existing record: return cached result if finalized or signal in-progress
      if (existing.state == State.FINALIZED) {
        if (existing.isExpired(expirationMillis)) {
          // Remove expired and treat as new
          keyStore.remove(scopedKey, existing);
          return handleOperation(idempotencyKey, operationType, resourcePath, requestHash, operation);
        }
        LOG.debug("Returning cached result for idempotency key: {}", scopedKey);
        return existing.getResult();
      }

      // IN_PROGRESS with matching hash
      if (shouldReconcileOnInProgressOnce()) {
        // Attempt to compute result now and finalize the record for reconciliation
        T result = operation.get();
        existing.finalizeWithResult(result);
        return result;
      }
      throw new CommitFailedException("request_in_progress");
    }

    // We own execution for this new key; mark as in-progress and then finalize
    IdempotencyRecord record = keyStore.get(scopedKey);
    try {
      LOG.debug("Executing operation for new idempotency key: {}", scopedKey);
      T result = operation.get();
      // Optionally simulate a finalize failure (test hook)
      if (shouldFailFinalizeOnce()) {
        throw new FinalizeStepFailed();
      }
      // finalize and cache result
      record.finalizeWithResult(result);
      return result;
    } catch (RuntimeException e) {
      // Remove the reservation to allow safe retry, unless this is a simulated finalize failure
      if (!(e instanceof FinalizeStepFailed)) {
        keyStore.remove(scopedKey, record);
      }
      LOG.debug("Operation failed for idempotency key {}, not caching", scopedKey, e);
      throw e;
    } catch (Exception e) {
      keyStore.remove(scopedKey, record);
      LOG.debug("Operation failed for idempotency key {}, not caching", scopedKey, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Validates the format and constraints of an idempotency key.
   *
   * @param key the key to validate
   * @throws IllegalArgumentException if the key is invalid
   */
  private void validateIdempotencyKey(String key) {
    Preconditions.checkArgument(key != null, "Idempotency key cannot be null");
    Preconditions.checkArgument(
        key.length() >= MIN_KEY_LENGTH && key.length() <= MAX_KEY_LENGTH,
        "Idempotency key length must be between %s and %s characters, got %s",
        MIN_KEY_LENGTH,
        MAX_KEY_LENGTH,
        key.length());
    Preconditions.checkArgument(
        VALID_KEY_PATTERN.matcher(key).matches(),
        "Idempotency key must match pattern %s, got '%s'",
        VALID_KEY_PATTERN.pattern(),
        key);
  }

  /**
   * Creates a scoped key that combines operation type, resource path, and idempotency key. This
   * ensures keys are scoped to specific operations and resources.
   */
  private String createScopedKey(String operationType, String resourcePath, String idempotencyKey) {
    return operationType + ":" + resourcePath + ":" + idempotencyKey;
  }

  /** Removes expired keys from the store. This is called periodically to prevent memory leaks. */
  private void cleanupExpiredKeys() {
    boolean hasRemovedKeys =
        keyStore.entrySet().removeIf(entry -> entry.getValue().isExpired(expirationMillis));

    if (hasRemovedKeys) {
      LOG.debug("Cleaned up expired idempotency keys");
    }
  }

  /** Gets the current number of stored idempotency keys. Useful for monitoring and testing. */
  public int getStoredKeyCount() {
    return keyStore.size();
  }

  /** Clears all stored idempotency keys. Useful for testing. */
  public void clear() {
    keyStore.clear();
  }

  /** Record of an idempotent operation, including the request hash and result. */
  private enum State {
    IN_PROGRESS,
    FINALIZED
  }

  private static class FinalizeStepFailed extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

  private static class IdempotencyRecord {
    private volatile Object result;
    private volatile State state;
    private final long createdAt;

    private IdempotencyRecord(State state) {
      this.state = state;
      this.createdAt = System.currentTimeMillis();
    }

    static IdempotencyRecord inProgress() { return new IdempotencyRecord(State.IN_PROGRESS); }

    synchronized <T> void finalizeWithResult(T value) {
      this.result = value;
      this.state = State.FINALIZED;
    }

    boolean isExpired(long expirationMillis) {
      return System.currentTimeMillis() - createdAt > expirationMillis;
    }

    @SuppressWarnings("unchecked")
    <T> T getResult() {
      return (T) result;
    }
  }

  /**
   * Finalize an existing IN_PROGRESS record for the given scoped key with a provided result.
   * Intended for reconciliation flows when the operation has succeeded but finalization failed.
   */
  public <T> T finalizeInProgress(
      String idempotencyKey,
      String operationType,
      String resourcePath,
      String requestHash,
      T result) {
    String scopedKey = createScopedKey(operationType, resourcePath, idempotencyKey);
    IdempotencyRecord record = keyStore.get(scopedKey);
    Preconditions.checkArgument(record != null, "No idempotency record found for key");
    Preconditions.checkArgument(record.state == State.IN_PROGRESS, "Record is not IN_PROGRESS");
    record.finalizeWithResult(result);
    return result;
  }

  /** Utility class to generate request hashes for different operation types. */
  public static class RequestHashGenerator {

    /**
     * Generate a hash for a request object. This implementation uses the object's string
     * representation, but could be enhanced to use more sophisticated hashing.
     */
    public static String generateHash(Object request) {
      if (request == null) {
        return "null";
      }
      return String.valueOf(request.toString().hashCode());
    }
  }
}
