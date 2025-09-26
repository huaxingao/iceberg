---
title: "Iceberg REST Catalog Idempotency"
---

# Iceberg REST Catalog Idempotency

- Authors: Your Name (@handle)
- Status: Draft
- Target area: REST Catalog API, REST clients
- Affected modules: `open-api`, `core` (REST server/client), catalog providers
- Tracking: GitHub issue TBA

## Motivation

- Retries of mutation calls (network timeouts, transient 5xx, client retries) can create duplicate resources or conflicting state.
- Lack of client-controlled idempotency makes pipelines fragile and retry-unsafe.
- Standardizing idempotency semantics across REST implementations enables safe retries and consistent behavior.

## Goals

- Add client-controlled idempotency to REST Catalog mutation endpoints.
- Define clear semantics: first processed request wins; duplicates return the same result without reprocessing.
- Detect conflicts when the same key is reused for different requests.
- Provide a production-ready server design that works across nodes/processes.
- Maintain backward compatibility.

## Non-goals

- Idempotency for read-only operations.
- Engine-side retries or transaction semantics beyond the REST API scope.
- Catalog-specific storage/HA guidance beyond what’s needed for idempotency records.

## User Stories / Use Cases

- Retry-safe table creation, updates, renames, and namespace operations during infra/network issues
  - Feature: Idempotency-Key (transport retries), capability discovery, client retry algorithm
  - Acceptance: duplicate requests replay original response; 409 on key reuse with different payload

- Streaming micro-batch dedup across restarts/time (e.g., foreachBatch)
  - Feature: Operation token (e.g., appId + batchId) enforced by catalog; TTL > snapshot retention
  - Acceptance: first attempt applies; subsequent attempts with same token are no-ops/replay even after restarts

- Batch job dedup (daily/hourly pipelines, backfills)
  - Feature: Operation token (job-run-id, business batch-id)
  - Acceptance: re-runs with same token do not re-apply updates

- Multi-sink writes (foreachBatch to multiple tables)
  - Guidance: one token per table commit (table-scoped); separate streaming writes preferred for parallelism

- Trivially idempotent metadata ops (add/remove column, set property)
  - Feature: Idempotency-Key sufficient for safe retries; operation token optional

- Catalogs without idempotency support
  - Feature: Optional client-side deconfliction fallback (refresh + snapshot-id match) for table updates
  - Acceptance: if refreshed current snapshot equals intended snapshot, treat commit as succeeded

- Snapshot expiration resilience
  - Feature: Operation token store TTL decoupled from snapshot GC; dedup survives snapshot retention

- Cross-language clients
  - Feature: `/v1/config.capabilities` discovery for both Idempotency-Key and Operation tokens

## Public API Changes

- REST API
  - New optional header: `Idempotency-Key` on mutation endpoints.
  - New 409 error type: `IdempotencyKeyConflictException` when the same key is reused for a different request.
  - Header constraints: `[A-Za-z0-9][A-Za-z0-9_.-]*`, length 1..255.
- OpenAPI
  - Add `components.parameters.idempotency-key`.
  - Attach to `POST`/mutation endpoints: `createTable`, `updateTable`, `renameTable`, `createNamespace`, `updateProperties`, `registerTable`, `transactions/commit`, `reportMetrics`, etc.
  - Add `IdempotencyKeyConflictResponse` (409) referencing `IcebergErrorResponse`.
- Client libraries
  - Clients may auto-generate idempotency keys for mutation `POST`s, or accept user-provided keys.
  - Map HTTP 409 with type `IdempotencyKeyConflictException` to the corresponding client exception.

- Server capabilities (discovery)
  - Extend `/v1/config` to advertise idempotency support and minimum retention:
    - `capabilities.idempotency.supported` (boolean)
    - `capabilities.idempotency.tokenLifetime` (string, ISO-8601 duration, e.g., `PT30M`, `PT24H`)
  - Example response fragment:
    ```json
    {
      "capabilities": {
        "idempotency": {
          "supported": true,
          "tokenLifetime": "PT30M"
        }
      }
    }
    ```
  - If unsupported, either omit the `idempotency` object or set `supported=false`.

## Detailed Design

### Semantics

- If `Idempotency-Key` is absent: current behavior (non-idempotent).
- If present:
  - First request processes normally, persists the idempotent record (scoped to operation type + resource path).
  - Duplicate requests with the same key must return the prior response, not re-execute.
  - If the same key is used but request hash differs, return 409 `IdempotencyKeyConflictException`.
  - Failures are never cached; a subsequent retry with the same key runs the operation again.
  - Keys may expire after TTL; expired keys allow reprocessing on first subsequent call.

### Idempotency Scope (Scoped Key)

- Scoped key = `operationType + '\n' + resourcePath + '\n' + idempotencyKey`.
- Prevents collisions when the same `Idempotency-Key` is used across different operations/resources.
- Enables precise conflict detection within the intended operation/resource boundary.

### Request Hashing

- Compute `requestHash = SHA-256(canonicalPayload)` where `canonicalPayload` includes:
  - Raw request body bytes OR canonical JSON representation
  - Content-type, and optional versioning headers if they alter semantics
- Exclude `Idempotency-Key` itself from the hash.
- Used to differentiate requests under the same scoped key: mismatch → 409.

### Storage Model (Durable, Cross-node)

- Use a storage-backed record:
  - `scoped_key` (PK/unique)
  - `request_hash`
  - `status` (`SUCCEEDED|FAILED|IN_PROGRESS` optional)
  - `http_status`
  - `response_headers` (optional, small)
  - `response_body` (bounded length)
  - `created_at`, `expires_at`, `version`
- Must support atomic insert with unique constraint on `scoped_key`.
- Recommended backends: JDBC (transactional DB), Redis (SETNX + TTL), or catalog metadata store if it supports atomicity and uniqueness.

### Write Path and Concurrency

Pseudocode for a single-node or multi-node deployment:

```text
if key missing:
  execute op
  persist (scoped_key, request_hash, response) atomically (insert)
  return response
else:
  fetch record by scoped_key
  if record.expires_at < now -> treat as missing (optionally delete)
  if record.request_hash != request_hash -> 409 IdempotencyKeyConflictException
  return stored response
```

Cross-node safety via uniqueness (DB unique constraint or Redis SETNX). Do not cache failures.

### Capabilities Discovery

- Servers SHOULD expose idempotency support and retention via `/v1/config` as described above.
- `tokenLifetime` is the minimum duration records are retained; servers MAY retain longer.
- If absent or `supported=false`, clients MUST treat the server as non-idempotent.

### Client Retry Algorithm

- Preconditions: `/v1/config.capabilities.idempotency.supported == true`.
- Maintain `firstAttemptTime` and reuse the same `Idempotency-Key` for retries.
- On network timeout or HTTP 5xx/502/503/504 (or commit state unknown):
  1. If `Retry-After` is present, sleep accordingly (bounded by client policy).
  2. If `now - firstAttemptTime < tokenLifetime - clockSkew`, retry with the same key.
  3. Use capped exponential backoff with jitter; stop when total time exceeds the bound.
- If unsupported, clients MAY use a best-effort fallback for table commits (e.g., fetch latest metadata and verify the intended snapshot).

### Optional Client-side Deconfliction Fallback (when unsupported)

When `/v1/config` does not advertise idempotency support, the client MAY perform a lightweight
post-commit verification for table updates to mitigate "commit state unknown" scenarios:

- Scope: table update/commit operations only (not general namespace/table creation).
- Trigger conditions: network timeout, HTTP 5xx, or explicit "commit state unknown" error.
- Procedure:
  1. Fetch the latest table metadata.
  2. Compare against the intended commit (e.g., snapshot ID, metadata location, or commit token if available).
  3. If a match is found, treat the prior attempt as succeeded and return success to the caller.
  4. If no match is found, surface the error to the caller (do not blindly retry without idempotency support).
- Notes:
  - This is best-effort and not a substitute for server-side idempotency.
  - Implementations SHOULD bound the number of lookup attempts and apply exponential backoff.
  - Engines/catalogs may expose stronger commit tokens that improve match reliability.

### Expiration and Cleanup

- Set `expires_at = created_at + ttl`.
- Background cleanup job (DB scheduled job, app cron), or rely on store TTL (e.g., Redis).
- Reads should treat expired records as missing.

### Error Handling

- If key reused with different `requestHash`: return 409 with type `IdempotencyKeyConflictException`.
- On retries: duplicates return the stored response when available.

### Observability

- Metrics: idempotency hit/miss rate, conflicts, in-progress wait count, storage errors.
- Logs: include `scoped_key`, `request_hash`, and operation type.

### Security & Privacy

- Validate header format/length.
- Bound cached response size.
- Do not store sensitive headers; store minimal replay data.
- Authorization follows the underlying operation’s checks.

## Compatibility, Deprecation, and Migration Plan

- Backward compatible: header is optional; existing clients unaffected.
- Rolling upgrades: introduce storage schema first; enable feature flag after schema is present.
- Failure modes: if storage unavailable, operations without the header continue; with the header, return 503 to avoid violating semantics.

## Performance Considerations

- Overhead only when header is present: request hashing, storage calls, and bounded response serialization.
- Size storage for QPS and TTL.

## Testing Plan

- Unit: validation, scoping, hash, first-vs-duplicate, conflict, TTL, no-cache-on-failure.
- Concurrency: multi-thread tests assert single execution.
- Integration: JDBC/Redis backends; multi-process uniqueness enforcement.
- Client: auto-generated vs provided keys; 409 mapping.

## Rollout / Adoption Plan

- Phase 1: OpenAPI spec, server feature flag, client error mapping.
- Phase 2: JDBC reference implementation; response replay storage.
- Phase 3: Redis implementation; metrics/observability.
- Phase 4: Enable by default for new REST catalog deployments.

## Operational Configuration (non-normative)

- Names are illustrative and may vary by provider:
  - `rest.idempotency.enabled` (bool, default: false)
  - `rest.idempotency.ttl-ms` (default: 24h)
  - `rest.idempotency.store` (`jdbc|redis|custom`)
  - `rest.idempotency.max-response-bytes` (e.g., 256KB)
  - `rest.idempotency.cleanup.cron` or rely on store TTL
  - `rest.idempotency.request-hash-algorithm` (default: SHA-256)

## Alternatives Considered

- Server-generated idempotency tokens (requires preflight; less ergonomic than header).
- Caching failures (rejected to keep semantics clean; failures should be retried).
- Distributed locks (more operational complexity; uniqueness constraints scale better).
- Only duplicate detection without replay (worse UX; re-execution risks side effects).

## Open Questions

- Should we support “in-progress” signaling (202 + polling token) vs brief 409/429 with `Retry-After`? Start simple; revisit later.
- Standardize request canonicalization beyond raw body + critical headers? Start with SHA-256 over raw body; allow extensions.

## References

- REST Catalog OpenAPI (to be updated with `Idempotency-Key` and 409 response)
- Examples: `docs/idempotency-examples.md`

