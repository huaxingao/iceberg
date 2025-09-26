# Apache Iceberg REST API Idempotency Extension - POC Implementation

## Overview

This POC adds client-controlled idempotency to the Iceberg REST Catalog via an optional `Idempotency-Key` header. The baseline semantics are key-only: the first accepted request wins; later requests with the same key in the same (operation, resource) scope do not re-execute and return the original finalized result.

## What’s Included

- OpenAPI updates (`open-api/rest-catalog-open-api.yaml`)
  - `components.parameters.idempotency-key`
  - Capability discovery fields under `CatalogConfig.capabilities.idempotency` (supported, tokenLifetime)
  - Representative response for in-progress duplicates (409)
- Server-side components
  - `IdempotencyHandler` — minimal in-memory handler (key-only baseline)
  - Test adapter wiring in `open-api` test fixtures
- Client-side components
  - `IdempotencyAwareRESTClient` — helper to send/reuse keys across retries

## Baseline Semantics (Key-Only)

- Scope: keys apply within (operation type, resource path, Idempotency-Key)
- First request wins: server executes and finalizes the result
- Duplicate with same key:
  - If FINALIZED: return the original response (e.g., 200/201/204 or terminal 4xx)
  - If IN_PROGRESS: return 409 (request_in_progress); servers MAY block and replay
- Failures (5xx) are not stored; caller may retry with the same key
- Keys may expire; retention window advertised via `/v1/config` (`tokenLifetime`)

## Optional Mode: Payload-Binding (Non-Baseline)

Some deployments may enable a guardrail that binds the key to a canonical payload fingerprint (e.g., canonical JSON + SHA‑256). This mode is optional and can be advertised via `/v1/config` so clients/tests can adapt. The POC keeps server logic baseline (key-only) but the client/utilities contain a canonical hasher for experimentation.

## Error Handling

- 2xx/201/204: success; duplicates return the original success
- 409: duplicate while first request is in progress (request_in_progress)
- 5xx: transient; not cached; clients MAY retry (with the same key) when idempotency is supported

## Validation & Limits

- Key format: `^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`, length 1–255
- Generation: recommend cryptographically random (e.g., UUID v4)
- Expiration: implementation-specific; advertise minimum retention via `/v1/config`

## Files

- `open-api/rest-catalog-open-api.yaml` — spec updates
- `core/src/main/java/org/apache/iceberg/rest/IdempotencyHandler.java` — in-memory server handler
- `open-api/src/testFixtures/java/.../RESTServerCatalogAdapter.java` — fixture wiring
- `core/src/main/java/org/apache/iceberg/rest/IdempotencyAwareRESTClient.java` — client helper
- Tests under `core` and `open-api` exercising baseline behavior

## Quick Examples

Server (fixture) uses the handler to wrap mutations; duplicates return prior result or 409 when in progress. Client helper lets you create a context that reuses a generated key across retries.

## Compatibility

- Header is optional; existing clients continue to work
- Capability discovery lets clients enable/disable automatic same-key retries
- Storage/cleanup of keys is implementation-specific; POC uses in-memory store

## Next Steps

- Harden capability gating in CTK and `iceberg-rest-fixture`
- Optional payload-binding experiments gated by `/v1/config`
- Cross-catalog testing with different retention windows