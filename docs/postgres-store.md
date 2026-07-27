# PostgreSQL store

## Status

Implemented as an experimental `ateapi` persistence backend. Redis remains the
default.

## Overview

`atepg` implements the same `store.Interface` used by `ateredis`, so the
control API, worker cache, syncer, and debug API do not depend on the selected
backend. `ateapi` chooses the implementation once at startup:

```text
--store-backend=redis|postgres
--postgres-connection-string=<libpq DSN or URI>
```

The PostgreSQL implementation uses `pgxpool` directly. It does not introduce
an ORM or a second public storage abstraction.

## Shared store contract

The contract is defined in `cmd/ateapi/internal/store/store.go`. Both backends
implement the following behavior:

| Resource | Operations | Contract details |
|---|---|---|
| Atespace | create, get, exists, list, delete | Create returns the stored resource with server-assigned metadata. Delete only succeeds for an empty atespace and returns the deleted resource. |
| Actor | create, get, update, list, delete | Create and update return the stored resource without mutating the input. Updates use an expected version. Delete only accepts suspended or crashed actors and returns the deleted resource. An empty atespace argument to `ListActors` means all atespaces. |
| Worker | create, get, update, list, delete, watch | Updates use an expected version. Delete is idempotent. Watches deliver create, update, and delete events until closed, cancelled, or disconnected. |
| Workflow lock | acquire | `AcquireLock` returns an automatically renewed `store.Lock`. Its context is cancelled if the lease is lost, and `Close` stops renewal and releases the lease. |
| Debug data | clear all | `DebugClearAll` removes resource and lock data for local testing and debugging. |

All list methods take a page size and opaque page token and return a page plus
the next token. Callers must not interpret tokens or transfer them between
backends. The shared sentinel errors are:

- `store.ErrNotFound` for a missing resource;
- `store.ErrAlreadyExists` for a create conflict;
- `store.ErrPersistenceRetry` for an optimistic-concurrency conflict;
- `store.ErrFailedPrecondition` when resource state prevents an operation; and
- `store.ErrLockConflict` when another client owns a workflow lock.

## Data model

The backend creates four tables using an embedded, idempotent schema:

| Table | Primary key | Purpose |
|---|---|---|
| `atespaces` | `name` | Atespace state |
| `actors` | `(atespace, name)` | Actor state |
| `workers` | `(worker_namespace, worker_pool, worker_pod)` | Worker state |
| `leases` | `key` | TTL-based workflow locks |

Resource tables store:

- native columns for keys, versions, timestamps, ordering, and fields used by
  SQL preconditions; and
- the complete binary protobuf in a `BYTEA` column.

This keeps protobufs as the source of truth while allowing PostgreSQL to
enforce integrity and perform indexed updates and listing. Writes clone input
messages before assigning metadata or versions, matching `ateredis`. Redis
stores its resource protobufs as protojson; PostgreSQL stores the binary
encoding.

Actors have a foreign key to their atespace with `ON DELETE RESTRICT`.
Consequently, creating an actor in a missing atespace and deleting a non-empty
atespace are rejected by the database even when they race with an earlier API
existence check. `ateredis` performs separate existence/emptiness checks, so it
cannot enforce those two relationships atomically.

## Reads, writes, and concurrency

Creates use `INSERT`. Actor and atespace creates assign UID, version 1, and
create/update timestamps and return the persisted clone; worker creation
assigns version 1 and retains the interface's error-only return. PostgreSQL
constraint errors are mapped to the shared store sentinel errors. In
particular, an actor whose atespace is missing returns
`store.ErrFailedPrecondition`.

Actor and worker updates use one conditional `UPDATE ... RETURNING` statement.
The predicate includes the expected version and immutable fields. A successful
actor update returns the persisted clone; worker updates retain the interface's
error-only return. If no row matches, a point read distinguishes:

- a missing resource;
- a stale version, returned as `store.ErrPersistenceRetry`; or
- an attempted change to an immutable field.

Actor deletion includes the allowed suspended/crashed statuses in the
`DELETE ... RETURNING` predicate. Atespace deletion uses
`DELETE ... RETURNING` and relies on the actor foreign key. Both operations
return the deleted protobuf. Worker deletion is idempotent.

Actor-to-worker assignment still follows the existing workflow and updates the
two resources separately. The PostgreSQL backend intentionally does not change
control-plane behavior while changing persistence.

## Listing and page tokens

Lists use ordered keyset pagination and fetch one extra row to determine
whether a next page exists:

- atespaces by `name`;
- actors within an atespace by `name`;
- global actors by `(atespace, name)`; and
- workers by `(worker_namespace, worker_pool, worker_pod)`.

Opaque base64-encoded tokens contain a format version, resource kind, list
scope, and the last key. Tokens are rejected when reused with another resource
or atespace.

This differs from `ateredis`, which uses `SCAN` across sorted Redis masters.
Its token contains a shard hash and cursor, its result order is not a resource
ordering guarantee, and a topology change can invalidate the token.
PostgreSQL tokens contain no shard topology and provide a stable key order,
but, like Redis `SCAN`, keyset pagination is not a snapshot: concurrent writes
can affect later pages.

## Worker notifications

Worker creates, updates, and deletes publish events through PostgreSQL
`LISTEN`/`NOTIFY`. The row mutation and `pg_notify` call share a transaction,
so a notification is delivered only after the corresponding write commits.
If event serialization, notification, or commit fails, the resource mutation
also fails. By contrast, `ateredis` publishes after its resource mutation and
logs publish failures, so the write can succeed without a notification.

`WatchWorkers` holds a dedicated PostgreSQL connection and forwards decoded
events through `store.WorkerWatch.Events`. Calling `Close`, cancelling the
parent context, or losing the connection closes the event channel. The worker
cache treats closure as a signal to re-subscribe and performs a full relist, as
it does with Redis. Notifications remain best effort and are not replayed.

The payload is a compact JSON envelope containing the protojson worker and must
fit the backend's 8,000-byte limit for PostgreSQL notifications. Oversized
events fail and roll back the write instead of silently skipping the
notification.

## Workflow locks

The `leases` table implements the same automatically renewed lock contract as
`ateredis`. Acquisition is a conditional upsert that inserts a lease or
atomically replaces an expired lease using PostgreSQL's clock. A held lease is
renewed periodically only if its key, owner token, and unexpired state still
match. Transient renewal errors are retried within the renewal deadline. If
renewal cannot preserve ownership, `Lock.Context()` is cancelled so the
workflow can stop relying on exclusive access.

`Lock.Close()` is idempotent: it stops the renewal goroutine and deletes the row
only when both key and owner token match. A bounded background context is used
for release so cancellation of the acquiring request does not prevent cleanup;
if release fails, expiry eventually makes the lease reclaimable.

This deliberately uses rows rather than advisory locks because lease expiry
must not depend on the lifetime of a client connection.

## Startup and deployment

Selecting PostgreSQL requires a connection string. Startup opens a pool, pings
the database, and applies the schema; any failure prevents `ateapi` from
starting. TLS is configured with standard connection-string parameters such as
`sslmode`, `sslrootcert`, `sslcert`, and `sslkey`.

The repository includes a single-replica development PostgreSQL StatefulSet:

```bash
ATE_API_STORE_BACKEND=postgres \
ATE_API_POSTGRES_CONNECTION_STRING="${PG_DSN}" \
./hack/install-ate.sh \
  --deploy-postgres \
  --create-api-server-env-vars \
  --deploy-ate-apiserver
```

The install script writes the selected backend and connection string to the
`ate-api-server-envvars` ConfigMap. The API server manifest reads both values
through its existing `@env` flag mechanism.

## Testing

`atepg` runs the backend-neutral store contract suite against a real
PostgreSQL container. Additional tests cover PostgreSQL-specific behavior,
including foreign-key enforcement, transactional worker notifications,
keyset-token validation, lock expiry and contention, and debug clearing.

The implementation is exercised by the storage and lifecycle workloads under
`benchmarking/`. See
[`../benchmarking/REPRODUCING_ATEPG_BENCHMARKS.md`](../benchmarking/REPRODUCING_ATEPG_BENCHMARKS.md)
for the reproducible GKE benchmark procedure.

## Current limitations

This is a development and benchmarking implementation, not a production
PostgreSQL deployment design. In particular:

- the schema is created at startup rather than managed by versioned migrations;
- the included StatefulSet is single-replica and does not provide backup,
  failover, or disaster recovery;
- database roles are not separated by privilege;
- worker notifications are not durable; and
- actor-to-worker assignment is not a single database transaction.

## Code map

- `cmd/ateapi/internal/store/store.go`: backend-neutral interface, events,
  watches, locks, and sentinel errors.
- `cmd/ateapi/internal/store/ateredis/ateredis.go`: default Redis
  implementation whose contract `atepg` follows.
- `cmd/ateapi/internal/store/atepg/atepg.go`: store operations, notifications,
  and leases.
- `cmd/ateapi/internal/store/atepg/schema.go`: embedded schema.
- `cmd/ateapi/internal/store/atepg/pagetoken.go`: keyset page tokens.
- `cmd/ateapi/internal/store/atepg/atepg_test.go`: contract and
  PostgreSQL-specific tests.
- `cmd/ateapi/main.go`: backend selection and connection setup.
- `manifests/ate-install/postgres.yaml`: development PostgreSQL deployment.
