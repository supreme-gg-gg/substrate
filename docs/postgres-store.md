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
messages before assigning metadata or versions.

Actors have a foreign key to their atespace with `ON DELETE RESTRICT`.
Consequently, creating an actor in a missing atespace and deleting a non-empty
atespace are rejected by the database even when they race with an earlier API
existence check.

## Reads, writes, and concurrency

Creates use `INSERT`; PostgreSQL constraint errors are mapped to the existing
store sentinel errors.

Actor and worker updates use one conditional `UPDATE ... RETURNING` statement.
The predicate includes the expected version and immutable fields. A successful
update increments the version. If no row matches, a point read distinguishes:

- a missing resource;
- a stale version, returned as `store.ErrPersistenceRetry`; or
- an attempted change to an immutable field.

Actor deletion includes the allowed suspended/crashed statuses in the `DELETE`
predicate. Atespace deletion relies on the actor foreign key. Worker deletion
is idempotent.

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
or atespace. Unlike Redis cursor tokens, they contain no shard topology.

## Worker notifications

Worker creates, updates, and deletes publish events through PostgreSQL
`LISTEN`/`NOTIFY`. The row mutation and `pg_notify` call share a transaction,
so a notification is delivered only after the corresponding write commits.

`WatchWorkers` holds a dedicated PostgreSQL connection and forwards decoded
events to the existing worker-watch channel. Notifications remain best effort:
if the connection is lost, the watch closes and the worker cache reconnects
and performs a full relist, as it does with Redis.

The payload is a compact JSON envelope containing the protojson worker and must
fit PostgreSQL's approximately 8 KiB notification limit. Oversized events fail
the write instead of silently skipping the notification.

## Workflow locks

The `leases` table preserves the existing token-and-TTL lock contract.
Acquisition is a conditional upsert that inserts a lease or atomically replaces
an expired lease using PostgreSQL's clock. Release deletes only when both the
key and owner token match.

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

- `cmd/ateapi/internal/store/atepg/atepg.go`: store operations, notifications,
  and leases.
- `cmd/ateapi/internal/store/atepg/schema.go`: embedded schema.
- `cmd/ateapi/internal/store/atepg/pagetoken.go`: keyset page tokens.
- `cmd/ateapi/internal/store/atepg/atepg_test.go`: contract and
  PostgreSQL-specific tests.
- `cmd/ateapi/main.go`: backend selection and connection setup.
- `manifests/ate-install/postgres.yaml`: development PostgreSQL deployment.
