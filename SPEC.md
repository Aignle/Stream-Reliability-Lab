# Stream Reliability Lab Specification

## Goal

Build a small, locally runnable portfolio project that demonstrates how to
test, observe, and analyze the reliability of a real-time event-driven system.

The MVP will simulate creator-platform events, deliver them to a WebSocket
service, persist their lifecycle, render accepted events in a browser overlay,
and summarize reliability measurements in a Streamlit dashboard.

## Users

The MVP has one local operator who can:

- create and run a deterministic event simulation;
- observe events appearing in a browser overlay;
- introduce duplicate deliveries, intentionally invalid payloads, and
  controlled connection faults;
- inspect event-level lifecycle records; and
- review run-level reliability metrics.

## Event lifecycle

Each simulated event has a UUID, a run UUID, an event type, a UTC creation
timestamp, and a JSON payload. A run records its deterministic random seed and
configuration.

For each delivery attempt, the service must:

1. validate the incoming event;
2. persist the accepted event and delivery attempt before acknowledging it;
3. identify whether the event has already been processed;
4. record duplicate deliveries;
5. create at most one visible effect for an event; and
6. publish the visible effect to connected overlay clients.

Delivery is at least once. Idempotent processing prevents duplicate visible
effects; the system does not claim universal exactly-once delivery.

## MVP components

### Simulator

A Python process generates deterministic event sequences from a configured
seed. It can control event count, event rate, duplicate probability, and a
small set of failure scenarios. It delivers events to the service and records
client-observed outcomes.

### Event service

A FastAPI application exposes health and run-inspection HTTP endpoints plus
WebSocket endpoints for event delivery and overlay updates. Route handlers
delegate validation, persistence, idempotency, and metrics work to domain and
repository layers.

### Storage

DuckDB is the only MVP database. It stores runs, events, delivery attempts,
processing outcomes, and timestamps needed to calculate reliability metrics.
The schema must make duplicate attempts auditable without duplicating visible
effects.

### Browser overlay

A small HTML, CSS, and JavaScript page connects to the service over WebSocket
and renders each processed event once. React is not used. Playwright verifies
connection behavior, event rendering, and duplicate suppression.

### Analysis dashboard

A Streamlit application reads run data through FastAPI analytics endpoints and
reports at least:

- events generated, accepted, processed, and visibly rendered;
- total delivery attempts and duplicate attempts;
- end-to-end latency distribution;
- payload-rejection count and rate, with identity conflicts reported
  separately; and
- run configuration and deterministic seed.

## Local operation

Docker Compose starts the event service and analysis dashboard. Development
commands must also support running tests and static checks outside containers.
No real creator-platform credentials or network integrations are required.

## Acceptance criteria

The MVP is complete when:

- a deterministic simulation can be started locally;
- events travel through the WebSocket service and appear in the overlay;
- accepted events are persisted before acknowledgement;
- intentionally repeated deliveries are recorded as duplicates;
- a duplicate delivery never creates a second visible effect;
- the dashboard reports run-level reliability metrics from DuckDB;
- pytest, Ruff, mypy, and relevant Playwright checks pass in GitHub Actions;
- Docker Compose provides a documented local startup path; and
- the repository contains no credentials or real user data.

## Non-goals

The MVP does not include real Twitch or TikTok authentication, Kafka,
Kubernetes, React, cloud deployment, user accounts, payments, AI features, or
more than one database.

## Planned milestones

1. Repository and Python tooling scaffold.
2. Event models, DuckDB schema, and persistence layer.
3. FastAPI delivery service with idempotent processing.
4. Deterministic simulator and failure controls.
5. Browser overlay and Playwright coverage.
6. Streamlit reliability dashboard.
7. Docker Compose, GitHub Actions, and complete operating documentation.
