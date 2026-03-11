# ADR 001: Run Electric SQL from Source

## Status
Accepted

## Context
Electric SQL can be deployed via:
1. Docker container (`electricsql/electric:latest`)
2. Electric Cloud (hosted service)
3. From source (Elixir application)

## Decision
Run Electric SQL from source as an Elixir application for local development.

## Rationale
- Avoids Docker dependency
- More control over the sync service
- Better debugging and development experience
- Aligns with preference for native tooling

## Consequences

### Prerequisites
- Elixir/Erlang runtime (via asdf)
- Node.js and pnpm (via asdf)

### Setup
Electric must be cloned and run separately from the main project.

### Trade-offs
- More complex initial setup vs. single `docker compose up`
- Need to manage Elixir dependencies
- Docker option remains available as fallback (see `docker-compose.yml`)

## Implementation
See `docs/electric-setup.md` for setup instructions.
