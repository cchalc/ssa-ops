# Databricks Local-First Data App

## Goal

Build a local-first data application that fetches data from Databricks, syncs via Electric SQL, and provides reactive UI updates.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Databricks    │────▶│    Lakebase     │────▶│   Electric SQL  │────▶│  TanStack DB    │
│   SQL Warehouse │     │   (Postgres)    │     │   (sync layer)  │     │  (local state)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │                       │
   Query tables            Data lands here        Shape streams           useLiveQuery
   via logfood             via pipeline           to browser              Reactive UI
```

## Configuration

| Setting | Value |
|---------|-------|
| Deploy Profile | `fevm-cjc` (Lakebase, Apps) |
| Deploy Workspace | `https://fevm-cjc-aws-workspace.cloud.databricks.com/` |
| Data Profile | `logfood` (SQL Warehouse queries) |
| Data Workspace | `https://adb-2548836972759138.18.azuredatabricks.net/` |
| Lakebase Instance | `cjc-ssa-ops-dev` |
| Lakebase Database | `cjc_ssa_ops_dev` |
| Environment | direnv (`.envrc` + `.env`) |

---

## Progress

### ✅ Completed

- [x] Clone and sync repo with GitHub via jujutsu
- [x] Configure Databricks `logfood` profile
- [x] Set up direnv with `.envrc`
- [x] Add security exclusions to `.gitignore`
- [x] Document fish shell requirements
- [x] **Phase 1: Lakebase Setup**
  - [x] Create Lakebase instance `cjc-ssa-ops-dev` on `fevm-cjc` workspace
  - [x] Create database `cjc_ssa_ops_dev`
  - [x] Set up DAB for infrastructure deployment

### 🔄 In Progress

- [ ] **Phase 1b: Electric SQL Setup**
  - [ ] Configure Electric sync for Lakebase tables
  - [ ] Set up Electric client in the app

### ⏳ Upcoming

- [ ] **Phase 2: SQL Warehouse → Lakebase Pipeline**
  - [ ] Create ETL notebook/workflow
  - [ ] Define target schema in Lakebase
  - [ ] Schedule periodic sync

- [ ] **Phase 3: TanStack DB Collections**
  - [ ] Create collection with Electric adapter
  - [ ] Define TypeScript types
  - [ ] Set up shape definitions

- [ ] **Phase 4: Data Explorer UI**
  - [ ] Create data page route
  - [ ] Build data table component
  - [ ] Add filtering/search
  - [ ] Show sync status indicator

- [ ] **Phase 5: Offline Support**
  - [ ] Enable offline persistence
  - [ ] Handle reconnection sync
  - [ ] Test offline/online transitions

---

## Files to Create

```
src/
├── lib/
│   └── electric.ts         # Electric client setup
├── db/
│   ├── collections/
│   │   └── trips.ts        # Trips collection
│   └── schema.ts           # TypeScript types
├── routes/
│   └── data/
│       └── index.tsx       # Data explorer
└── components/
    ├── DataTable.tsx       # Table component
    └── SyncStatus.tsx      # Sync indicator
```

---

## Dependencies to Add

```bash
pnpm add @databricks/sql
```

Note: Electric SQL and TanStack DB already installed.

---

## Environment Variables

Defined in `.env` (copy from `.env.example`):

```env
# Lakebase connection
LAKEBASE_HOST=
LAKEBASE_DATABASE=
LAKEBASE_USER=
LAKEBASE_PASSWORD=

# Electric SQL
ELECTRIC_URL=

# Databricks SQL Warehouse (optional)
DATABRICKS_WAREHOUSE_ID=
```

---

## Demo Plan

Start with NYC Taxi trips to prove the flow:

1. Query `samples.nyctaxi.trips` from SQL Warehouse
2. Land subset in Lakebase `trips` table
3. Sync to browser via Electric
4. Display in data table UI

---

## Session Notes

_Add notes, decisions, and learnings here as we work._

- **2026-03-08**: Initial setup complete. Repo synced, direnv configured, fish shell documented.
