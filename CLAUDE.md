# SSA Activity Dashboard

An SSA operations dashboard built with TanStack Start, connecting to Databricks GTM data.

## IMPORTANT: Read GTM Docs Before Working on Metrics

Before working on any metrics, data model, or SQL:

1. **Read GTM Gold Documentation**: `docs/gtmhub_docs.md` contains links to authoritative docs
2. **Understand the Medallion Architecture**: Bronze → Silver → Gold layers
3. **Use GTM naming conventions**: `[account/individual]_[metric]_[timegrain]`
4. **Query logfood to understand available data** before designing metrics

## GTM Data Model Reference

### Key GTM Tables (on logfood / main catalog)

| Layer | Table | Description |
|-------|-------|-------------|
| Gold | `main.gtm_gold.account_obt` | Account-level metrics at fiscal quarter granularity |
| Gold | `main.gtm_gold.individual_obt` | Individual-level metrics at fiscal quarter |
| Gold | `main.gtm_gold.account_period_over_period` | Daily snapshots for trend analysis |
| Gold | `main.gtm_gold.account_consumption_daily` | Daily consumption metrics |
| Gold | `main.gtm_gold.usecase_pipe_gen_amount` | Pipe gen tracking |
| Silver | `main.gtm_silver.use_case_detail` | UCO/use case data (current) |
| Silver | `main.gtm_silver.use_case_detail_history` | UCO historical snapshots |
| Silver | `main.gtm_silver.opportunity_detail` | Opportunity data (current) |
| Silver | `main.gtm_silver.account_dim` | Account dimension with owner |
| Silver | `main.gtm_silver.individual_hierarchy_salesforce` | User hierarchy |
| Silver | `main.gtm_silver.targets_account` | Account-level targets |

### SSA Source Tables (stitch.salesforce on logfood)

| Table | Description |
|-------|-------------|
| `stitch.salesforce.approvalrequest__c` | ASQ (Approval/Support Request) records |
| `stitch.salesforce.approved_usecase__c` | UCO (Use Case Opportunity) records |
| `stitch.salesforce.user` | Salesforce user records |
| `stitch.salesforce.account` | Customer accounts |

### Key Design Principles

1. **Only accounts with valid owners appear in GTM Gold** - owner must be active AE
2. **OBTs aggregated at fiscal quarter level** - use PoP tables for daily granularity
3. **Fiscal year ends January 31** (Databricks FY)
4. **SSAs not impacted by security hardening** - can see all GTM data
5. **Never hardcode BU filters** - make views portable

### Naming Conventions

- Tables: `[account/individual]_[metric]_[timegrain]` (e.g., `account_consumption_daily`)
- OBTs: `[account/individual]_obt`
- Targets: `target_[metric]`
- Forecasts: `forecast_[metric]`
- Metric Views: `mv_[domain]_[focus]` (e.g., `mv_asq_operations`)

### GTM Functions (utility views)

- `main.gtm_gold.check_user_setup` - Validate user configuration
- `main.gtm_gold.check_hub_visibility` - Check user visibility rules
- `main.gtm_gold.check_pipe_gen_status` - Diagnose pipe gen issues
- `main.gtm_gold.check_user_targets` - Look up user targets

### Refresh Cadences

- Consumption actuals: Once daily ~11am PST
- OBTs & Materialized Views: Every 1 hour
- GTM Silver/Gold Workflows: Every 2 hours
- Pipe Gen, Clari, Use Cases: Every 2 hours

### SSA-Specific Data (approval_request_detail)

Key columns for ASQ analysis:
- `approval_request_id`, `approval_request_name` - identifiers
- `owner_user_id`, `owner_user_name` - SSA owner
- `account_id`, `account_name` - customer
- `status` - Complete, Approved, Rejected, In Progress, New, Assigned
- `support_type` - Platform Administration, Production Architecture Review & Design, etc.
- `technical_specialization` - Data Science, Data Engineering, Platform, etc.
- `business_unit` - AMER Enterprise & Emerging, AMER Industries, EMEA, APJ
- `region_level_1` - CAN, RCT, FINS, etc.
- `estimated_effort_in_days`, `actual_effort_in_days` - effort tracking
- `created_date`, `target_end_date`, `actual_completion_date` - SLA dates

### Business Unit Mapping

| business_unit | region_level_1 | Description |
|---------------|----------------|-------------|
| AMER Enterprise & Emerging | CAN | Canada |
| AMER Enterprise & Emerging | RCT, EE & Startup, DNB, CMEG, LATAM | US regions |
| AMER Industries | MFG, FINS, HLS, PS | Verticals |
| EMEA | SEMEA, UKI, Central, BeNo, Emerging | Europe |
| APJ | ANZ, India, Asean + GCR, Korea, Japan | Asia-Pacific |

### Joining ASQ to Business Metrics

```sql
-- Link ASQ work to consumption impact
SELECT
  a.approval_request_name,
  a.account_name,
  a.status,
  a.business_unit,
  ao.dbu_dollars_qtd,
  ao.spend_tier
FROM main.gtm_silver.approval_request_detail a
LEFT JOIN main.gtm_gold.account_obt ao
  ON a.account_id = ao.account_id
  AND ao.fiscal_year_quarter = "FY'26 Q4"
WHERE a.snapshot_date = (SELECT MAX(snapshot_date) FROM main.gtm_silver.approval_request_detail)
```

## Shell Environment

**Fish Shell Only**: This machine runs fish shell. Always use fish shell syntax.

- No `for x in ...; do ... done` - use `for x in ...; ...; end`
- No `$()` for command substitution in assignment - use `set VAR (command)`
- No `export VAR=value` - use `set -x VAR value`
- No `&&` chaining in some contexts - use `; and` or separate commands

## Environment Setup

This project uses direnv. Environment is configured via:
- `.envrc` - Sets `DATABRICKS_CONFIG_PROFILE=logfood` and loads `.env`
- `.env` - Local secrets (gitignored, copy from `.env.example`)

## Stack

- **TanStack Start** - Full-stack React framework (SPA/SSR, deploys everywhere)
- **Radix UI** - Accessible component library with themes
- **vite-plugin-capsize-radix** - Pixel-perfect typography
- **Dozens of font pairings included** - Ask the agent to set one up

## Project Structure

```
src/
├── components/
│   ├── Header.tsx        # App header with ThemePicker
│   └── ThemePicker.tsx   # Font theme dropdown
├── contexts/
│   └── ThemeContext.tsx  # Font theme state + CSS variable switching
├── routes/
│   ├── __root.tsx        # Root layout, CSS imports, Theme wrapper
│   └── index.tsx         # Home page
├── router.tsx
└── styles.css            # CSS custom properties for fonts
```

## Styling Rules

### No spacing props on text elements

Capsize normalizes text boxes to actual glyph bounds (no extra leading), so spacing between text elements must be controlled via `gap` on the parent container—not margins, padding, or line-height on the text itself.

```tsx
// ❌ DON'T - line-height hacks, margins, or padding on text
<Heading style={{ lineHeight: 1.3 }}>
<Heading mb="2">
<Heading pb="1">

// ✅ DO - use gap on parent Flex container
<Flex direction="column" gap="3">
  <Heading>Title</Heading>
  <Text>Content</Text>
</Flex>
```

### Spacing scale

Radix uses 1-9 scale:
- `gap="2"` - Tight (related items)
- `gap="3"` - Default
- `gap="4"` - Comfortable
- `gap="6"` - Section separation

### Avoid inline styles

Use Radix props instead of `style={{}}`. When unsure how to style something, look up the Radix docs at https://www.radix-ui.com/themes/docs

### State management (TanStack DB only)

Use TanStack DB for all state. For client-only UI state, use a local-only collection. Never use `useState`.

## Available Themes

| ID | Name | Fonts | Vibe |
|----|------|-------|------|
| inter | Inter | Inter | Clean & modern |
| source | Source Serif | Source Serif 4 + Source Sans 3 | Elegant editorial |
| alegreya | Alegreya | Alegreya + Alegreya Sans | Literary & warm |
| playfair | Playfair + Lato | Playfair Display + Lato | Classic craft |
| fraunces | Fraunces + Figtree | Fraunces + Figtree | Modern wonky |

Dozens more font pairings available. See https://github.com/KyleAMathews/vite-plugin-capsize-radix-ui/blob/main/SKILL.md for the full list.

## Adding Routes

Create new routes in `src/routes/`:

```tsx
// src/routes/about.tsx
import { createFileRoute } from '@tanstack/react-router'
import { Container, Flex, Heading, Text } from '@radix-ui/themes'

export const Route = createFileRoute('/about')({
  component: AboutPage,
})

function AboutPage() {
  return (
    <Container size="2" py="6">
      <Flex direction="column" gap="4">
        <Heading size="8">About</Heading>
        <Text>Your content here.</Text>
      </Flex>
    </Container>
  )
}
```

## Included Skills

Skills ship inside the library packages via `@tanstack/intent`. To list all available skills:

```bash
npx @tanstack/intent@latest list
```

<!-- intent-skills:start -->
# Skill mappings — when working in these areas, load the linked skill file into context.

### TanStack DB (`@tanstack/db`, `@tanstack/react-db`)

- **Setting up collections or adding a new data source** → `node_modules/@tanstack/db/skills/db-core/collection-setup/SKILL.md`
- **Writing live queries, filtering, joining, or aggregating data** → `node_modules/@tanstack/db/skills/db-core/live-queries/SKILL.md`
- **Mutations, optimistic updates, or server sync** → `node_modules/@tanstack/db/skills/db-core/mutations-optimistic/SKILL.md`
- **Building a custom collection adapter** → `node_modules/@tanstack/db/skills/db-core/custom-adapter/SKILL.md`
- **TanStack DB overview or general questions** → `node_modules/@tanstack/db/skills/db-core/SKILL.md`
- **Integrating DB with TanStack Start or other meta-frameworks** → `node_modules/@tanstack/db/skills/meta-framework/SKILL.md`
- **Using TanStack DB in React (useLiveQuery, hooks)** → `node_modules/@tanstack/react-db/skills/react-db/SKILL.md`
- **Offline support and transaction persistence** → `node_modules/@tanstack/offline-transactions/skills/offline/SKILL.md`

### Electric (`@electric-sql/client`)

- **Adding a new synced feature end-to-end** → `node_modules/@electric-sql/client/skills/electric-new-feature/SKILL.md`
- **Configuring shapes, ShapeStream, or sync options** → `node_modules/@electric-sql/client/skills/electric-shapes/SKILL.md`
- **Designing Postgres schema and shape definitions** → `node_modules/@electric-sql/client/skills/electric-schema-shapes/SKILL.md`
- **Using Electric with Drizzle or Prisma** → `node_modules/@electric-sql/client/skills/electric-orm/SKILL.md`
- **Debugging sync issues** → `node_modules/@electric-sql/client/skills/electric-debugging/SKILL.md`
- **Postgres security for Electric** → `node_modules/@electric-sql/client/skills/electric-postgres-security/SKILL.md`
- **Setting up auth proxy** → `node_modules/@electric-sql/client/skills/electric-proxy-auth/SKILL.md`
- **Deploying Electric** → `node_modules/@electric-sql/client/skills/electric-deployment/SKILL.md`

### Durable Streams (`@durable-streams/client`, `@durable-streams/state`)

- **Getting started with Durable Streams** → `node_modules/@durable-streams/client/skills/getting-started/SKILL.md`
- **Reading from streams (stream(), LiveMode, cursors)** → `node_modules/@durable-streams/client/skills/reading-streams/SKILL.md`
- **Writing data (append, IdempotentProducer)** → `node_modules/@durable-streams/client/skills/writing-data/SKILL.md`
- **Server deployment (dev server, Caddy)** → `node_modules/@durable-streams/client/skills/server-deployment/SKILL.md`
- **Production readiness checklist** → `node_modules/@durable-streams/client/skills/go-to-production/SKILL.md`
- **Defining state schemas** → `node_modules/@durable-streams/state/skills/state-schema/SKILL.md`
- **Stream-backed reactive database (createStreamDB)** → `node_modules/@durable-streams/state/skills/stream-db/SKILL.md`
<!-- intent-skills:end -->

## Skills

A skill is a set of local instructions in a `SKILL.md` file.

### Available skills

- `frontend-design` - Create distinctive, production-grade frontend interfaces with high design quality. (file: skills/frontend-design/SKILL.md)
