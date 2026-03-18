# SSA Activity Dashboard - External References

This document links to authoritative sources for SSA charter, ASQ definitions, and performance criteria. These are external documents maintained elsewhere - do not duplicate content here.

---

## Glossary & Definitions

### SSA/ASQ Terminology
**Location:** `~/.cursor/skills/internal-jargon/resources/GLOSSARY.txt`

Contains definitions for:
- ASQ (Approval/Support Request)
- SSA (Specialist Solutions Architect)
- UCO (Use Case Opportunity)
- Support types (Deep Dive, Technical Review, etc.)
- Specialization categories

---

## Charter Documents

### POC Staffing Model
**Location:** `~/.cursor/skills/fe-poc-doc/resources/POC_DOC_TEMPLATE.md`

Defines:
- POC scope and staffing guidelines
- ASQ → POC escalation criteria
- Success criteria templates
- Resource allocation rules

### KPIs & Performance Criteria
**Location:** `~/.claude/plugins/cache/experimental-plugin-marketplace/kudos-scanner/*/skills/nominate-awards/SKILL.md`

Defines:
- SSA performance metrics
- Award nomination criteria
- Quarterly review guidelines
- Impact measurement framework

---

## Salesforce Schema

### ApprovalRequest__c Fields
**Location:** `~/.claude/plugins/cache/fe-vibe/fe-ssagent/1.0.5/skills/ssagent-sfdc/SKILL.md`

Documents all Salesforce fields:
- Standard ASQ fields
- Custom fields (Notes, Effort, etc.)
- Status workflow
- Related object relationships

---

## Analytics Context

### Core Analytics Objectives
**Location:** `~/cowork/tickets/instructions.md`

Defines:
- Team reporting requirements
- Executive dashboard goals
- Metric priorities

### Data Source Guidance
**Location:** `~/cowork/tickets/CLAUDE.md`

Provides:
- Preferred data sources
- Table access patterns
- Query optimization tips

---

## Existing SQL Patterns

### Team ASQ Analysis
**Location:** `~/cowork/general/team_asq_analysis.sql`

Contains:
- Original view implementations
- `cjc_asq_with_ai_summary`
- `cjc_asq_person_metrics`
- `cjc_asq_missing_notes`
- `cjc_asq_gantt_data`
- `cjc_asq_capacity`

---

## Quick Reference Links

| Topic | Where to Look |
|-------|---------------|
| "What is an ASQ?" | GLOSSARY.txt |
| "What fields are on ASQ?" | ssagent-sfdc/SKILL.md |
| "What are SSA KPIs?" | nominate-awards/SKILL.md |
| "How to estimate effort?" | POC_DOC_TEMPLATE.md |
| "Which tables to use?" | tickets/CLAUDE.md |
