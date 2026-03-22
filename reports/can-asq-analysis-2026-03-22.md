# Canadian Region ASQ Analysis Report
**Generated:** 2026-03-22

## Executive Summary

| Metric | Value |
|--------|-------|
| ASQs Analyzed | 25 (Last 30 days) |
| Unassigned | 14 |
| With UCO Linkage | 25 (100%) |
| Production UCOs (U5/U6) | 16 |
| High Value (>$100K DBU) | 3 |
| Competitive Deals | 8 |

---

## Priority ASQ Table

| ASQ | Account | UCO Stage | Monthly DBU | Competitor | Importance | Current Owner | Recommended SSA |
|-----|---------|-----------|-------------|------------|------------|---------------|-----------------|
| AR-000114455 | Hydro Quebec | U6 (Live) | $350,000 | Other | **HIGH VALUE** + Production | Unassigned | **Mathieu Pelletier** (Geospatial expert) |
| AR-000114145 | Rogers Communications | U6 (Live) | $150,000 | Snowflake | **HIGH VALUE** + Snowflake Compete | Unassigned | **Aaron Binns** (Light workload) |
| AR-000114395 | CN Rail | U6 (Live) | $97,186 | - | Production UCO | Unassigned | **Elmer Cecilio** (Data Engineering) |
| AR-000114152 | Magna International | U3 (Scoping) | $25,000 | Talend | Active Pipeline | Yash Baheti ⚠️ | Reassign to **Aaron Binns** |
| AR-000114074 | Ontario Power Gen | U6 (Live) | $25,000 | - | Production UCO | Unassigned | **Ben Mackenzie** (Data Science) |
| AR-000113992 | Clio | U6 (Live) | $30,000 | - | Production UCO | Pedro Zanlorensi ⚠️ | Reassign to **Aaron Binns** |
| AR-000114137 | CIRO | U5 (Onboarding) | $10,000 | Microsoft Fabric | Production + MS Compete | Yash Baheti ⚠️ | Reassign to **Aaron Binns** |
| AR-000114329 | ENMAX | U2 (Qualifying) | $10,000 | Microsoft Fabric | MS Displacement | Unassigned | **Ben Mackenzie** (Data Science) |
| AR-000114080 | Cenovus Energy | U6 (Live) | $13,000 | Microsoft Fabric | Production + MS Compete | Unassigned | **Ben Mackenzie** (Data Science) |
| AR-000114471 | Fleetworthy | U6 (Live) | $10,000 | AWS EMR | Production UCO | Unassigned | **Ben Mackenzie** (Data Science) |

⚠️ = Owner currently overloaded (>50 effort days)

---

## Description Quality Issues

The following ASQs need improved request descriptions:

| ASQ | Account | Issue | Suggested Fix |
|-----|---------|-------|---------------|
| AR-000114498 | Baytex Energy | No title | Add descriptive title and context |
| AR-000114329 | ENMAX | No clear ask | Define specific SSA deliverable |
| AR-000114366 | Nova Scotia | No title, no ask | Complete request form properly |
| AR-000114074 | Ontario Power Gen | Vague "GenAi SSA" | Specify use case and expected outcome |
| AR-000114152 | Magna International | No context | Add migration scope and blockers |
| AR-000114137 | CIRO | Missing details | Describe Fabric competition context |
| AR-000113862 | 1Password | No title/ask | Complete with Snowflake compete strategy |

**Recommended Description Template:**
```
Cloud: [AWS/Azure/GCP]
Context: [Business situation and why SSA engagement is needed]
What has been done: [Previous SA/DSA work]
Ask: [Specific deliverable - e.g., architecture review, POC support, demo]
Expected Outcome: [What success looks like]
Timeline: [Urgency/deadline]
```

---

## SSA Capacity for Assignment

### Available SSAs (LIGHT Workload)

| SSA | Current ASQs | Effort Days | Specializations | Best For |
|-----|--------------|-------------|-----------------|----------|
| Aaron Binns | 7 | 0 | General | High-priority unassigned |
| Nasir Dakri | 1 | 0 | General | Overflow |
| Elmer Cecilio | 1 | 1 | Data Engineering | DE requests |
| Ben Mackenzie | 2 | 2 | Data Science, ML | AI/ML requests |
| Chad Lortie | 2 | 2 | Data Science | AI/ML overflow |
| Mathieu Pelletier | 6 | 16 | Geospatial, Analytics | Geospatial |
| Harsha Pasala | 5 | 13 | Data Engineering | DE overflow |
| Allan Cao | 8 | 13 | Data Governance | Governance requests |

### Overloaded SSAs (Recommend Reassignment)

| SSA | Current ASQs | Effort Days | Critical | Action Needed |
|-----|--------------|-------------|----------|---------------|
| Qi Su | 147 | 318 | 140 | Major reassignment needed |
| Kathleen Wong | 175 | 875 | 0 | Review stale ASQs |
| Fernando Vásquez | 18 | 248 | 18 | Reassign overdue |
| Yash Baheti | 14 | 70 | 0 | Reassign 2 ASQs |
| Pedro Zanlorensi | 22 | 107 | 1 | Reassign Clio ASQ |
| Carlos Eduardo Dip | 14 | 70 | 0 | Reassign Couche-Tard |

---

## Competitive Intelligence

| Competitor | UCOs in CAN | Total Monthly DBU | Key Accounts |
|------------|-------------|-------------------|--------------|
| Snowflake | 86 | $951,213 | Rogers, 1Password, Province of Ontario |
| Microsoft Fabric | 96 | $381,635 | ENMAX, CIRO, Cenovus |
| Azure Synapse | 64 | $606,333 | Alimentation Couche-Tard |
| AWS EMR | 8 | $106,048 | Fleetworthy, Wawanesa |

---

## Immediate Actions

1. **Assign Hydro Quebec (AR-000114455)** to Mathieu Pelletier - $350K/mo production account
2. **Assign Rogers (AR-000114145)** to Aaron Binns - $150K/mo Snowflake compete
3. **Reassign Magna (AR-000114152)** from Yash Baheti - owner overloaded
4. **Reassign Clio (AR-000113992)** from Pedro Zanlorensi - owner overloaded
5. **Fix descriptions** on 7 ASQs missing proper context/ask

---

*Report generated from ssa-ops ASQ queries against logfood workspace*
