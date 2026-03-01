# Fantasy Baseball MVP Scope Definition

**Issue:** CC-41  
**Date:** 2026-02-03  
**Status:** Draft - Pending Jesse Approval

---

## Executive Summary

**Product:** Fantasy Baseball Research Assistant  
**Goal:** Personal scouting + strategy tool for dominating fantasy leagues  
**MVP Timeline:** 4-6 weeks  
**Success Metric:** Successfully used for 2026 fantasy draft

---

## MVP Definition

### What MVP IS
- A tool Jesse uses personally for fantasy baseball research
- Data aggregation from multiple sources
- Basic player comparison and analysis
- Draft preparation assistance

### What MVP IS NOT
- A commercial product (yet)
- Multi-user support
- Real-time league integration
- Advanced AI predictions

---

## Must-Have Features (MVP)

### 1. Data Foundation
| Feature | Description | Data Source |
|---------|-------------|-------------|
| Player Database | All MLB players + top 100 prospects | MLB API, FanGraphs |
| Stats Import | 2025 season stats + 3-year averages | FanGraphs, MLB API |
| Projections | 2026 fantasy projections | FanGraphs, ATC |
| Prospect Rankings | Top 100 prospects with FV grades | FanGraphs |

### 2. Core Views
| Feature | Description |
|---------|-------------|
| Player Search | Find any player by name, team, position |
| Player Card | Full profile: stats, projections, news |
| Comparison Tool | Side-by-side player comparison |
| Prospect List | Sortable/filterable prospect rankings |

### 3. Draft Tools
| Feature | Description |
|---------|-------------|
| Draft Board | Visual draft tracker with rankings |
| Positional Scarcity | Track position depth during draft |
| Target List | Mark players to target/avoid |

### 4. Data Pipeline
| Feature | Description |
|---------|-------------|
| Daily Updates | Automated stat refresh |
| News Aggregation | Player news from multiple sources |
| Injury Tracking | Monitor injury reports |

---

## Nice-to-Have (Post-MVP)

### Phase 2: Intelligence
- [ ] Twitter/X sentiment analysis
- [ ] ADP movement tracking
- [ ] Hype score algorithm
- [ ] Waiver wire recommendations

### Phase 3: Strategy
- [ ] Auction draft optimizer
- [ ] Trade simulator
- [ ] League-specific analysis
- [ ] Lineup optimizer

### Phase 4: Advanced
- [ ] Multi-league sync
- [ ] Custom scoring support
- [ ] API for external tools
- [ ] Mobile app

---

## MVP Timeline

### Week 1-2: Foundation
- [ ] Finalize database schema
- [ ] Build data ingestion pipeline
- [ ] Import player base data
- [ ] Set up automated updates

### Week 3-4: Core Features
- [ ] Player search API
- [ ] Player card UI
- [ ] Comparison tool
- [ ] Prospect views

### Week 5-6: Draft Tools
- [ ] Draft board UI
- [ ] Target tracking
- [ ] Positional scarcity view
- [ ] Testing & refinement

---

## Data Requirements from Jesse's League

### ESPN League Access
- [ ] League ID
- [ ] Team ID (Jesse's team)
- [ ] ESPN credentials (or read-only API key)

### League Settings
- [ ] Scoring type (Roto vs Points)
- [ ] Categories tracked
- [ ] Position requirements (C, 1B, 2B, etc.)
- [ ] Roster size
- [ ] IL spots
- [ ] Prospect eligibility rules

### Historical Data
- [ ] Past 3 years of league standings
- [ ] Jesse's draft history
- [ ] Notable trades/transactions

---

## Success Metrics

### MVP Success Criteria
1. **Functionality:** Can search/compare any MLB player
2. **Data Freshness:** Stats updated within 24 hours
3. **Draft Readiness:** Successfully used for 2026 draft
4. **Performance:** Page loads < 2 seconds

### Post-MVP Success
1. **Usage:** Daily use during season
2. **Value:** Identifies 3+ waiver gems per month
3. **Draft:** Top 3 finish in league

---

## Technical Decisions

### Database
- **Choice:** SQLite (MVP) → PostgreSQL (if scale)
- **Rationale:** Simple, portable, no server needed

### Backend
- **Choice:** FastAPI (existing)
- **Rationale:** Python ecosystem, async, good docs

### Frontend
- **Choice:** Static HTML/JS (MVP) → React (if needed)
- **Rationale:** Fast iteration, no build complexity

### Hosting
- **Choice:** Raspberry Pi (local) → Cloud (if external access needed)
- **Rationale:** Free, already set up, private

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Data source API changes | Medium | High | Cache data, multiple sources |
| Scope creep | High | Medium | Strict MVP definition |
| ESPN API limitations | Medium | High | Manual export fallback |
| Time constraints | Medium | High | Cut nice-to-have features |

---

## Open Questions

1. **Is this for personal use only, or potential product?**
   - Affects: Scope, polish level, multi-user features

2. **Which league platform? ESPN primary, others later?**
   - Affects: API integration priority

3. **How much manual data entry is acceptable?**
   - Affects: Automation effort vs manual fallback

4. **Target draft date?**
   - Affects: Timeline urgency

---

## Next Steps

1. [ ] Jesse reviews and approves this scope
2. [ ] Answer open questions
3. [ ] Create Linear sub-issues for MVP features
4. [ ] Begin Week 1-2 development

---

## Appendix: Feature Priority Matrix

| Feature | User Value | Effort | Priority |
|---------|------------|--------|----------|
| Player database | High | Medium | P0 |
| Player search | High | Low | P0 |
| Player card | High | Medium | P0 |
| Comparison tool | High | Medium | P0 |
| Draft board | High | High | P0 |
| Prospect rankings | Medium | Low | P1 |
| Daily updates | Medium | Medium | P1 |
| News aggregation | Medium | Medium | P1 |
| Sentiment analysis | Low | High | P2 |
| Auction optimizer | Low | High | P2 |

---

*Document created: 2026-02-03*  
*Author: Steve (AI Agent)*  
*Review: Pending Jesse approval*
