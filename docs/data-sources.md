# Fantasy Baseball Data Sources

Comprehensive analysis of fantasy baseball data APIs for ClawdCorp Fantasy Baseball project.

## Executive Summary

| Platform | Auth Required | API Quality | MLB Support | MVP Suitability |
|----------|---------------|-------------|-------------|-----------------|
| **ESPN** | Public: No<br>Private: Cookies | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | **HIGH** |
| **Sleeper** | None | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | **MEDIUM** |
| **Fantrax** | Session Cookie | ⭐⭐⭐ | ⭐⭐⭐⭐ | **LOW-MEDIUM** |
| **CBS** | OAuth + Partnership | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | **LOW** |

---

## 1. ESPN Fantasy Baseball

### Overview
Most popular fantasy baseball platform with unofficial but well-maintained Python API.

### Authentication
- **Public Leagues**: None required - just league ID
- **Private Leagues**: `espn_s2` and `swid` cookies from browser

**Getting Private League Credentials:**
1. Login to ESPN Fantasy
2. Open browser dev tools (F12)
3. Go to Application/Storage → Cookies
4. Copy `espn_s2` and `swid` values

### API Library
```bash
pip install espn-api
```

### Rate Limits
- **Documented**: No official limits published
- **Recommended**: 1 request per second maximum
- **Warning**: ESPN may block aggressive scraping

### Data Availability
| Data Type | Availability | Notes |
|-----------|--------------|-------|
| Teams | ✅ Full | Rosters, standings, records |
| Players | ✅ Full | Stats, projections, ownership % |
| Transactions | ✅ Yes | Recent activity, trades, waivers |
| History | ✅ Yes | Past seasons available |
| Real-time | ✅ Yes | Live scoring updates |

### Pros
- Most popular platform (largest user base)
- Well-maintained Python library (`espn-api`)
- Rich player stats and projections
- Real-time data updates
- Public leagues need no authentication

### Cons
- Private leagues require cookie extraction
- No official API documentation
- Rate limits not transparent
- Can break if ESPN changes site structure

### Recommendation
**PRIMARY DATA SOURCE FOR MVP**

ESPN is the best choice for the initial MVP due to:
- Largest user base (most potential users)
- Solid Python library support
- Public leagues work without auth complexity

---

## 2. Sleeper Fantasy

### Overview
Modern fantasy platform with official public API. NFL-first but growing MLB support.

### Authentication
**None required** - fully public API

### API Documentation
https://docs.sleeper.app/

### Base URL
```
https://api.sleeper.app/v1
```

### Rate Limits
- **Documented**: No explicit limits
- **Recommended**: 500ms between requests
- **Large Endpoints**: `/players` can be slow (bulk data)

### Data Availability
| Data Type | Availability | Notes |
|-----------|--------------|-------|
| Users | ✅ Public | User profiles, avatars |
| Leagues | ✅ Full | Settings, rosters, matchups |
| Players | ✅ Full | Complete player database |
| Trending | ✅ Yes | Add/drop trends |
| Transactions | ✅ Yes | League activity feed |
| Baseball | ⚠️ Partial | NFL-first, MLB features newer |

### Pros
- Fully public API (no auth needed)
- Official documentation
- Fast and reliable
- Good community support
- Free to use
- Modern API design

### Cons
- NFL-first platform
- Baseball features less mature
- Smaller MLB user base
- Some endpoints return very large datasets

### Recommendation
**SECONDARY SOURCE / FUTURE INTEGRATION**

Great API but smaller MLB user base makes it lower priority than ESPN.

---

## 3. Fantrax

### Overview
Popular for dynasty/keeper leagues with deep customization. No official API.

### Authentication
**Session Cookie Required**

**Getting Credentials:**
1. Login to Fantrax
2. Open browser dev tools
3. Copy session cookie
4. Use in API requests

### API Type
Private/Reverse-engineered API (unofficial)

### Base URL
```
https://www.fantrax.com/fxea
```

### Rate Limits
- **Documented**: No official limits
- **Recommended**: 1 request per second maximum
- **Warning**: Aggressive scraping may result in account ban

### Data Availability
| Data Type | Availability | Notes |
|-----------|--------------|-------|
| Leagues | ✅ With auth | Full league settings |
| Rosters | ✅ With auth | Complete roster data |
| Players | ✅ With auth | Stats, availability |
| Transactions | ✅ With auth | Activity feed |
| Standings | ✅ With auth | Current rankings |

### Pros
- Very popular for dynasty leagues
- Extremely customizable league settings
- Good for deep/keeper leagues
- Active developer community (unofficial)

### Cons
- No official API support
- Requires cookie extraction (brittle)
- Can break if site changes
- Authentication can be flaky
- More complex integration than other platforms

### Recommendation
**PHASE 2 / DYNASTY MODE**

Not recommended for MVP due to complexity, but valuable for dynasty league support later.

---

## 4. CBS Sports

### Overview
Major sports network with official API, but requires developer partnership.

### Authentication
**OAuth 2.0 + Developer Partnership**

**Getting Access:**
1. Apply at https://developer.cbssports.com/
2. Wait for partnership approval
3. Receive API credentials
4. Implement OAuth flow

### Developer Portal
https://developer.cbssports.com/

### API Type
Official but restricted

### Base URL
```
https://api.cbssports.com
```

### Rate Limits
- **Documented**: Depends on partnership tier
- **Typical**: 1,000-10,000 requests/day
- **Commercial**: Higher tiers available

### Data Availability
| Data Type | Availability | Notes |
|-----------|--------------|-------|
| Leagues | ✅ With API key | Requires partnership |
| Rosters | ✅ With API key | Requires partnership |
| Players | ✅ With API key | Requires partnership |
| Scores | ✅ With API key | Requires partnership |
| News | ✅ With API key | Requires partnership |
| Public | ❌ No | No public endpoints |

### Pros
- Official API with support
- Reliable and documented
- Comprehensive data coverage
- Good for commercial projects

### Cons
- Requires developer partnership application
- Not suitable for personal/MVP projects
- Rate limits on free tier
- No public testing endpoints
- Approval process can take weeks

### Recommendation
**NOT RECOMMENDED FOR MVP**

Great API but partnership requirement makes it unsuitable for initial development.

---

## Test Scripts

Test scripts for each platform are located in:
```
fantasy-baseball/scripts/
├── test_espn.py      # ESPN API tests
├── test_sleeper.py   # Sleeper API tests
├── test_fantrax.py   # Fantrax API tests
└── test_cbs.py       # CBS API tests
```

### Running Tests

```bash
cd fantasy-baseball/scripts

# ESPN (public league)
python test_espn.py

# ESPN (private league)
export ESPN_S2="your_cookie"
export ESPN_SWID="your_cookie"
python test_espn.py

# Sleeper
python test_sleeper.py

# Fantrax
export FANTRAX_SESSION="your_session_cookie"
python test_fantrax.py

# CBS
export CBS_API_KEY="your_key"
export CBS_ACCESS_TOKEN="your_token"
python test_cbs.py
```

---

## Implementation Recommendations

### Phase 1: MVP (Current)
**Primary: ESPN**
- Largest user base
- Public leagues work without auth
- Solid Python library

**Why not others:**
- Sleeper: Smaller MLB user base
- Fantrax: Complex auth (cookies)
- CBS: Requires partnership

### Phase 2: Multi-Platform Support
Add Sleeper and Fantrax support:
- Sleeper for modern API users
- Fantrax for dynasty leagues

### Phase 3: Full Coverage
If CBS partnership approved, add CBS support.

---

## Technical Architecture

### Proposed Service Structure
```
backend/app/services/
├── __init__.py
├── base_service.py       # Abstract base class
├── espn_service.py       # ESPN implementation ✅
├── sleeper_service.py    # Sleeper implementation
├── fantrax_service.py    # Fantrax implementation
└── cbs_service.py        # CBS implementation
```

### Base Service Interface
```python
class FantasyBaseballService(ABC):
    @abstractmethod
    def get_teams(self) -> List[Dict]: ...
    
    @abstractmethod
    def get_players(self) -> List[Dict]: ...
    
    @abstractmethod
    def get_player_stats(self, player_id: str) -> Dict: ...
```

---

## Rate Limiting Strategy

All services should implement rate limiting:

```python
import time
from functools import wraps

def rate_limit(seconds: float = 1.0):
    """Decorator to rate limit API calls."""
    def decorator(func):
        last_call = [0]
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_call[0]
            if elapsed < seconds:
                time.sleep(seconds - elapsed)
            result = func(*args, **kwargs)
            last_call[0] = time.time()
            return result
        return wrapper
    return decorator
```

---

## References

- ESPN API Library: https://github.com/cwendt94/espn-api
- Sleeper API Docs: https://docs.sleeper.app/
- CBS Developer Portal: https://developer.cbssports.com/
- CC-3: Research and test data sources for Fantasy Baseball

---

## Last Updated
2026-01-29

*Part of CC-3 deliverables*
