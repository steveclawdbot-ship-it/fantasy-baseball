#!/usr/bin/env python3
"""Test script for Fantrax Fantasy Baseball API

Fantrax API Notes:
- No official public API
- Uses private API endpoints (reverse engineered)
- Requires authentication via session cookies
- Rate limits: Not documented
- Community library: fantrax-api (unofficial)
"""

import requests
import json
import time
from typing import Dict, List, Optional

BASE_URL = "https://www.fantrax.com/fxea"

class FantraxAPI:
    """Simple wrapper for Fantrax private API."""
    
    def __init__(self, session_cookie: Optional[str] = None):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Origin": "https://www.fantrax.com",
            "Referer": "https://www.fantrax.com/"
        })
        
        if session_cookie:
            self.session.cookies.set("session", session_cookie)
    
    def _post(self, endpoint: str, data: Dict) -> Optional[Dict]:
        """Make POST request with rate limiting."""
        url = f"{BASE_URL}{endpoint}"
        try:
            time.sleep(1)  # Be conservative
            response = self.session.post(url, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"   ❌ API Error: {e}")
            if hasattr(e.response, 'text'):
                print(f"   Response: {e.response.text[:200]}")
            return None
    
    def get_league(self, league_id: str) -> Optional[Dict]:
        """Get league details."""
        data = {
            "leagueId": league_id,
            "method": "getLeagueInfo"
        }
        return self._post("/general", data)
    
    def get_standings(self, league_id: str) -> Optional[Dict]:
        """Get league standings."""
        data = {
            "leagueId": league_id,
            "method": "getStandings"
        }
        return self._post("/general", data)
    
    def get_rosters(self, league_id: str) -> Optional[Dict]:
        """Get team rosters."""
        data = {
            "leagueId": league_id,
            "method": "getTeamRosters"
        }
        return self._post("/general", data)
    
    def get_players(self, league_id: str, page: int = 1) -> Optional[Dict]:
        """Get available players."""
        data = {
            "leagueId": league_id,
            "method": "getPlayers",
            "params": {
                "page": page,
                "perPage": 50
            }
        }
        return self._post("/general", data)


def test_public_endpoints():
    """Test Fantrax public endpoints (limited)."""
    print("\n" + "="*60)
    print("Fantrax API - Public Endpoints Test")
    print("="*60)
    
    api = FantraxAPI()
    
    print("\n⚠️  Fantrax has no public API endpoints")
    print("   All endpoints require authentication")
    print("   This is a private API (reverse engineered)")
    
    return None


def test_authenticated_endpoints():
    """Test Fantrax with authentication."""
    print("\n" + "="*60)
    print("Fantrax API - Authenticated Endpoints Test")
    print("="*60)
    
    import os
    session_cookie = os.getenv("FANTRAX_SESSION")
    
    if not session_cookie:
        print("\n⚠️  Skipping authenticated test (no session cookie)")
        print("   Set FANTRAX_SESSION env var to test")
        print("   Cookie can be extracted from browser after login")
        return None
    
    api = FantraxAPI(session_cookie)
    league_id = "your_league_id"  # Replace with actual league ID
    
    print(f"\n1. Testing league access: {league_id}")
    league = api.get_league(league_id)
    
    if league:
        print(f"   ✓ League accessible")
        print(f"   Data: {json.dumps(league, indent=2)[:200]}...")
        return True
    else:
        print("   ❌ League access failed")
        return False


def document_api_details():
    """Print API documentation details."""
    print("\n" + "="*60)
    print("Fantrax API Documentation")
    print("="*60)
    
    details = {
        "authentication": {
            "type": "Session cookie required",
            "getting_cookie": "Login to Fantrax, copy session cookie from browser",
            "note": "No official API keys or OAuth"
        },
        "api_type": "Private/Reverse-engineered",
        "documentation": "None official - community maintained",
        "base_url": "https://www.fantrax.com/fxea",
        "data_availability": {
            "leagues": "✓ With auth",
            "rosters": "✓ With auth",
            "players": "✓ With auth",
            "stats": "✓ With auth",
            "transactions": "✓ With auth"
        },
        "rate_limits": {
            "documented": "No official limits",
            "recommended": "1 request per second max",
            "warning": "Aggressive scraping may result in ban"
        },
        "pros": [
            "Popular for dynasty leagues",
            "Very customizable league settings",
            "Good for deep leagues",
            "Active developer community (unofficial)"
        ],
        "cons": [
            "No official API support",
            "Requires cookie extraction",
            "Can break if site changes",
            "Authentication can be flaky",
            "More complex than other platforms"
        ],
        "mvp_suitability": "LOW-MEDIUM - Complex setup, but powerful for dynasty"
    }
    
    print(json.dumps(details, indent=2))


if __name__ == "__main__":
    print("\n🔍 Fantrax Fantasy API Test Suite")
    print("\n⚠️  WARNING: Fantrax uses a private API")
    print("   No official support - use at your own risk")
    
    # Run tests
    public_result = test_public_endpoints()
    auth_result = test_authenticated_endpoints()
    document_api_details()
    
    # Summary
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    print("Public API: ⏭️  SKIP (no public endpoints)")
    print(f"Authenticated: {'✅ PASS' if auth_result else '⏭️  SKIP' if auth_result is None else '❌ FAIL'}")
    
    print("\n⚠️  Fantrax requires session cookie extraction")
    print("   Not recommended for production use without official API")
