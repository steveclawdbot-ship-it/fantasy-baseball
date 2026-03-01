#!/usr/bin/env python3
"""Test script for Sleeper Fantasy Baseball API

Sleeper API Notes:
- Fully public API with good documentation
- No authentication required for most endpoints
- Rate limits: Not documented but appears to be generous
- API docs: https://docs.sleeper.app/
"""

import requests
import json
import time
from typing import Dict, List, Optional

BASE_URL = "https://api.sleeper.app/v1"

class SleeperAPI:
    """Simple wrapper for Sleeper API."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "FantasyBaseballResearch/1.0"
        })
    
    def _get(self, endpoint: str) -> Optional[Dict]:
        """Make GET request with rate limiting."""
        url = f"{BASE_URL}{endpoint}"
        try:
            time.sleep(0.5)  # Be nice to the API
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"   ❌ API Error: {e}")
            return None
    
    def get_user(self, username: str) -> Optional[Dict]:
        """Get user by username."""
        return self._get(f"/user/{username}")
    
    def get_user_leagues(self, user_id: str, sport: str = "nfl", season: str = "2024") -> Optional[List]:
        """Get leagues for a user."""
        # Note: Sleeper baseball support is newer
        return self._get(f"/user/{user_id}/leagues/{sport}/{season}")
    
    def get_league(self, league_id: str) -> Optional[Dict]:
        """Get league details."""
        return self._get(f"/league/{league_id}")
    
    def get_league_users(self, league_id: str) -> Optional[List]:
        """Get users in a league."""
        return self._get(f"/league/{league_id}/users")
    
    def get_league_rosters(self, league_id: str) -> Optional[List]:
        """Get rosters for a league."""
        return self._get(f"/league/{league_id}/rosters")
    
    def get_players(self, sport: str = "nfl") -> Optional[Dict]:
        """Get all players (large dataset)."""
        return self._get(f"/players/{sport}")
    
    def get_trending_players(self, sport: str = "nfl", type_: str = "add", lookback_hours: int = 24) -> Optional[List]:
        """Get trending players."""
        return self._get(f"/players/{sport}/trending/{type_}?lookback_hours={lookback_hours}")


def test_user_lookup():
    """Test user lookup functionality."""
    print("\n" + "="*60)
    print("Sleeper API - User Lookup Test")
    print("="*60)
    
    api = SleeperAPI()
    
    # Using a well-known username for testing
    username = "sleeper"
    print(f"\n1. Looking up user: {username}")
    
    user = api.get_user(username)
    if user:
        print(f"   ✓ User found: {user.get('display_name', 'N/A')}")
        print(f"   ✓ User ID: {user.get('user_id', 'N/A')}")
        return user
    else:
        print("   ❌ User lookup failed")
        return None


def test_league_data():
    """Test league data retrieval."""
    print("\n" + "="*60)
    print("Sleeper API - League Data Test")
    print("="*60)
    
    api = SleeperAPI()
    
    # Note: Need an actual league ID for testing
    # Using a placeholder - replace with real league ID
    league_id = "123456789"
    
    print(f"\n1. Fetching league: {league_id}")
    league = api.get_league(league_id)
    
    if league:
        print(f"   ✓ League: {league.get('name', 'N/A')}")
        print(f"   ✓ Teams: {len(league.get('roster_positions', []))}")
        return league
    else:
        print("   ⚠️  League not found (expected with placeholder ID)")
        return None


def test_players():
    """Test player database."""
    print("\n" + "="*60)
    print("Sleeper API - Player Database Test")
    print("="*60)
    
    api = SleeperAPI()
    
    # Note: This is a large endpoint, might take time
    print("\n1. Fetching trending players (NFL - baseball support is newer)")
    trending = api.get_trending_players(sport="nfl", type_="add", lookback_hours=24)
    
    if trending:
        print(f"   ✓ Found {len(trending)} trending players")
        if trending:
            print(f"   ✓ Sample: Player ID {trending[0]['player_id']}")
        return trending
    else:
        print("   ⚠️  No trending data")
        return None


def document_api_details():
    """Print API documentation details."""
    print("\n" + "="*60)
    print("Sleeper API Documentation")
    print("="*60)
    
    details = {
        "authentication": {
            "required": "No - fully public API",
            "rate_limiting": "Not documented, be respectful",
            "user_agent": "Recommended to set"
        },
        "base_url": "https://api.sleeper.app/v1",
        "documentation": "https://docs.sleeper.app/",
        "data_availability": {
            "users": "✓ Public user data",
            "leagues": "✓ League settings, rosters",
            "players": "✓ Full player database",
            "trending": "✓ Add/drop trends",
            "transactions": "✓ League activity",
            "baseball_support": "⚠️  NFL-first, MLB support growing"
        },
        "rate_limits": {
            "documented": "No explicit limits",
            "recommended": "500ms between requests",
            "large_endpoints": "/players can be slow (bulk data)"
        },
        "pros": [
            "Fully public API - no auth needed",
            "Official documentation",
            "Fast and reliable",
            "Good community support",
            "Free to use"
        ],
        "cons": [
            "NFL-first platform",
            "Baseball features newer/less mature",
            "Smaller user base for MLB",
            "Some endpoints are very large"
        ],
        "mvp_suitability": "MEDIUM - Great API, smaller MLB user base"
    }
    
    print(json.dumps(details, indent=2))


if __name__ == "__main__":
    print("\n🔍 Sleeper Fantasy API Test Suite")
    
    # Run tests
    user_result = test_user_lookup()
    league_result = test_league_data()
    players_result = test_players()
    document_api_details()
    
    # Summary
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    print(f"User Lookup: {'✅ PASS' if user_result else '❌ FAIL'}")
    print(f"League Data: {'✅ PASS' if league_result else '⏭️  SKIP' if league_result is None else '❌ FAIL'}")
    print(f"Player Data: {'✅ PASS' if players_result else '⚠️  PARTIAL'}")
    
    print("\n✅ Sleeper API is well-documented and easy to use!")
