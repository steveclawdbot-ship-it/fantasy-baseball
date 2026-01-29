#!/usr/bin/env python3
"""Test script for ESPN Fantasy Baseball API

ESPN API Notes:
- Public leagues: No auth required, just league ID
- Private leagues: Need espn_s2 and swid cookies
- Rate limits: Not officially documented, but be reasonable (~1 req/sec)
- Library: espn-api (pip install espn-api)
"""

import os
import sys
import json
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from app.services.espn_service import ESPNService

# Test configuration
TEST_LEAGUE_ID = 12345  # Replace with actual public league ID for testing
TEST_YEAR = 2025

def test_public_league():
    """Test ESPN with public league (no auth)."""
    print("\n" + "="*60)
    print("ESPN API - Public League Test")
    print("="*60)
    
    try:
        service = ESPNService(
            league_id=TEST_LEAGUE_ID,
            year=TEST_YEAR
        )
        
        print("\n1. Testing get_league_settings()")
        settings = service.get_league_settings()
        print(f"   ✓ League: {settings.get('name', 'N/A')}")
        print(f"   ✓ Teams: {settings.get('num_teams', 'N/A')}")
        print(f"   ✓ Scoring: {settings.get('scoring_type', 'N/A')}")
        
        print("\n2. Testing get_teams()")
        teams = service.get_teams()
        print(f"   ✓ Found {len(teams)} teams")
        if teams:
            print(f"   ✓ Sample: {teams[0]['name']} (Owner: {teams[0]['owner']})")
        
        print("\n3. Testing get_players()")
        players = service.get_players()
        print(f"   ✓ Found {len(players)} players on rosters")
        if players:
            print(f"   ✓ Sample: {players[0]['name']} ({players[0]['position']})")
        
        print("\n4. Testing get_free_agents()")
        free_agents = service.get_free_agents()
        print(f"   ✓ Found {len(free_agents)} free agents")
        if free_agents:
            print(f"   ✓ Sample: {free_agents[0]['name']} ({free_agents[0]['ownership']}% owned)")
        
        print("\n✅ ESPN API test passed!")
        return True
        
    except Exception as e:
        print(f"\n❌ ESPN API test failed: {e}")
        return False


def test_private_league():
    """Test ESPN with private league (requires auth)."""
    print("\n" + "="*60)
    print("ESPN API - Private League Test")
    print("="*60)
    
    # Check for credentials
    espn_s2 = os.getenv("ESPN_S2")
    swid = os.getenv("ESPN_SWID")
    
    if not espn_s2 or not swid:
        print("\n⚠️  Skipping private league test (no credentials)")
        print("   Set ESPN_S2 and ESPN_SWID env vars to test private leagues")
        return None
    
    try:
        service = ESPNService(
            league_id=TEST_LEAGUE_ID,
            year=TEST_YEAR,
            espn_s2=espn_s2,
            swid=swid
        )
        
        settings = service.get_league_settings()
        print(f"\n✅ Private league access successful: {settings.get('name')}")
        return True
        
    except Exception as e:
        print(f"\n❌ Private league test failed: {e}")
        return False


def document_api_details():
    """Print API documentation details."""
    print("\n" + "="*60)
    print("ESPN API Documentation")
    print("="*60)
    
    details = {
        "authentication": {
            "public_leagues": "None - just league ID",
            "private_leagues": "espn_s2 and swid cookies from browser",
            "getting_cookies": "Login to ESPN, open dev tools, copy cookies"
        },
        "rate_limits": {
            "documented": "No official limits published",
            "recommended": "1 request per second max",
            "note": "ESPN may block aggressive scraping"
        },
        "data_availability": {
            "teams": "✓ Full roster data",
            "players": "✓ Stats, projections, ownership",
            "transactions": "✓ Recent activity",
            "history": "✓ Past seasons available"
        },
        "pros": [
            "Most popular fantasy platform",
            "Well-maintained Python library",
            "Rich player stats and projections",
            "Real-time data updates"
        ],
        "cons": [
            "Private leagues need cookie extraction",
            "No official API documentation",
            "Rate limits not transparent",
            "Can break if ESPN changes site"
        ],
        "mvp_suitability": "HIGH - Best overall option"
    }
    
    print(json.dumps(details, indent=2))


if __name__ == "__main__":
    print("\n🔍 ESPN Fantasy Baseball API Test Suite")
    
    # Run tests
    public_result = test_public_league()
    private_result = test_private_league()
    document_api_details()
    
    # Summary
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    print(f"Public League: {'✅ PASS' if public_result else '❌ FAIL'}")
    print(f"Private League: {'✅ PASS' if private_result else '⏭️  SKIP' if private_result is None else '❌ FAIL'}")
    
    sys.exit(0 if public_result else 1)
