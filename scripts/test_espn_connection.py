#!/usr/bin/env python3
"""Test connection to ESPN Fantasy Baseball API

Usage:
    # Test public league (no auth)
    python test_espn_connection.py --league-id 12345 --year 2025
    
    # Test private league (with auth)
    export ESPN_S2="your_cookie"
    export ESPN_SWID="your_cookie"
    python test_espn_connection.py --league-id 12345 --year 2025
"""

import argparse
import sys
import os
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from app.services.espn_service import ESPNService, create_espn_service


def test_connection(league_id: int, year: int) -> bool:
    """Test ESPN API connection."""
    print("\n" + "="*60)
    print("ESPN Fantasy Baseball API - Connection Test")
    print("="*60)
    
    # Check for credentials
    espn_s2 = os.getenv("ESPN_S2")
    swid = os.getenv("ESPN_SWID")
    
    if espn_s2 and swid:
        print("\n🔐 Private league credentials found in environment")
        service = create_espn_service(league_id, year, {
            "espn_s2": espn_s2,
            "swid": swid
        })
    else:
        print("\n🌐 No credentials found - testing public league")
        print("   (Set ESPN_S2 and ESPN_SWID env vars for private league)")
        service = create_espn_service(league_id, year)
    
    # Test connection
    print(f"\n1. Testing connection to league {league_id} ({year})...")
    if not service.test_connection():
        print("\n❌ Connection test failed!")
        return False
    
    print("\n✅ Connection successful!")
    return True


def test_data_retrieval(league_id: int, year: int) -> bool:
    """Test data retrieval endpoints."""
    print("\n" + "="*60)
    print("ESPN API - Data Retrieval Test")
    print("="*60)
    
    service = ESPNService.from_env(league_id, year)
    
    tests = []
    
    # Test 1: League Settings
    print("\n2. Testing get_league_settings()...")
    try:
        settings = service.get_league_settings()
        print(f"   ✅ League: {settings['name']}")
        print(f"   ✅ Teams: {settings['num_teams']}")
        print(f"   ✅ Scoring: {settings['scoring_type']}")
        tests.append(True)
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        tests.append(False)
    
    # Test 2: Teams
    print("\n3. Testing get_teams()...")
    try:
        teams = service.get_teams()
        print(f"   ✅ Retrieved {len(teams)} teams")
        if teams:
            print(f"   ✅ Sample: {teams[0]['name']}")
        tests.append(True)
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        tests.append(False)
    
    # Test 3: Players
    print("\n4. Testing get_players()...")
    try:
        players = service.get_players()
        print(f"   ✅ Retrieved {len(players)} players")
        if players:
            print(f"   ✅ Sample: {players[0]['name']}")
        tests.append(True)
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        tests.append(False)
    
    # Test 4: Free Agents
    print("\n5. Testing get_free_agents()...")
    try:
        free_agents = service.get_free_agents(size=50)
        print(f"   ✅ Retrieved {len(free_agents)} free agents")
        if free_agents:
            print(f"   ✅ Sample: {free_agents[0]['name']}")
        tests.append(True)
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        tests.append(False)
    
    return all(tests)


def main():
    parser = argparse.ArgumentParser(description="Test ESPN Fantasy Baseball API connection")
    parser.add_argument("--league-id", type=int, required=True, help="ESPN league ID")
    parser.add_argument("--year", type=int, default=2025, help="League year (default: 2025)")
    parser.add_argument("--quick", action="store_true", help="Quick connection test only")
    args = parser.parse_args()
    
    print("\n🔍 ESPN Fantasy Baseball API Connection Test")
    print(f"   League ID: {args.league_id}")
    print(f"   Year: {args.year}")
    
    # Test connection
    if not test_connection(args.league_id, args.year):
        print("\n❌ Connection test failed - stopping")
        sys.exit(1)
    
    if args.quick:
        print("\n✅ Quick test completed successfully!")
        sys.exit(0)
    
    # Test data retrieval
    if test_data_retrieval(args.league_id, args.year):
        print("\n" + "="*60)
        print("✅ All tests passed! ESPN API is ready to use.")
        print("="*60)
        sys.exit(0)
    else:
        print("\n" + "="*60)
        print("⚠️  Some tests failed. Check output above.")
        print("="*60)
        sys.exit(1)


if __name__ == "__main__":
    main()
