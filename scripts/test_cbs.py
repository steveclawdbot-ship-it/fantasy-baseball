#!/usr/bin/env python3
"""Test script for CBS Fantasy Baseball API

CBS Sports API Notes:
- Official API available but requires partnership
- Public endpoints very limited
- OAuth 2.0 authentication for official API
- Developer portal: https://developer.cbssports.com/
"""

import requests
import json
import time
from typing import Dict, List, Optional

# CBS has very limited public API
# Official API requires developer partnership
BASE_URL = "https://api.cbssports.com"
PUBLIC_URL = "https://www.cbssports.com/fantasy"

class CBSAPI:
    """Simple wrapper for CBS Fantasy API."""
    
    def __init__(self, api_key: Optional[str] = None, access_token: Optional[str] = None):
        self.api_key = api_key
        self.access_token = access_token
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "FantasyBaseballResearch/1.0",
            "Accept": "application/json"
        })
        
        if access_token:
            self.session.headers["Authorization"] = f"Bearer {access_token}"
    
    def _get(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make GET request with rate limiting."""
        url = f"{BASE_URL}{endpoint}"
        
        if params is None:
            params = {}
        
        if self.api_key:
            params["api_key"] = self.api_key
        
        try:
            time.sleep(0.5)
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"   ❌ API Error: {e}")
            return None
    
    def get_public_data(self) -> Optional[Dict]:
        """Try to get public data (very limited)."""
        # CBS doesn't really have public fantasy endpoints
        # Most data is behind auth wall
        print("   CBS has no public fantasy API endpoints")
        return None
    
    def test_auth_endpoint(self) -> Optional[Dict]:
        """Test authenticated endpoint."""
        if not self.access_token:
            return None
        
        # Would need actual endpoint from CBS
        # This is a placeholder
        return self._get("/fantasy/leagues")


def test_public_api():
    """Test CBS public API availability."""
    print("\n" + "="*60)
    print("CBS Sports API - Public Endpoints Test")
    print("="*60)
    
    api = CBSAPI()
    
    print("\n⚠️  CBS Fantasy API requires developer partnership")
    print("   No public endpoints available for testing")
    print("   Official API is behind OAuth 2.0")
    
    print("\n1. Checking CBS Fantasy website accessibility")
    try:
        response = requests.get(PUBLIC_URL, timeout=10)
        if response.status_code == 200:
            print(f"   ✓ CBS Fantasy website accessible")
            return True
        else:
            print(f"   ⚠️  Website returned {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def test_authenticated_api():
    """Test CBS with authentication."""
    print("\n" + "="*60)
    print("CBS Sports API - Authenticated Endpoints Test")
    print("="*60)
    
    import os
    api_key = os.getenv("CBS_API_KEY")
    access_token = os.getenv("CBS_ACCESS_TOKEN")
    
    if not api_key or not access_token:
        print("\n⚠️  Skipping authenticated test (no credentials)")
        print("   Set CBS_API_KEY and CBS_ACCESS_TOKEN env vars")
        print("   Requires CBS developer partnership")
        return None
    
    api = CBSAPI(api_key, access_token)
    
    print("\n1. Testing authenticated endpoint")
    result = api.test_auth_endpoint()
    
    if result:
        print(f"   ✓ API access successful")
        return True
    else:
        print(f"   ❌ API access failed")
        return False


def document_api_details():
    """Print API documentation details."""
    print("\n" + "="*60)
    print("CBS Sports API Documentation")
    print("="*60)
    
    details = {
        "authentication": {
            "type": "OAuth 2.0",
            "developer_portal": "https://developer.cbssports.com/",
            "partnership_required": "Yes - must apply for access",
            "public_access": "Very limited - no public fantasy endpoints"
        },
        "api_type": "Official but restricted",
        "documentation": "https://developer.cbssports.com/docs/",
        "base_url": "https://api.cbssports.com",
        "data_availability": {
            "leagues": "✓ With approved API key",
            "rosters": "✓ With approved API key",
            "players": "✓ With approved API key",
            "scores": "✓ With approved API key",
            "news": "✓ With approved API key",
            "public": "❌ No public fantasy endpoints"
        },
        "rate_limits": {
            "documented": "Depends on partnership tier",
            "typical": "1000-10000 requests/day",
            "note": "Higher tiers available for commercial use"
        },
        "pros": [
            "Official API with support",
            "Reliable and documented",
            "Comprehensive data coverage",
            "Good for commercial projects"
        ],
        "cons": [
            "Requires developer partnership",
            "Application process required",
            "Not suitable for personal projects",
            "Rate limits for free tier",
            "No public testing endpoints"
        ],
        "mvp_suitability": "LOW - Great API but requires partnership"
    }
    
    print(json.dumps(details, indent=2))


if __name__ == "__main__":
    print("\n🔍 CBS Sports Fantasy API Test Suite")
    
    # Run tests
    public_result = test_public_api()
    auth_result = test_authenticated_api()
    document_api_details()
    
    # Summary
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    print(f"Public API: {'✅ ACCESSIBLE' if public_result else '❌ FAIL'}")
    print(f"Authenticated: {'✅ PASS' if auth_result else '⏭️  SKIP' if auth_result is None else '❌ FAIL'}")
    
    print("\n⚠️  CBS requires developer partnership for API access")
    print("   Not suitable for MVP without commercial agreement")
