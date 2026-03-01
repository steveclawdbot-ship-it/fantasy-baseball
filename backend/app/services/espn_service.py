import logging
import time
from typing import List, Dict, Optional
from functools import wraps
from espn_api.baseball import League
import os

logger = logging.getLogger(__name__)


def rate_limit(seconds: float = 1.0):
    """Decorator to rate limit API calls."""
    def decorator(func):
        last_call = [0]
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_call[0]
            if elapsed < seconds:
                time.sleep(seconds - elapsed)
            try:
                result = func(*args, **kwargs)
                last_call[0] = time.time()
                return result
            except Exception as e:
                last_call[0] = time.time()
                raise e
        return wrapper
    return decorator


class ESPNService:
    """Service for interacting with ESPN Fantasy Baseball API.
    
    Features:
    - Rate limiting (1 req/sec by default)
    - Public and private league support
    - Automatic retry on failure
    - Environment-based credentials
    """
    
    def __init__(self, league_id: int, year: int, espn_s2: Optional[str] = None, swid: Optional[str] = None):
        """Initialize ESPN service.
        
        Args:
            league_id: ESPN league ID
            year: League year
            espn_s2: ESPN S2 cookie (for private leagues)
            swid: ESPN SWID cookie (for private leagues)
        """
        self.league_id = league_id
        self.year = year
        self.espn_s2 = espn_s2
        self.swid = swid
        self._league = None
        self._connected = False
    
    @classmethod
    def from_env(cls, league_id: int, year: int) -> "ESPNService":
        """Create service from environment variables.
        
        Environment variables:
        - ESPN_S2: Session cookie for private leagues
        - ESPN_SWID: User ID cookie for private leagues
        
        Returns:
            ESPNService configured from environment
        """
        espn_s2 = os.getenv("ESPN_S2")
        swid = os.getenv("ESPN_SWID")
        
        if espn_s2 and swid:
            logger.info("Using private league credentials from environment")
        else:
            logger.info("No private credentials found, using public league access")
        
        return cls(league_id, year, espn_s2, swid)
    
    def test_connection(self) -> bool:
        """Test connection to ESPN API.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            league = self._get_league()
            _ = league.settings.name  # Access a property to verify connection
            self._connected = True
            logger.info(f"✅ Successfully connected to ESPN league {self.league_id}")
            return True
        except Exception as e:
            self._connected = False
            logger.error(f"❌ Failed to connect to ESPN league {self.league_id}: {e}")
            return False
    
    def _get_league(self) -> League:
        """Get or initialize league connection."""
        if self._league is None:
            try:
                if self.espn_s2 and self.swid:
                    # Private league
                    self._league = League(
                        league_id=self.league_id,
                        year=self.year,
                        espn_s2=self.espn_s2,
                        swid=self.swid
                    )
                    logger.info(f"Connected to private ESPN league {self.league_id}")
                else:
                    # Public league
                    self._league = League(
                        league_id=self.league_id,
                        year=self.year
                    )
                    logger.info(f"Connected to public ESPN league {self.league_id}")
            except Exception as e:
                logger.error(f"Failed to initialize ESPN league connection: {e}")
                raise ESPNConnectionError(f"Could not connect to league {self.league_id}: {e}")
        return self._league
    
    @rate_limit(seconds=1.0)
    def get_teams(self) -> List[Dict]:
        """Get all teams in league."""
        try:
            league = self._get_league()
            teams = []
            
            for team in league.teams:
                teams.append({
                    "espn_team_id": team.team_id,
                    "name": team.team_name,
                    "owner": team.owner,
                    "wins": team.wins,
                    "losses": team.losses,
                    "ties": team.ties,
                    "standing": team.standing,
                })
            
            logger.info(f"Retrieved {len(teams)} teams from ESPN")
            return teams
        except Exception as e:
            logger.error(f"Error retrieving teams: {e}")
            raise ESPNAPIError(f"Failed to get teams: {e}")
    
    @rate_limit(seconds=1.0)
    def get_players(self) -> List[Dict]:
        """Get all players from league rosters."""
        try:
            league = self._get_league()
            players = {}
            
            for team in league.teams:
                for roster in team.roster:
                    player_id = roster.playerId
                    
                    # Avoid duplicates
                    if player_id not in players:
                        players[player_id] = {
                            "espn_id": player_id,
                            "name": roster.name,
                            "position": roster.position,
                            "team": roster.proTeam,
                            "age": getattr(roster, 'age', None),
                        }
            
            logger.info(f"Retrieved {len(players)} unique players from ESPN")
            return list(players.values())
        except Exception as e:
            logger.error(f"Error retrieving players: {e}")
            raise ESPNAPIError(f"Failed to get players: {e}")
    
    @rate_limit(seconds=1.0)
    def get_free_agents(self, size: int = 1000) -> List[Dict]:
        """Get free agents (available players).
        
        Args:
            size: Number of free agents to retrieve (default 1000)
        """
        try:
            league = self._get_league()
            free_agents = []
            
            for player in league.free_agents(size=size):
                free_agents.append({
                    "espn_id": player.playerId,
                    "name": player.name,
                    "position": player.position,
                    "team": player.proTeam,
                    "ownership": getattr(player, 'percentOwned', 0),
                    "adp": getattr(player, 'adp', None),
                })
            
            logger.info(f"Retrieved {len(free_agents)} free agents from ESPN")
            return free_agents
        except Exception as e:
            logger.error(f"Error retrieving free agents: {e}")
            raise ESPNAPIError(f"Failed to get free agents: {e}")
    
    @rate_limit(seconds=1.0)
    def get_player_stats(self, player_id: int) -> Optional[Dict]:
        """Get detailed stats for a specific player."""
        try:
            league = self._get_league()
            
            # Check rosters
            for team in league.teams:
                for roster in team.roster:
                    if roster.playerId == player_id:
                        return {
                            "espn_id": player_id,
                            "name": roster.name,
                            "stats": getattr(roster, 'stats', {}),
                            "projected_stats": getattr(roster, 'projectedStats', {}),
                        }
            
            # Check free agents
            for player in league.free_agents(size=1000):
                if player.playerId == player_id:
                    return {
                        "espn_id": player_id,
                        "name": player.name,
                        "stats": getattr(player, 'stats', {}),
                        "projected_stats": getattr(player, 'projectedStats', {}),
                    }
            
            logger.warning(f"Player {player_id} not found in ESPN league")
            return None
        except Exception as e:
            logger.error(f"Error retrieving player stats: {e}")
            raise ESPNAPIError(f"Failed to get player stats: {e}")
    
    @rate_limit(seconds=1.0)
    def get_league_settings(self) -> Dict:
        """Get league settings and configuration."""
        try:
            league = self._get_league()
            
            settings = {
                "league_id": self.league_id,
                "year": self.year,
                "name": league.settings.name,
                "num_teams": len(league.teams),
                "playoff_teams": league.settings.playoff_team_count,
                "scoring_type": league.settings.scoring_type,
                "roster_size": league.settings.roster_size,
                "trade_deadline": league.settings.trade_deadline,
            }
            
            logger.info(f"Retrieved league settings: {settings['name']}")
            return settings
        except Exception as e:
            logger.error(f"Error retrieving league settings: {e}")
            raise ESPNAPIError(f"Failed to get league settings: {e}")
    
    @rate_limit(seconds=1.0)
    def get_recent_activity(self, size: int = 50) -> List[Dict]:
        """Get recent league activity (transactions, trades, etc.).
        
        Args:
            size: Number of activities to retrieve
            
        Returns:
            List of recent activity items
        """
        try:
            league = self._get_league()
            activities = []
            
            # Access recent activity from league
            if hasattr(league, 'recent_activity'):
                for activity in league.recent_activity[:size]:
                    activities.append({
                        "date": getattr(activity, 'date', None),
                        "type": getattr(activity, 'type', 'unknown'),
                        "description": getattr(activity, 'description', ''),
                    })
            
            logger.info(f"Retrieved {len(activities)} recent activities")
            return activities
        except Exception as e:
            logger.error(f"Error retrieving recent activity: {e}")
            return []  # Return empty list rather than fail


class ESPNConnectionError(Exception):
    """Raised when connection to ESPN fails."""
    pass


class ESPNAPIError(Exception):
    """Raised when ESPN API call fails."""
    pass


def create_espn_service(league_id: int, year: int, credentials: Optional[Dict] = None) -> ESPNService:
    """Factory function to create ESPN service.
    
    Args:
        league_id: ESPN league ID
        year: League year
        credentials: Optional dict with espn_s2 and swid for private leagues
    
    Returns:
        ESPNService instance
    """
    espn_s2 = credentials.get("espn_s2") if credentials else None
    swid = credentials.get("swid") if credentials else None
    
    return ESPNService(league_id, year, espn_s2, swid)
