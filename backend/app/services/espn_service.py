import logging
from typing import List, Dict, Optional
from espn_api.baseball import League

logger = logging.getLogger(__name__)


class ESPNService:
    """Service for interacting with ESPN Fantasy Baseball API."""
    
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
    
    def _get_league(self) -> League:
        """Get or initialize league connection."""
        if self._league is None:
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
        return self._league
    
    def get_teams(self) -> List[Dict]:
        """Get all teams in league."""
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
    
    def get_players(self) -> List[Dict]:
        """Get all players from league rosters."""
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
    
    def get_free_agents(self) -> List[Dict]:
        """Get free agents (available players)."""
        league = self._get_league()
        free_agents = []
        
        for player in league.free_agents(size=1000):
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
    
    def get_player_stats(self, player_id: int) -> Optional[Dict]:
        """Get detailed stats for a specific player."""
        league = self._get_league()
        
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
    
    def get_league_settings(self) -> Dict:
        """Get league settings and configuration."""
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
        
        logger.info(f"Retrieved league settings: {settings}")
        return settings


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
