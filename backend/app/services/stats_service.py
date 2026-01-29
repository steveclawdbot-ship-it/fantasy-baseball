import logging
from typing import Dict, List, Optional
from pybaseball import pitching_stats, batting_stats, statcast_pitcher, statcast_batter
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class BaseballStatsService:
    """Service for pulling baseball stats from pybaseball."""
    
    def __init__(self):
        """Initialize Baseball Stats service."""
        logger.info("Baseball Stats Service initialized")
    
    def get_player_season_stats(self, year: int, player_name: str = None) -> List[Dict]:
        """Get player stats for a specific season from Baseball Reference/FanGraphs.
        
        Args:
            year: Season year (e.g., 2024)
            player_name: Optional player name to filter
            
        Returns:
            List of player stat dictionaries
        """
        try:
            logger.info(f"Pulling batting stats for {year}...")
            batting_df = batting_stats(year, year)
            
            if player_name:
                batting_df = batting_df[batting_df['Name'].str.contains(player_name, case=False)]
            
            players = []
            for _, row in batting_df.head(1000).iterrows():  # Limit to 1000 for performance
                players.append({
                    "name": row.get('Name'),
                    "team": row.get('Tm'),
                    "age": row.get('Age'),
                    "avg": float(row.get('BA', 0)) if row.get('BA') not in ['--', None] else None,
                    "hr": int(row.get('HR', 0)) if row.get('HR') not in ['--', None] else None,
                    "rbi": int(row.get('RBI', 0)) if row.get('RBI') not in ['--', None] else None,
                    "sb": int(row.get('SB', 0)) if row.get('SB') not in ['--', None] else None,
                    "ops": float(row.get('OPS', 0)) if row.get('OPS') not in ['--', None] else None,
                    "war": float(row.get('WAR', 0)) if row.get('WAR') not in ['--', None] else None,
                    "wrc_plus": float(row.get('wRC+', 0)) if row.get('wRC+') not in ['--', None] else None,
                })
            
            logger.info(f"Retrieved {len(players)} players with batting stats")
            return players
            
        except Exception as e:
            logger.error(f"Failed to get player stats: {e}")
            return []
    
    def get_player_statcast(self, player_id: int, start_dt: str = None, end_dt: str = None) -> Dict:
        """Get Statcast data for a specific player.
        
        Args:
            player_id: MLBAM player ID
            start_dt: Start date (YYYY-MM-DD)
            end_dt: End date (YYYY-MM-DD)
            
        Returns:
            Statcast data dictionary
        """
        try:
            logger.info(f"Pulling statcast data for player ID {player_id}")
            
            if start_dt and end_dt:
                df = statcast_pitcher(start_dt, end_dt, player_id)
            else:
                # Default to yesterday
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                today = datetime.now().strftime('%Y-%m-%d')
                df = statcast_pitcher(yesterday, today, player_id)
            
            if df.empty:
                # Try batter data
                if start_dt and end_dt:
                    df = statcast_batter(start_dt, end_dt, player_id)
                else:
                    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                    today = datetime.now().strftime('%Y-%m-%d')
                    df = statcast_batter(yesterday, today, player_id)
            
            if df.empty:
                logger.warning(f"No statcast data found for player ID {player_id}")
                return {}
            
            # Get first row summary
            row = df.iloc[0]
            return {
                "player_id": player_id,
                "player_name": row.get('player_name'),
                "pitch_type": row.get('pitch_type'),
                "release_speed": float(row.get('release_speed', 0)),
                "spin_rate": float(row.get('release_spin_rate', 0)),
                "pfx_x": float(row.get('pfx_x', 0)),
                "pfx_z": float(row.get('pfx_z', 0)),
            }
            
        except Exception as e:
            logger.error(f"Failed to get statcast data: {e}")
            return {}
    
    def get_league_leaders(self, year: int, stat: str = 'HR', limit: int = 10) -> List[Dict]:
        """Get league leaders for a specific stat.
        
        Args:
            year: Season year
            stat: Stat to rank by (HR, RBI, SB, WAR, wRC+, OPS, etc.)
            limit: Number of players to return
            
        Returns:
            List of player dictionaries with leaderboard data
        """
        try:
            logger.info(f"Getting {stat} leaders for {year}...")
            batting_df = batting_stats(year, year)
            
            # Map stat names to column names
            stat_map = {
                'HR': 'HR',
                'RBI': 'RBI',
                'SB': 'SB',
                'WAR': 'WAR',
                'wRC+': 'wRC+',
                'OPS': 'OPS',
                'BA': 'BA',
                'OBP': 'OBP',
                'SLG': 'SLG',
            }
            
            col = stat_map.get(stat, 'HR')
            
            # Filter out missing values and sort
            df = batting_df[batting_df[col] != '--'].copy()
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.nlargest(limit, col)
            
            leaders = []
            for _, row in df.iterrows():
                leaders.append({
                    "name": row.get('Name'),
                    "team": row.get('Tm'),
                    "stat": stat,
                    "value": float(row.get(col, 0)) if col in ['BA', 'OPS', 'OBP', 'SLG', 'WAR', 'wRC+'] else int(row.get(col, 0)),
                })
            
            logger.info(f"Retrieved {len(leaders)} {stat} leaders")
            return leaders
            
        except Exception as e:
            logger.error(f"Failed to get league leaders: {e}")
            return []


def create_stats_service() -> BaseballStatsService:
    """Factory function to create Baseball Stats service.
    
    Returns:
        BaseballStatsService instance
    """
    return BaseballStatsService()
