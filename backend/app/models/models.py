from sqlalchemy import Column, Integer, String, Float, Text, DateTime, JSON, ForeignKey
from sqlalchemy.sql import func
from app.db.database import Base


class Player(Base):
    """Player model."""
    __tablename__ = "players"

    id = Column(Integer, primary_key=True, index=True)
    espn_id = Column(Integer, unique=True, index=True, nullable=False)
    name = Column(String, nullable=False)
    position = Column(String)
    team = Column(String)
    age = Column(Integer)
    
    # Stats
    avg = Column(Float)
    hr = Column(Integer)
    rbi = Column(Integer)
    sb = Column(Integer)
    ops = Column(Float)
    
    # Projections
    projected_avg = Column(Float)
    projected_hr = Column(Integer)
    projected_rbi = Column(Integer)
    projected_sb = Column(Integer)
    projected_ops = Column(Float)
    
    # Metadata
    adp = Column(Float)
    adp_trend = Column(String)  # "up", "down", "stable"
    ownership = Column(Float)  # Percentage owned
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
    
    def to_dict(self):
        """Convert to dictionary."""
        return {
            "id": self.id,
            "espn_id": self.espn_id,
            "name": self.name,
            "position": self.position,
            "team": self.team,
            "age": self.age,
            "avg": self.avg,
            "hr": self.hr,
            "rbi": self.rbi,
            "sb": self.sb,
            "ops": self.ops,
            "projected_avg": self.projected_avg,
            "projected_hr": self.projected_hr,
            "projected_rbi": self.projected_rbi,
            "projected_sb": self.projected_sb,
            "projected_ops": self.projected_ops,
            "adp": self.adp,
            "adp_trend": self.adp_trend,
            "ownership": self.ownership,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class PlayerCard(Base):
    """PlayerCard model for scouting notes."""
    __tablename__ = "player_cards"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    notes = Column(Text)
    tags = Column(JSON)  # ["power hitter", "speed", "prospect"]
    rating = Column(Integer)  # 1-10 scale
    scout_name = Column(String)
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())


class Team(Base):
    """Team model for fantasy leagues."""
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True, index=True)
    espn_league_id = Column(Integer, index=True, nullable=False)
    espn_team_id = Column(Integer, index=True, nullable=False)
    name = Column(String, nullable=False)
    owner = Column(String)
    
    # League config
    league_name = Column(String)
    league_type = Column(String)  # "dynasty", "redraft", "best_ball"
    num_teams = Column(Integer)
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())


class Scout(Base):
    """Scout model for user assessments of players."""
    __tablename__ = "scouts"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False, index=True)
    scout_name = Column(String, nullable=False)  # Who made the assessment
    
    # Assessment
    overall_rating = Column(Integer)  # 1-10
    hit_rating = Column(Integer)  # 1-10
    power_rating = Column(Integer)  # 1-10
    speed_rating = Column(Integer)  # 1-10
    fielding_rating = Column(Integer)  # 1-10
    
    # Notes
    notes = Column(Text)
    summary = Column(String(500))  # Brief summary
    
    # Tags/labels
    tags = Column(JSON)  # ["breakout candidate", "sell high", "buy low"]
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())


class ADPData(Base):
    """ADP (Average Draft Position) tracking over time."""
    __tablename__ = "adp_data"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False, index=True)
    
    # ADP info
    adp = Column(Float, nullable=False)
    min_pick = Column(Integer)
    max_pick = Column(Integer)
    
    # Source and date
    source = Column(String)  # "nfbc", "fantrax", "custom"
    league_type = Column(String)  # "mixed", "only", " dynasty"
    date_recorded = Column(DateTime, nullable=False)
    
    # Trend tracking
    adp_change_7d = Column(Float)  # Change from 7 days ago
    adp_change_30d = Column(Float)  # Change from 30 days ago
    
    created_at = Column(DateTime, server_default=func.now())


class Prospect(Base):
    """Prospect tracking and rankings."""
    __tablename__ = "prospects"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False, index=True, unique=True)
    
    # Prospect rankings
    overall_rank = Column(Integer)
    position_rank = Column(Integer)
    
    # Future Value (scouting scale)
    hit_future_value = Column(Integer)  # 20-80 scale
    power_future_value = Column(Integer)
    speed_future_value = Column(Integer)
    field_future_value = Column(Integer)
    overall_future_value = Column(Integer)
    
    # ETA and status
    eta = Column(String)  # "2025", "2026", "2027+"
    risk_level = Column(String)  # "low", "medium", "high"
    
    # Source
    ranking_source = Column(String)  # "fangraphs", "baseballamerica", etc.
    ranking_date = Column(DateTime)
    
    # Hype/notes
    hype_score = Column(Float)  # 0-100
    notes = Column(Text)
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())


class TradeValue(Base):
    """Player trade valuation over time."""
    __tablename__ = "trade_values"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False, index=True)
    
    # Valuation
    trade_value = Column(Integer)  # Points on trade value chart
    value_tier = Column(String)  # "elite", "strong", "solid", "fringe"
    
    # Context
    league_type = Column(String)  # "dynasty", "redraft"
    format = Column(String)  # "5x5", "points", etc.
    
    # Change tracking
    value_change_7d = Column(Integer)
    value_change_30d = Column(Integer)
    
    # Metadata
    valuation_date = Column(DateTime, nullable=False)
    source = Column(String)  # "calculator", "custom", etc.
    
    created_at = Column(DateTime, server_default=func.now())


class PlayerOffenseAdvanced(Base):
    """Advanced offensive metrics from Fangraphs-style sources."""
    __tablename__ = "player_offense_advanced"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False, index=True)
    season = Column(Integer, nullable=False, index=True)

    # Advanced offensive metrics
    wrc_plus = Column(Float)  # Weighted Runs Created+ (park/league adjusted)
    iso = Column(Float)  # Isolated Power (SLG - AVG)
    bb_pct = Column(Float)  # Walk percentage
    k_pct = Column(Float)  # Strikeout percentage
    obp = Column(Float)  # On-base percentage
    slg = Column(Float)  # Slugging percentage
    woba = Column(Float)  # Weighted On-Base Average
    xwoba = Column(Float)  # Expected wOBA (based on exit velo/launch angle)

    # Metadata
    extraction_timestamp = Column(DateTime, nullable=False)
    source = Column(String)  # "fangraphs", etc.

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    __table_args__ = (
        # Unique constraint: one row per player per season
        {'sqlite_autoincrement': True},
    )


class PlayerStatcast(Base):
    """Statcast quality-of-contact and athleticism metrics."""
    __tablename__ = "player_statcast"

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False, index=True)
    season = Column(Integer, nullable=False, index=True)

    # Quality of contact metrics
    barrel_pct = Column(Float)  # Percentage of batted balls that are "barrels"
    hard_hit_pct = Column(Float)  # Percentage of balls hit >= 95 mph
    avg_exit_velocity = Column(Float)  # Average exit velocity (mph)
    max_exit_velocity = Column(Float)  # Maximum exit velocity (mph)
    launch_angle = Column(Float)  # Average launch angle (degrees)
    sweet_spot_pct = Column(Float)  # Percentage of balls hit at 8-32 degree launch angle
    xslg = Column(Float)  # Expected slugging based on exit velo/launch angle

    # Athleticism metrics
    sprint_speed = Column(Float)  # ft/sec on competitive runs

    # Rolling windows (stored as JSON for flexibility)
    rolling_7d = Column(JSON)  # Recent 7-day rolling averages
    rolling_14d = Column(JSON)  # Recent 14-day rolling averages
    rolling_30d = Column(JSON)  # Recent 30-day rolling averages

    # Metadata
    extraction_timestamp = Column(DateTime, nullable=False)
    source = Column(String)  # "statcast", "baseball_savant", etc.

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    __table_args__ = (
        {'sqlite_autoincrement': True},
    )
