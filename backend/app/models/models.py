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
