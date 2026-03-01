# Fantasy Baseball MVP - Monorepo

Your personal scouting + strategy assistant for dominating ESPN/Fantrax/Sleeper leagues through data-driven decisions, sentiment tracking, and prospect targeting.

## 🎯 Core Mission

"Be the GM who knows more than the league by understanding player value, market sentiment, and future potential before anyone else does."

---

## 📁 Monorepo Structure

```
fantasy-baseball/
├── backend/          # FastAPI Python backend
│   ├── app/
│   │   ├── api/     # API endpoints
│   │   ├── models/  # Database models
│   │   ├── services/# External integrations
│   │   └── db/      # Database config
│   ├── venv/        # Python virtual environment
│   ├── requirements.txt
│   └── start.sh
├── frontend/         # Web dashboard
│   └── index.html   # Static HTML/CSS/JS
├── docs/            # Project documentation
├── package.json      # Root package file
└── README.md
```

---

## 🚀 Getting Started

### Backend (FastAPI)

```bash
# Install dependencies
cd backend
./venv/bin/pip install -r requirements.txt

# Start backend server
./start.sh

# Backend runs on: http://localhost:8000
# API docs: http://localhost:8000/docs
```

### Frontend (Static Dashboard)

```bash
# Serve frontend
cd frontend
python3 -m http.server 8001

# Frontend runs on: http://localhost:8001
```

### Both Together (Root Scripts)

```bash
# Install npm dependencies
npm install

# Run both backend and frontend
npm run dev

# Or run separately
npm run dev:backend
npm run dev:frontend
```

---

## 📖 API Endpoints

### Health
- `GET /api/health` - Health check

### Players
- `GET /api/players/` - List all players (with filtering)
- `GET /api/players/{id}` - Get player by ID
- `GET /api/players/espn/{espn_id}` - Get player by ESPN ID
- `POST /api/players/` - Create new player
- `PUT /api/players/{id}` - Update player
- `DELETE /api/players/{id}` - Delete player
- `GET /api/players/stats/leaders` - Get stat leaders

### Teams
- `GET /api/teams/` - List all teams
- `GET /api/teams/{id}` - Get team by ID
- `POST /api/teams/` - Create new team
- `PUT /api/teams/{id}` - Update team

---

## 🏗️ Tech Stack

**Backend:**
- FastAPI (async web framework)
- SQLAlchemy (ORM)
- SQLite (database)
- espn-api (ESPN Fantasy Baseball)

**Frontend:**
- HTML/CSS/JavaScript (static)
- Python http.server (dev server)

---

## 🎯 Project Vision & Roadmap

### What This Tool Is (Not Just a Dashboard)

#### 1. Roster Optimization & Strategy Analysis
- Analyze team construction vs. league averages
- Suggest optimal lineup configurations based on matchups (vs. LHP/RHP, park factors)
- Identify category weaknesses
- Salary cap modeling for auction leagues

#### 2. Sentiment & Hype Tracking
- Track social media buzz (Twitter/X, Reddit, fantasy forums)
- Monitor ADP movement (offseason vs. pre-draft)
- Identify sleepers and busts
- Detect buy low / sell high signals

#### 3. Target Identification
- ADP targets (players to draft before value rises)
- Waiver wire gems (free agents before others notice)
- Trade targets (undervalued players)
- Pop-up prospects (minor league standouts)

#### 4. Draft Strategy
- Snake draft optimization
- Auction league bid modeling
- Points vs. Roto league adjustments
- Positional scarcity analysis

#### 5. Prospect Focus (Secret Weapon)
- Deep prospects (top 400 + sleepers)
- Breakout candidates
- Future value projections (3-5 year WAR)
- Minor league deep dive

### Phase Roadmap

**Phase 1: MVP - "The Scout's Dashboard"** ✅ IN PROGRESS
- SQLite database schema + import scripts
- Data ingestion pipeline (FG, MLB API)
- Player search and comparison interface
- Hype trend visualization
- Player card template
- Draft board generator

**Phase 2: Research Expansion**
- Twitter/X API integration
- Sentiment analysis engine
- Prospect buzz tracker
- Waiver wire aggregator
- Trade value recalculations

**Phase 3: Strategy Tools**
- Auction draft optimizer
- Trade simulator
- League config sharer
- Waiver priority ranker
- Team context integration

---

## 📈 Data Strategy

### Primary Database Tables
- `players` - Master player list (MLB + top prospects)
- `prospects` - Deep minor league stats + tools
- `player_stats` - Yearly MLB stats, minors, projections
- `hype_metrics` - Twitter mentions, Reddit discussion, ADP movement
- `market_sentiment` - Aggregate buzz score (Positive/Neutral/Negative)

### Data Sources
- **MLB API:** Player stats, biographical data, transaction history
- **FanGraphs:** Advanced metrics, Future Value, prospect rankings
- **ESPN API:** Fantasy league integration
- **Twitter/X Scraping:** Industry expert tracking (planned)
- **Baseball Reference:** Historical stats

---

## 🔧 Development

### Backend Tests

```bash
cd backend
./venv/bin/pytest
```

### Frontend

Open `frontend/index.html` in a browser - no build step required.

---

## 📦 Deployment

### Backend

```bash
cd backend
./venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### Frontend

Serve `frontend/` directory with any web server (nginx, apache, etc.).

---

## 📝 Project Status

See [Linear Project](https://linear.app/toastcorp/project/Fantasy-Baseball-Research-MVP) for detailed progress.

---

## 📄 License

MIT
