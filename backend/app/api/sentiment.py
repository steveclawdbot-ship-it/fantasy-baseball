from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.db.database import get_db
from collections import defaultdict
from pathlib import Path
import sqlite3

router = APIRouter()

POSITIVE_KEYWORDS = [
    "breakout", "sleeper", "must add", "buy", "target", "value", "rising", "hot", "league winner"
]
NEGATIVE_KEYWORDS = [
    "bust", "avoid", "fade", "injury", "concern", "drop", "panic", "cold", "sell"
]


def score_text(s: str) -> float:
    s = (s or "").lower()
    pos = sum(1 for k in POSITIVE_KEYWORDS if k in s)
    neg = sum(1 for k in NEGATIVE_KEYWORDS if k in s)
    if pos == 0 and neg == 0:
        return 0.0
    return max(-1.0, min(1.0, (pos - neg) / max(1, (pos + neg))))


def pick_snippet(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    low = t.lower()
    for k in POSITIVE_KEYWORDS + NEGATIVE_KEYWORDS:
        idx = low.find(k)
        if idx >= 0:
            start = max(0, idx - 90)
            end = min(len(t), idx + 160)
            return t[start:end].replace("\n", " ").strip()
    return t[:180].replace("\n", " ").strip()


@router.get("/overview", response_model=dict)
async def sentiment_overview(
    days: int = Query(7, ge=1, le=30),
    limit: int = Query(10, ge=3, le=25),
    db: AsyncSession = Depends(get_db),
):
    # top players by ADP priority (fallback to power stats)
    players_sql = text(
        """
        SELECT name
        FROM players
        ORDER BY
          CASE WHEN adp IS NULL THEN 1 ELSE 0 END,
          adp ASC,
          hr DESC
        LIMIT 300
        """
    )
    player_rows = (await db.execute(players_sql)).fetchall()
    tracked_players = [r[0] for r in player_rows if r[0]]

    videos_sql = text(
        """
        SELECT v.video_id, v.title, v.video_url, v.channel, v.published_at, s.show_name
        FROM youtube_videos v
        JOIN podcast_sources s ON s.id = v.source_id
        WHERE v.published_at IS NULL
           OR datetime(v.published_at) >= datetime('now', :window)
        ORDER BY COALESCE(v.published_at, v.extracted_at) DESC
        LIMIT 500
        """
    )
    window = f"-{days} days"

    # primary: current API db connection
    try:
        video_rows = (await db.execute(videos_sql, {"window": window})).fetchall()
    except Exception:
        # fallback: shared workspace db populated by crawler
        alt_db = Path("/home/jesse/clawd-steve/data/fantasy_baseball.db")
        if alt_db.exists():
            conn = sqlite3.connect(alt_db)
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT v.video_id, v.title, v.video_url, v.channel, v.published_at, s.show_name
                    FROM youtube_videos v
                    JOIN podcast_sources s ON s.id = v.source_id
                    WHERE v.published_at IS NULL
                       OR datetime(v.published_at) >= datetime('now', ?)
                    ORDER BY COALESCE(v.published_at, v.extracted_at) DESC
                    LIMIT 500
                    """,
                    (window,),
                )
                video_rows = cur.fetchall()
            finally:
                conn.close()
        else:
            video_rows = []

    # transcript-level context signal (vector-placeholder integration step)
    transcript_map = {}
    transcripts_sql = text(
        """
        SELECT video_id, transcript
        FROM youtube_transcripts
        WHERE status = 'ok'
        """
    )
    try:
        transcript_rows = (await db.execute(transcripts_sql)).fetchall()
        for vid, transcript in transcript_rows:
            transcript_map[vid] = {
                "score": score_text(transcript or ""),
                "snippet": pick_snippet(transcript or ""),
            }
    except Exception:
        alt_db = Path("/home/jesse/clawd-steve/data/fantasy_baseball.db")
        if alt_db.exists():
            conn = sqlite3.connect(alt_db)
            try:
                cur = conn.cursor()
                cur.execute("SELECT video_id, transcript FROM youtube_transcripts WHERE status='ok'")
                for vid, transcript in cur.fetchall():
                    transcript_map[vid] = {
                        "score": score_text(transcript or ""),
                        "snippet": pick_snippet(transcript or ""),
                    }
            finally:
                conn.close()

    player_stats = defaultdict(lambda: {
        "player": None,
        "mentions": 0,
        "sentiment_sum": 0.0,
        "sources": set(),
        "evidence": [],
        "transcript_hits": 0,
    })

    discussed_videos = []

    for row in video_rows:
        video_id, title, video_url, channel, published_at, show_name = row
        title_l = (title or "").lower()
        title_score = score_text(title or "")
        transcript_ctx = transcript_map.get(video_id, {"score": 0.0, "snippet": ""})
        s = (title_score * 0.65) + (transcript_ctx["score"] * 0.35)

        matched_any = False
        for p in tracked_players:
            p_l = p.lower()
            if p_l in title_l:
                matched_any = True
                stat = player_stats[p]
                stat["player"] = p
                stat["mentions"] += 1
                stat["sentiment_sum"] += s
                stat["sources"].add(show_name)
                if transcript_ctx.get("snippet"):
                    stat["transcript_hits"] += 1
                if len(stat["evidence"]) < 3:
                    stat["evidence"].append({
                        "title": title,
                        "url": video_url,
                        "source": show_name,
                        "channel": channel,
                        "published_at": published_at,
                        "sentiment": round(s, 3),
                        "transcript_snippet": transcript_ctx.get("snippet", ""),
                    })

        if matched_any:
            discussed_videos.append({
                "video_id": video_id,
                "title": title,
                "url": video_url,
                "source": show_name,
                "channel": channel,
                "published_at": published_at,
                "sentiment": round(s, 3),
            })

    players = []
    for _, stat in player_stats.items():
        mentions = stat["mentions"]
        if mentions == 0:
            continue
        avg_sentiment = stat["sentiment_sum"] / mentions
        transcript_bonus = min(0.25, stat["transcript_hits"] * 0.05)
        players.append({
            "player": stat["player"],
            "mentions": mentions,
            "sentiment": round(avg_sentiment, 3),
            "consensus_sources": len(stat["sources"]),
            "conviction": round(min(1.0, (mentions / 5) + transcript_bonus), 3),
            "transcript_hits": stat["transcript_hits"],
            "evidence": stat["evidence"],
        })

    players_sorted_pos = sorted(players, key=lambda x: (x["sentiment"], x["mentions"]), reverse=True)
    players_sorted_neg = sorted(players, key=lambda x: (x["sentiment"], -x["mentions"]))
    discussed = sorted(players, key=lambda x: x["mentions"], reverse=True)
    disagreement = sorted(players, key=lambda x: (x["consensus_sources"], -abs(x["sentiment"])) , reverse=True)

    return {
        "window_days": days,
        "source_count": len({r[5] for r in video_rows}),
        "video_count": len(video_rows),
        "widgets": {
            "sentiment_risers": players_sorted_pos[:limit],
            "sentiment_fallers": players_sorted_neg[:limit],
            "most_discussed": discussed[:limit],
            "disagreement_board": disagreement[:limit],
        },
        "sample_videos": discussed_videos[:limit],
    }
