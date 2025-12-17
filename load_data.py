import json
import psycopg2
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def load_json_to_db(json_path):
    conn = psycopg2.connect(DB_URL)
    with open(json_path, 'r', encoding='utf-8') as f:
        raw = json.load(f)
        data = raw["videos"]

    with conn.cursor() as cur:
        for video in data:
            vca = video["video_created_at"]
            if vca.endswith('Z'):
                vca = vca[:-1] + '+00:00'
            video_created_at = datetime.fromisoformat(vca)
            if video_created_at.tzinfo is None:
                video_created_at = video_created_at.replace(tzinfo=timezone.utc)

            ca = video.get("created_at") or datetime.now(timezone.utc).isoformat()
            if ca.endswith('Z'):
                ca = ca[:-1] + '+00:00'
            created_at = datetime.fromisoformat(ca)
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)

            ua = video.get("updated_at") or datetime.now(timezone.utc).isoformat()
            if ua.endswith('Z'):
                ua = ua[:-1] + '+00:00'
            updated_at = datetime.fromisoformat(ua)
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)

            cur.execute("""
                INSERT INTO videos (
                    id, creator_id, video_created_at, views_count, likes_count,
                    comments_count, reports_count, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                video["id"],
                video["creator_id"],
                video_created_at,
                video["views_count"],
                video["likes_count"],
                video["comments_count"],
                video["reports_count"],
                created_at,
                updated_at
            ))

            for snap in video.get("snapshots", []):
                s_ca = snap["created_at"]
                if s_ca.endswith('Z'):
                    s_ca = s_ca[:-1] + '+00:00'
                snap_created_at = datetime.fromisoformat(s_ca)
                if snap_created_at.tzinfo is None:
                    snap_created_at = snap_created_at.replace(tzinfo=timezone.utc)

                s_ua = snap.get("updated_at") or datetime.now(timezone.utc).isoformat()
                if s_ua.endswith('Z'):
                    s_ua = s_ua[:-1] + '+00:00'
                snap_updated_at = datetime.fromisoformat(s_ua)
                if snap_updated_at.tzinfo is None:
                    snap_updated_at = snap_updated_at.replace(tzinfo=timezone.utc)

                cur.execute("""
                    INSERT INTO video_snapshots (
                        id, video_id, views_count, likes_count, comments_count, reports_count,
                        delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    snap["id"],
                    snap["video_id"],
                    snap["views_count"],
                    snap["likes_count"],
                    snap["comments_count"],
                    snap["reports_count"],
                    snap["delta_views_count"],
                    snap["delta_likes_count"],
                    snap["delta_comments_count"],
                    snap["delta_reports_count"],
                    snap_created_at,
                    snap_updated_at
                ))
        conn.commit()
    conn.close()
    print("Данные успешно загружены")

if __name__ == "__main__":
    load_json_to_db("videos.json")
