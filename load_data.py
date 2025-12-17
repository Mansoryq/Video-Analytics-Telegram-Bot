import json
import psycopg2
from datetime import datetime
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
            cur.execute("""
                INSERT INTO videos (
                    id, creator_id, video_created_at, views_count, likes_count,
                    comments_count, reports_count, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                video["id"],
                video["creator_id"],
                video["video_created_at"],
                video["views_count"],
                video["likes_count"],
                video["comments_count"],
                video["reports_count"],
                video.get("created_at") or datetime.utcnow().isoformat(),
                video.get("updated_at") or datetime.utcnow().isoformat()
            ))

            for snap in video.get("snapshots", []):
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
                    snap["created_at"],
                    snap.get("updated_at") or datetime.utcnow().isoformat()
                ))
        conn.commit()
    conn.close()
    print("Данные загружены!")

if __name__ == "__main__":
    load_json_to_db("videos.json")