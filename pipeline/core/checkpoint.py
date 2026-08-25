from __future__ import annotations

import sqlite3
import time
from pathlib import Path


class CheckpointStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute(
            """
            create table if not exists processed_images (
                relative_path text primary key,
                image_id text,
                sha256 text,
                status text,
                updated_at real
            )
            """
        )
        self.connection.commit()

    def is_done(self, relative_path: str) -> bool:
        row = self.connection.execute(
            "select 1 from processed_images where relative_path = ? limit 1",
            (relative_path,),
        ).fetchone()
        return row is not None

    def mark_done(self, record: dict) -> None:
        final = record.get("final", {})
        self.connection.execute(
            """
            insert into processed_images(relative_path, image_id, sha256, status, updated_at)
            values (?, ?, ?, ?, ?)
            on conflict(relative_path) do update set
                image_id = excluded.image_id,
                sha256 = excluded.sha256,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (
                record.get("relative_path"),
                record.get("image_id"),
                record.get("sha256"),
                final.get("decision_status"),
                time.time(),
            ),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

