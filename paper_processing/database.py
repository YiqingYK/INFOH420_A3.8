"""Local SQLite storage. Only validated, complete papers are committed."""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def connect(db_path: Path):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(db_path, timeout=30)
    db.row_factory = sqlite3.Row
    try:
        db.execute("""CREATE TABLE IF NOT EXISTS papers (
            filename TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            keywords TEXT NOT NULL,
            category TEXT NOT NULL,
            code_urls TEXT,
            dataset_urls TEXT,
            markdown TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )""")
        db.commit()
        with db:
            yield db
    finally:
        db.close()


def get_processed_filenames(db_path: Path) -> set[str]:
    with connect(db_path) as db:
        return {row[0] for row in db.execute("SELECT filename FROM papers")}


def store_paper(db_path, filename, metadata, markdown, batch_id):
    with connect(db_path) as db:
        db.execute("""INSERT INTO papers
            (filename, title, summary, keywords, category, code_urls,
             dataset_urls, markdown, batch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(filename) DO NOTHING""", (
                filename, metadata["title"], metadata["summary"],
                json.dumps(metadata["keywords"], ensure_ascii=False),
                metadata["category"],
                json.dumps(metadata["code_urls"], ensure_ascii=False),
                json.dumps(metadata["dataset_urls"], ensure_ascii=False),
                markdown, batch_id,
            ))


def batch_filenames(db_path, batch_id):
    with connect(db_path) as db:
        return [row[0] for row in db.execute(
            "SELECT filename FROM papers WHERE batch_id = ? ORDER BY filename", (batch_id,)
        )]


def list_papers(db_path: Path) -> list[dict]:
    """Return committed papers, decoding the three JSON list columns."""
    with connect(db_path) as db:
        papers = [dict(row) for row in db.execute(
            "SELECT * FROM papers ORDER BY created_at, filename"
        )]
    for paper in papers:
        for field in ("keywords", "code_urls", "dataset_urls"):
            paper[field] = json.loads(paper[field])
    return papers

def list_papers_by_batch(db_path: Path, batch_id: str) -> list[dict]:
    """Return only one batch's papers, decoding the three JSON list columns."""
    with connect(db_path) as db:
        papers = [dict(row) for row in db.execute(
            "SELECT * FROM papers WHERE batch_id = ? ORDER BY filename",
            (batch_id,),
        )]
    for paper in papers:
        for field in ("keywords", "code_urls", "dataset_urls"):
            paper[field] = json.loads(paper[field])
    return papers