"""Discover research PDFs with explicitly pending downstream integrations."""

import logging
import os
from datetime import timedelta
from html import escape
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

import sqlite3
import json
from urllib.request import Request, urlopen
from urllib.error import HTTPError


logger = logging.getLogger(__name__)


@dag(
    dag_id="paper_pipeline",
    description="PDF discovery with pending processing and mock output tasks",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
)
def paper_pipeline():
    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=2),
    )
    def scan_new_papers():
        """List discovered filenames. Processed-file filtering is still pending."""
        library_path = os.environ.get("RESEARCH_LIBRARY_PATH")
        if not library_path:
            raise AirflowFailException("RESEARCH_LIBRARY_PATH must be configured")
        papers_dir = Path(library_path) / "papers"
        try:
            filenames = sorted(
                path.name
                for path in papers_dir.iterdir()
                if path.suffix.lower() == ".pdf" and path.is_file()
            )
        except OSError:
            logger.exception("Unable to list PDFs in %s", papers_dir)
            raise

        db_path = os.environ.get("PAPERS_DB_PATH")
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_papers (
                    filename TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    keywords TEXT,
                    github_url TEXT,
                    dataset_url TEXT,
                    category TEXT,
                    processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            #This is just to input a simple test example into the DB if we haven't yet implemented the LLM->DB flow
            #Remove when we get all tasks ready and integrated
            #Put a random PDF into the folder with "example.pdf" title before this
            conn.execute(
            """
            INSERT OR IGNORE INTO processed_papers
                (filename, title, summary, keywords, github_url, dataset_url, category)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
               (
                    "example.pdf",
                    "Example paper title",
                    "A short summary of the paper.",
                    '["machine learning", "data"]',  # JSON stored as text
                    None,  # github_url
                    None,  # dataset_url
                    "None",  # category
                )
            )
        logger.info("Discovered %d PDF files in %s: %s", len(filenames), papers_dir, filenames)
        logger.info("Processed-file lookup is pending; discovery does not classify PDFs as new")
        return filenames

    @task(retries=0)
    def process_papers(discovered_filenames):
        """MOCK. Await Sofía's processed-file lookup and processing/storage module."""
        if not discovered_filenames:
            logger.info("MOCK processing: empty discovery batch")
            return {"status": "empty", "processed_filenames": []}
        logger.warning(
            "MOCK processing: %d discovered PDFs await the processed-file lookup "
            "and processing/storage integration. No PDFs processed.",
            len(discovered_filenames),
        )
        return {"status": "pending", "processed_filenames": []}

    @task(retries=0)
    def generate_html():
        """MOCK. Await Gustavo's HTML generator."""

        library_path = os.environ.get("RESEARCH_LIBRARY_PATH")
        if not library_path:
            raise AirflowFailException("RESEARCH_LIBRARY_PATH must be configured")
        output_dir = Path(library_path) / "output"

        db_path = os.environ.get("PAPERS_DB_PATH")
        if not db_path:
            raise AirflowFailException("PAPERS_DB_PATH must be configured")
        
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            all_files_metadata = conn.execute(
                f"SELECT * FROM processed_papers"
            ).fetchall()

        output_dir.mkdir(parents=True, exist_ok=True)
        papers_html = "\n".join(
            f"<article><h2>{escape(row['title'])}</h2>"
            f"<p>{escape(row['summary'])}</p>"
            f"<p>Category: {escape(row['category'] or 'None')}</p></article>"
            for row in all_files_metadata
        ) or "<p>No papers found.</p>"
        output_path = output_dir / "output.html"
        output_path.write_text(
            f"<!doctype html><html><head><meta charset=\"utf-8\">"
            f"<title>Papers</title></head><body><h1>Papers</h1>"
            f"{papers_html}</body></html>",
            encoding="utf-8",
        )
        logger.info("Generated paper list at %s", output_path)
        return str(output_path)

    @task(retries=0)
    def notify_discord(new_filenames):
        """MOCK. Await Gustavo's Discord notifier."""
        if not new_filenames:
            raise AirflowSkipException(
                "MOCK Discord notification skipped: no newly committed paper metadata"
            )

        db_path = os.environ.get("PAPERS_DB_PATH")
        if not db_path:
            raise AirflowFailException("PAPERS_DB_PATH must be configured")


        webhook_api = os.environ.get("DISCORD_WEBHOOK_API")
        if not webhook_api:
            raise AirflowFailException("DISCORD_WEBHOOK_API must be configured")

        placeholders = ", ".join("?" for new_file in new_filenames)
        
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            new_files_metadata = conn.execute(
                f"SELECT * FROM processed_papers WHERE filename IN ({placeholders})",
                tuple(new_filenames),
            ).fetchall()

#Maybe implement something here about analyzing deletions and renamings?
#        found_filenames = {row["filename"] for row in new_files_metadata}
#        missing_filenames = set(new_filenames) - found_filenames
#        if missing_filenames:
#            raise AirflowFailException(
#                f"Paper records not found in the database: {sorted(missing_filenames)}"
#            )
        
        for row in new_files_metadata:
            content = (
                f"New paper: {row['title']}\n"
                f"Filename: {row['filename']}\n"
                f"Summary: {row['summary']}\n"
                f"Keywords: {row['keywords'] or 'None'}\n"
                f"GitHub: {row['github_url'] or 'None'}\n"
                f"Dataset: {row['dataset_url'] or 'None'}\n"
                f"Category: {row['category'] or 'None'}"
            )
            if len(content) > 1000:
                content = content[:987] + "\n[truncated]"

            request = Request(
                webhook_api,
                data=json.dumps({"content": content}).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "localDEAIproject/1.0"
                },  
                method="POST",
            )
            try:
                with urlopen(request, timeout=15) as response:
                    response_body = response.read().decode("utf-8")
                    logger.info(
                        "Discord request for %s returned HTTP %s: %s",
                        row["filename"],
                        response.status,
                        response_body or "(empty response body)",
                    )
            except HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                logger.error("Webhook request returned HTTP %s: %s", exc.code, body)
                raise
            
    processing_result = process_papers(scan_new_papers())
    generate_html()
    notify_discord(["example.pdf"]) #Only for testing, must first do the setup in scan_new_papers
    #notify_discord(processing_result["processed_filenames"])


paper_pipeline_dag = paper_pipeline()
