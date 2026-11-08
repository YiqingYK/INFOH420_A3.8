"""Discover research PDFs with explicitly pending downstream integrations."""

import logging
import os
from datetime import timedelta
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
            #conn.executemany(
            #    "INSERT OR IGNORE INTO processed_papers 
            #           ('Example.pdf',
            # ) VALUES (?)",
            #    [(filename,) for filename in filenames],
            #)
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
    def generate_html(processing_result):
        """MOCK. Await Gustavo's HTML generator."""
        if not processing_result["processed_filenames"]:
            raise AirflowSkipException(
                "MOCK HTML generation skipped: no newly committed paper metadata"
            )
        raise AirflowSkipException("MOCK HTML generation: Gustavo's integration is pending")

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

        logger.info("db_path: %s",
                    db_path)

        webhook_api = os.environ.get("DISCORD_WEBHOOK_API")
        if not webhook_api:
            raise AirflowFailException("DISCORD_WEBHOOK_API must be configured")

        logger.info("Discord_webhook: %s",
                    webhook_api)

        placeholders = ", ".join("?" for new_file in new_filenames)
        
        logger.info("Placeholder: %s",
                    placeholders)
        with sqlite3.connect(db_path) as conn:
            logger.info("connection successful 1")
            conn.row_factory = sqlite3.Row
            
            logger.info("connection successful 2")
            new_files_metadata = conn.execute(
                f"SELECT * FROM processed_papers WHERE filename IN ({placeholders})",
                tuple(new_filenames),
            ).fetchall()

            logger.info("%s",", ".join(new_file for new_file in new_filenames))
        
            logger.info("%s",new_files_metadata)

#Maybe implement something here about analyzing deletions and renamings
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

            logger.info("Content for %s: %s\n",
                        row["filename"],
                        content)

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
    generate_html(processing_result)
    #notify_discord(["example.pdf"])
    notify_discord(processing_result["processed_filenames"])


paper_pipeline_dag = paper_pipeline()
