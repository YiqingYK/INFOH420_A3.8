"""Discover and process research PDFs; output integrations remain pending."""

import logging
import os
from datetime import timedelta
from html import escape
from pathlib import Path

import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

import json
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from paper_processing.database import list_papers, batch_filenames, list_papers_by_batch


logger = logging.getLogger(__name__)


@dag(
    dag_id="paper_pipeline",
    description="PDF to Markdown, local LLM metadata and SQLite; pending output tasks",
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
    def scan_all_papers():
        """List PDFs; storage checks filenames when processing the batch."""
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

        logger.info("Discovered %d PDF files in %s: %s", len(filenames), papers_dir, filenames)
        return filenames

    @task(retries=0)
    def process_papers(discovered_filenames):
        """Commit valid metadata and recover this run's prior partial commits."""
        from paper_processing import process_and_store_papers

        library_path = os.environ.get("RESEARCH_LIBRARY_PATH")
        db_path = os.environ.get("PAPERS_DB_PATH")
        if not library_path or not db_path:
            raise AirflowFailException("RESEARCH_LIBRARY_PATH and PAPERS_DB_PATH are required")
        context = get_current_context()
        batch_id = f"{context['dag'].dag_id}:{context['run_id']}"
        logger.info("Processing batch %s", batch_id)
        process_and_store_papers(
            discovered_filenames,
            Path(library_path) / "papers",
            Path(db_path),
            batch_id=batch_id,
        )
        return batch_id

    @task(retries=0)
    def generate_html():
        """MOCK. Work in progress."""

        library_path = os.environ.get("RESEARCH_LIBRARY_PATH")
        if not library_path:
            raise AirflowFailException("RESEARCH_LIBRARY_PATH must be configured")
        output_dir = Path(library_path) / "output"

        db_path = os.environ.get("PAPERS_DB_PATH")
        if not db_path:
            raise AirflowFailException("PAPERS_DB_PATH must be configured")
        
        all_files_metadata = list_papers(db_path)

        output_dir.mkdir(parents=True, exist_ok=True)
        papers_html = "\n".join(
            f"<article>"
            f"<h2>{escape(row['title'])}</h2>"
            f"<p>Filename: {escape(row['filename'])}</p>"
            f"<p>Summary: {escape(row['summary'])}</p>"
            f"<p>Keywords:</p>"
            f"<ul>{''.join(f'<li>{escape(k)}</li>' for k in (row.get('keywords') or []))}</ul>"
            f"<p>Category: {escape(row['category'] or 'None')}</p>"
            f"<p>Code URLs:</p>"
            f"<ul>{''.join(f'<li>{escape(k)}</li>' for k in (row.get('code_urls') or []))}</ul>"
            f"<p>Dataset:</p>"
            f"<ul>{''.join(f'<li>{escape(k)}</li>' for k in (row.get('dataset_urls') or []))}</ul>"
            f"</article>"
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
    def notify_discord(batch_id):
        """MOCK. Await Gustavo's Discord notifier."""

        webhook_api = os.environ.get("DISCORD_WEBHOOK_API")
        if not webhook_api:
            raise AirflowFailException("DISCORD_WEBHOOK_API must be configured")
        
        db_path = os.environ.get("PAPERS_DB_PATH")
        if not db_path:
            raise AirflowFailException("PAPERS_DB_PATH must be configured")

        new_papers = list_papers_by_batch(db_path, batch_id)
        if not new_papers:
            raise AirflowSkipException(
                "Discord notification skipped: no newly committed paper metadata"
            )

        #Maybe implement something here about analyzing deletions and renamings?
        
        for row in new_papers:
            content = (
                f"New paper: {row['title']}\n"
                f"Filename: {row['filename']}\n"
                f"Summary: {row['summary']}\n"
                f"Keywords: {row['keywords'] or 'None'}\n"
                f"Code URLs: {row['code_urls'] or 'None'}\n"
                f"Dataset: {row['dataset_urls'] or 'None'}\n"
                f"Category: {row['category'] or 'None'}"
            )
            if len(content) > 2000:
                content = content[:1987] + "\n[truncated]"

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
            
    batch_id = process_papers(scan_all_papers())
    generate_html()
    notify_discord(batch_id)

paper_pipeline_dag = paper_pipeline()
