"""Discover research PDFs with explicitly pending downstream integrations."""

import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

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
    def notify_discord(processing_result):
        """MOCK. Await Gustavo's Discord notifier."""
        if not processing_result["processed_filenames"]:
            raise AirflowSkipException(
                "MOCK Discord notification skipped: no newly committed paper metadata"
            )
        raise AirflowSkipException("MOCK Discord notification: Gustavo's integration is pending")

    processing_result = process_papers(scan_new_papers())
    generate_html(processing_result)
    notify_discord(processing_result)


paper_pipeline_dag = paper_pipeline()
