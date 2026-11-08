# Automated research paper library

Assignment 3, INFO-H420. Airflow discovers PDFs in a locally synced Google Drive folder. The processing module converts new PDFs to Markdown, calls local Ollama, validates JSON and commits metadata to SQLite outside Drive.

## Current scope

Implemented: scheduled discovery, PDF conversion, local LLM extraction, SQLite storage and integration with the processing task. HTML generation and Discord notifications are still explicit mocks owned by the output integration teammate. The complete assignment is not finished until those integrations and the short report are added.

## Reproducible startup

Run commands from this directory (`INFOH420_A3.8`). Install and start Docker Desktop. Make the Drive files available offline and create `ResearchLibrary/papers` and `ResearchLibrary/output`.

1. Copy `.env.example` to `.env` (`Copy-Item .env.example .env` in PowerShell, `cp .env.example .env` on macOS/Linux).
2. Set `RESEARCH_LIBRARY_DIR` to your absolute host folder. On Windows use forward slashes, for example `"C:/Users/you/Google Drive/ResearchLibrary"`. Keep quotes for spaces. Each teammate uses their own ignored `.env`.
3. Run `docker compose config --quiet`, then `docker compose up --build -d`.
4. Follow `docker compose logs -f ollama-init airflow`. First startup downloads the model; allow time and disk space. Ollama uses CPU by default, with no GPU required. Its model cache persists in a named volume. Airflow starts after a successful model download.
5. Open http://localhost:8080, username `admin`. Read the local generated password with `docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated`.

The DAG starts paused, runs hourly in UTC and allows one active run. Keep it paused until HTML and Discord are integrated. For manual checks:

```sh
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags test paper_pipeline
docker compose exec airflow python -m unittest discover -s /opt/airflow/tests -v
```

For the test command, first copy the test folder into the container: `docker compose cp tests airflow:/opt/airflow/tests`. Tests create their own temporary PDFs/database and mock Ollama calls. A DAG test processes real PDFs and stores records; output tasks currently skip.

`docker compose down` retains state. Metadata lives at `./data/papers.db`, outside the synced library. Airflow state has a separate volume. Do not commit databases, `.env`, PDFs or generated passwords.

## Processing module and team API

```python
from pathlib import Path
from paper_processing import get_processed_filenames, process_and_store_papers
from paper_processing.database import list_papers

result = process_and_store_papers(
    filenames=["paper.pdf"],
    papers_dir=Path("/opt/airflow/ResearchLibrary/papers"),
    db_path=Path("/opt/airflow/data/papers.db"),
    batch_id="manual:example-1",  # same value on retries, new value for a new batch
)
```

Returns `{"status": "complete", "batch_id": "...", "processed_filenames": [...]}` containing all records committed by this batch, including recovered commits on retry. First use initializes the database. Previously committed filenames are skipped without reading the PDF or calling Ollama. Filenames are unique, exact and case sensitive; changing a file under the same filename does not reprocess it. Use a new filename for revised papers.

Each paper commits independently after conversion and validation. If a later paper fails, the exception propagates, completed papers remain committed and downstream tasks do not run. Repeating with the same batch ID recovers earlier commits. An empty batch calls no LLM. The Airflow adapter uses `dag_id:run_id` as the stable identifier. Discovery passes every filename so resumed batches retain their membership; the processing module determines which ones are new.

Markdown is stored in the same SQLite record, so no extra file can become inconsistent with its metadata. Source PDFs are opened for reading and closed explicitly. Scanned PDFs without extractable text fail with an OCR message; OCR is outside this implementation.

## LLM configuration

Compose uses `LOCAL_LLM_BASE_URL=http://ollama:11434`, the native Ollama `/api/generate` endpoint. No API key or cloud account is needed. `.env` configures `LOCAL_LLM_MODEL` (default `qwen2.5:3b`), `LOCAL_LLM_TIMEOUT` in seconds (300) and `LOCAL_LLM_MAX_CHARS` (24000). After changing models, rerun `docker compose up --build -d` so initialization downloads it.

The request supplies a JSON schema and disables streaming. Required fields are nonempty strings `title`, `summary`, `category`, and a string array `keywords`. Optional `code_urls` and `dataset_urls` default to empty arrays. Invalid response JSON or field types trigger exactly one further generation; HTTP/transport errors propagate. Optional links are retained only when they are HTTP(S) URLs literally present in the full Markdown. Links are classified by their role in the paper, independently of domain: code can be hosted on GitHub, GitLab, university websites or elsewhere. Classification remains a model judgment; the code does not visit links or prove their contents.

Long papers send the first 75% and last 25% of the configured character budget, omitting the middle. The complete Markdown remains stored. This keeps small-model requests bounded but can miss information in the omitted section; character limits do not guarantee a particular token count. Summary quality and performance need checking with the team's real papers.

Implementation references: [Ollama structured outputs](https://ollama.com/blog/structured-outputs), [PyMuPDF4LLM API](https://pymupdf.readthedocs.io/en/latest/pymupdf4llm/api.html).

## Output integration contract

`list_papers(db_path)` returns dictionaries with decoded lists for `keywords`, `code_urls` and `dataset_urls`. The `papers` table also contains `filename`, `title`, `summary`, `category`, `markdown`, `batch_id`, `created_at`, `html_published_at` and `discord_notified_at`. The last two nullable columns are reserved for Gustavo's integrations; this module never marks output as delivered.

HTML should read all committed records, escape extracted text, and replace `output/index.html` atomically. Discord should read metadata for the batch filenames. Later runs must also recover records with pending output markers from abandoned batches; the current output mocks do not implement that recovery. HTML and Discord completion must be recorded separately. Discord timeouts can be ambiguous, so notification retries remain disabled pending the team's delivery policy. The database is initialized with the current schema; migrations from older schemas are not supported.

XCom contains only filenames and the small processing result. Markdown and full LLM responses never enter XCom. Database uniqueness protects duplicate inserts, and the DAG serializes runs.

## Local verification

With Python and the pinned dependencies:

```sh
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Tests cover first-run initialization, empty batches, unsafe filenames, duplicate skipping, batch replay, partial-failure recovery, JSON retry limits, optional link evidence and real PDF conversion without changing the input. Docker/real-model end-to-end validation must be performed on a machine with Docker Desktop; it was not available in the implementation environment.
