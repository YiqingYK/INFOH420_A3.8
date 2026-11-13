# Automated research paper library

Assignment 3, INFO-H420. Airflow discovers PDFs in a locally synced Google Drive folder. The processing module converts new PDFs to Markdown, calls a local model through native Ollama or an OpenAI-compatible endpoint, validates JSON and commits metadata to SQLite outside Drive.

## Current scope

Implemented: scheduled discovery, PDF conversion, local LLM extraction, SQLite storage, integration with the processing task, searchable static HTML generation and Discord webhook notifications. Delivery tracking, recovery of pending notifications and the short report remain pending.

## Reproducible startup

Run commands from this directory (`INFOH420_A3.8`). Install and start Docker Desktop. Make the Drive files available offline and create `ResearchLibrary/papers` and `ResearchLibrary/output`.

1. Copy `.env.example` to `.env` (`Copy-Item .env.example .env` in PowerShell, `cp .env.example .env` on macOS/Linux).
2. Set `RESEARCH_LIBRARY_DIR` to your absolute host folder. On Windows use forward slashes, for example `"C:/Users/you/Google Drive/ResearchLibrary"`. Keep quotes for spaces. Each teammate uses their own ignored `.env`.
3. Run `docker compose config --quiet`, then `docker compose up --build -d`.
4. Follow `docker compose logs -f airflow`. With the Ollama profile enabled, also follow `docker compose logs -f ollama-init`. The first Ollama startup downloads the model into a named volume. Allow time and disk space. Ollama uses CPU by default. For an OpenAI-compatible provider, start the local server separately.
5. Open http://localhost:8080, username `admin`. Read the local generated password with `docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated`.

The DAG runs at minute zero of each hour in UTC, with catchup disabled and at most one active run. The default Compose configuration creates new DAGs **unpaused**. To start a fresh test instance paused, add this to a Compose override before bringing it up:

```yaml
services:
  airflow:
    environment:
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
```

For an existing instance, check the actual pause state in the Airflow UI. For import and unit checks:

```sh
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags list
docker compose exec airflow python -m unittest discover -s /opt/airflow/tests -v
```

For the test command, first copy the test folder into the container: `docker compose cp tests airflow:/opt/airflow/tests`. Tests create their own temporary PDFs/database and mock LLM calls. Run `airflow dags test paper_pipeline` only with a separate test library, SQLite path and output, and a local notification receiver. It executes task code, including model requests and webhook POSTs. The saved timetable run is the evidence for automatic scheduling.

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

Returns `{"status": "complete", "batch_id": "...", "processed_filenames": [...]}` containing all records committed by this batch, including recovered commits on retry. First use initializes the database. Previously committed filenames are skipped without reading the PDF or calling the LLM. Filenames are unique, exact and case sensitive; changing a file under the same filename does not reprocess it. Use a new filename for revised papers.

Each paper commits independently after conversion and validation. If a later paper fails, the exception propagates, completed papers remain committed and downstream tasks do not run. Repeating with the same batch ID recovers earlier commits. An empty batch calls no LLM. The Airflow adapter uses `dag_id:run_id` as the stable identifier. Discovery passes every filename so resumed batches retain their membership; the processing module determines which ones are new.

Markdown is stored in the same SQLite record, so no extra file can become inconsistent with its metadata. Source PDFs are opened for reading and closed explicitly. Scanned PDFs without extractable text fail with an OCR message; OCR is outside this implementation.

## LLM configuration

Set `LOCAL_LLM_PROVIDER` in `.env` to `ollama` (default) or `openai_compatible`. `COMPOSE_PROFILES=${LOCAL_LLM_PROVIDER:-ollama}` selects the matching Compose profile, so Ollama is not started when using the OpenAI-compatible server. For Ollama, use `LOCAL_LLM_API=http://ollama:11434/api/generate`. To switch to an OpenAI-compatible local server (if you have one), set:

```env
LOCAL_LLM_PROVIDER=openai_compatible
COMPOSE_PROFILES=openai_compatible
LOCAL_LLM_API=http://host.docker.internal:1236/v1/chat/completions
LOCAL_LLM_MODEL=your-model-name
LOCAL_LLM_API_KEY=
```

Set `LOCAL_LLM_API_KEY` if that server requires a key. The server must be reachable from Docker; on the host machine, bind it to an address accessible to Docker rather than only `localhost`. The `openai_compatible` provider sends chat-completions requests with JSON Schema output and extracts the reply from `choices[0].message.content`; Ollama continues using its native generate API and JSON schema. The same model, timeout (300 seconds by default) and input-size (24000 characters) settings apply to both. After changing `.env`, rerun `docker compose up --build -d`.

The request supplies a JSON schema and disables streaming. Required fields are nonempty strings `title`, `summary`, `category`, and a string array `keywords`. Optional `code_urls` and `dataset_urls` default to empty arrays. Invalid response JSON or field types trigger exactly one further generation; transport errors also receive one retry, while HTTP errors propagate immediately. Optional links are retained only when they are HTTP(S) URLs literally present in the full Markdown. Links are classified by their role in the paper, independently of domain: code can be hosted on GitHub, GitLab, university websites or elsewhere. Classification remains a model judgment; the code does not visit links or prove their contents.

Long papers send the first 75% and last 25% of the configured character budget, omitting the middle. The complete Markdown remains stored. This keeps small-model requests bounded but can miss information in the omitted section; character limits do not guarantee a particular token count. Summary quality and performance need checking with the team's real papers.

Implementation references: [Ollama structured outputs](https://ollama.com/blog/structured-outputs), [PyMuPDF4LLM API](https://pymupdf.readthedocs.io/en/latest/pymupdf4llm/api.html).

## Output integration contract

`list_papers(db_path)` returns dictionaries with decoded lists for `keywords`, `code_urls` and `dataset_urls`. The `papers` table also contains `filename`, `title`, `summary`, `category`, `markdown`, `batch_id` and `created_at`. Output delivery markers are not implemented.

HTML reads all committed records and atomically replaces `output/index.html` with a self-contained static catalog. It displays each paper's metadata, provides text search across title, filename, summary and keywords, and filters by categories present in the database. Discord reads metadata for the current batch and sends one webhook message per paper, truncated to 2000 characters. It requires `DISCORD_WEBHOOK_API` and skips delivery when the batch has no new papers. Later runs do not recover pending notifications from earlier batches, and manually rerunning the notification task can duplicate messages already sent. HTML and Discord completion are not recorded. Discord timeouts can be ambiguous, so notification retries remain disabled pending the team's delivery policy. The database is initialized with the current schema; migrations from older schemas are not supported.

XCom contains only filenames, the batch ID and the HTML output path. Markdown and full LLM responses never enter XCom. Database uniqueness protects duplicate inserts, and the DAG serializes runs.

## Local verification

With Python and the pinned dependencies:

```sh
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Tests cover first-run initialization, empty batches, unsafe filenames, duplicate skipping, batch replay, partial-failure recovery, JSON retry limits, optional link evidence and real PDF conversion without changing the input. On 11 November 2026, an isolated Airflow 3.3.2 timetable run processed one Drive-synchronised PDF with Gemma 4 E2B through LM Studio. All four tasks succeeded on the first attempt in 17.839 seconds. The first model response took 12.105 seconds between request and response log entries. The run produced one SQLite record and one catalog entry. A local HTTP receiver accepted one notification payload with HTTP 204. In a separate step, the generated catalog was copied to shared Drive as `index-e2e-test.html` and verified visible on 12 November. The redesigned renderer was checked with the saved record on 13 November. Real Discord delivery and real-model multi-PDF processing remain unverified.

The original standard Docker build failed to resolve `pymupdf==1.26.7`. The evaluation used an offline image with the same dependency pins. On 13 November, standard and offline no-cache builds both succeeded on Linux ARM64 with the original pins. If the build cannot resolve or download the pinned wheels, check access to the package index and wheel host from Docker and confirm the target platform. A platform-matched, checksum-verified wheel directory can be installed with `pip --no-index --find-links=/path/to/wheels -r requirements.txt`.
