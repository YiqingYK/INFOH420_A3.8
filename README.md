# INFOH420_A3.8
Automated research paper processing pipeline using Apache Airflow, local LLM, SQLite, and Discord.

## Assignment 3 development setup

The DAG runs hourly at the top of the hour in UTC, with catchup disabled and at most one active run. Discovery lists regular PDF files directly inside the configured `papers/` directory, matches extensions case-insensitively, and returns filenames in Python's deterministic alphabetical order. It reads directory entries without opening or changing PDF contents.

Discovery reports all matching filenames. Determining which filenames are new is **pending Sofía's processed-file lookup**. The processing task is explicitly a mock and returns `{"status": "pending", "processed_filenames": []}` for a nonempty discovery batch, or `{"status": "empty", "processed_filenames": []}` for an empty batch. Both output tasks are explicitly mocks and skip when no metadata has been committed.

```text
Google Drive/
└── ResearchLibrary/
    ├── papers/
    └── output/
        └── index.html       # Future HTML output

Local repository/
├── compose.yaml
├── .env.example
├── dags/paper_pipeline.py
└── data/
    └── papers.db            # Future application database
```

### Start Airflow

1. Start Docker Desktop. In your locally synced Google Drive, make sure `ResearchLibrary/papers/` and `ResearchLibrary/output/` exist and are available locally.
2. Run `cp -n .env.example .env`. In `.env`, set `RESEARCH_LIBRARY_DIR` to the absolute macOS path of your own `ResearchLibrary` folder. Keep quotes around paths with spaces. If Docker Desktop denies the mount, allow file sharing for that directory.
3. From the repository root, run:

   ```sh
   docker compose config --quiet
   docker compose up -d
   docker compose logs --tail=50 airflow
   ```

4. Open [Airflow](http://localhost:8080) once startup completes. The username is `admin`. Read the generated password locally with:

   ```sh
   docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
   ```

Set `AIRFLOW_PORT` in `.env` if port 8080 is in use. The UI is bound to the local machine. Compose mounts Google Drive at `/opt/airflow/ResearchLibrary` for future reads from `papers/` and writes to `output/`. `PAPERS_DB_PATH` is a container path under `/opt/airflow/data/`, which maps to the repository's local `data/` directory. Airflow's own metadata and logs live in a separate Docker volume.

### Test discovery and orchestration

```sh
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow tasks list paper_pipeline
docker compose exec airflow airflow dags test paper_pipeline
```

Expect discovery and mock processing to succeed, both mock output tasks to be skipped, and the DAG run to succeed. Logs state the discovery count and that processed-file lookup is pending. Both `generate_html` and `notify_discord` receive the processing summary through XCom and wait for successful `process_papers`, which receives the scanner's filename list. The default `all_success` trigger rule prevents either output task from running after failed processing.

Directory access errors fail discovery and retry twice with exponential backoff starting at 30 seconds, capped at two minutes. Discovery has a two-minute execution timeout. Missing path configuration fails immediately. A missing, inaccessible, or invalid `papers/` directory is an error, distinct from an empty directory. The downstream mocks have zero retries and perform no external work. Before enabling retries for real processing, the team must confirm the idempotency contract below.

To enable hourly scheduler runs, unpause `paper_pipeline` in the UI. For a smoke test before connecting Drive, point `RESEARCH_LIBRARY_DIR` at a temporary local folder containing empty `papers/` and `output/` directories.

Run `docker compose down` to stop this project and retain its Airflow state.

### Provisional integration interfaces

These signatures are proposals for team confirmation. The repository currently contains no implementation of them. Paths below are container `pathlib.Path` values, derived at task runtime from `RESEARCH_LIBRARY_PATH` and `PAPERS_DB_PATH`. A filename is the exact basename, including its extension and case. The assignment assumes unique filenames.

| Owner and function | Inputs and output | Failure behavior |
| --- | --- | --- |
| Sofía — `get_processed_filenames(db_path: Path) -> set[str]` | Returns filenames with successfully committed metadata. Sofía owns database initialization and first-run behavior. | Raise on database access or schema errors. An error must never become an empty processed set. |
| Sofía — `process_and_store_paper(pdf_path: Path, db_path: Path) -> bool` | Converts one PDF, calls the local LLM, validates metadata, and commits it. Returns `True` for a newly committed record or `False` for an already stored filename. | Raise on conversion, LLM, validation, or storage failure. Roll back incomplete writes. Recheck the filename before processing and enforce its uniqueness in SQLite so retries do not create duplicate records. |
| Gustavo — `generate_html(db_path: Path, output_path: Path) -> None` | Reads committed metadata and publishes the complete `output/index.html`. Called only after a nonempty batch of newly committed filenames. | Raise on database, rendering, or filesystem errors. Write a temporary file and replace the HTML atomically. Repeated calls must safely regenerate it. |
| Gustavo — `notify_discord(filenames: list[str], db_path: Path, webhook_url: str) -> None` | Receives only newly committed filenames and reads their metadata for a batch notification. Empty batches produce no request. Webhook configuration remains pending integration. | Raise on unsuccessful responses or transport errors. Confirm duplicate-delivery handling before adding automatic retries. |

When these modules arrive, `process_papers` should subtract the persisted processed set from discovery, then process the remaining files in the existing sorted order. Recheck persisted state on every retry. It should return `{"status": "complete", "processed_filenames": [...]}` only after successful processing, with filenames that were newly committed. An exception must propagate so HTML and notifications cannot consume a failed batch.

XCom carries only filenames and small status summaries. PDF bytes, Markdown, and full LLM responses stay outside XCom. `max_active_runs=1` prevents overlapping runs of this DAG, while SQLite uniqueness must protect persisted data.

Recovery after a partial batch commit needs team agreement. A retry may find records already committed by a failed attempt. Filename uniqueness prevents duplicate rows, but it cannot by itself track pending HTML updates or notifications. The team must define durable delivery tracking before claiming reliable recovery or enabling notification retries. The current mock performs no processing, database writes, HTML generation, or notifications.

### Verification on November 7, 2026

Checked the existing running Airflow 3.3.2 container. DAG import reported no errors, and Compose validation passed. Nine Python checks using its installed Airflow package passed, covering the real 11-file directory, extension matching, sorting, directory filtering, empty batches, configuration and filesystem errors, dependency/retry settings, and mock output gating.

Two full `airflow dags test paper_pipeline` runs succeeded, against the real library and a temporary empty library. Both recorded `scan_new_papers=success`, `process_papers=success`, `generate_html=skipped`, and `notify_discord=skipped`. Their persisted XComs contained 11 or zero filenames, respectively, and the pending or empty processing summary.

A temporary file in the mounted `output/` directory was successfully written, read, and deleted by the Airflow user. All 11 original PDF names, sizes, and modification times were unchanged afterward. The local `data/` directory still contained only `.gitkeep`.

Content hashes could not be verified because every original PDF returned `OSError: [Errno 5] Input/output error` when read through Docker. Resolve Drive file availability or mount readability before integrating PDF conversion. Airflow also emitted a missing-Graphviz warning during CLI checks. The import and execution checks completed successfully.
