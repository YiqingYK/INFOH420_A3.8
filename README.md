# INFOH420_A3.8
Automated research paper processing pipeline using Apache Airflow, local LLM, SQLite, and Discord.

## Assignment 3 development setup

The DAG has an hourly schedule at the top of the hour in UTC, with catchup disabled and at most one active run. Keep it paused until the real integrations are ready. Discovery lists regular PDF files directly inside the configured `papers/` directory, matches extensions case-insensitively, and returns filenames in Python's deterministic alphabetical order. It reads directory entries without opening or changing PDF contents.

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

Keep `paper_pipeline` paused while integrations are pending. `airflow dags test` permits manual testing while it is paused. For a smoke test before connecting Drive, point `RESEARCH_LIBRARY_DIR` at a temporary local folder containing empty `papers/` and `output/` directories.

Run `docker compose down` to stop this project and retain its Airflow state.

### Provisional integration interfaces

These signatures are proposals for team confirmation. The repository currently contains no implementation of them. Paths below are container `pathlib.Path` values, derived at task runtime from `RESEARCH_LIBRARY_PATH` and `PAPERS_DB_PATH`. A filename is the exact basename, including its extension and case. The assignment assumes unique filenames.

| Owner and function | Inputs and output | Failure behavior |
| --- | --- | --- |
| Sofía — `get_processed_filenames(db_path: Path) -> set[str]` | Returns filenames with successfully committed metadata. Sofía owns database initialization and first-run behavior. | Raise on database access or schema errors. An error must never become an empty processed set. |
| Sofía — `process_and_store_papers(filenames: list[str], papers_dir: Path, db_path: Path, batch_id: str) -> dict` | Checks stored filenames, processes eligible PDFs, validates metadata, and commits it. Returns `{"status": "complete", "batch_id": "...", "processed_filenames": [...]}`. The list identifies valid committed records belonging to this batch, including commits recovered on retry. | Raise on conversion, LLM, validation, or storage failure. Roll back incomplete writes. Enforce unique filenames, record batch membership durably with each commit, and replay the batch result safely on retry. |
| Gustavo — `generate_html(db_path: Path, output_path: Path) -> None` | Reads committed metadata and publishes the complete `output/index.html`. Called only after a nonempty batch of newly committed filenames. | Raise on database, rendering, or filesystem errors. Write a temporary file and replace the HTML atomically. Repeated calls must safely regenerate it. |
| Gustavo — `notify_discord(filenames: list[str], db_path: Path, webhook_url: str, batch_id: str) -> None` | Receives only the batch's newly committed filenames, reads their metadata, and notifies for records still awaiting delivery. Empty batches produce no request. | Raise on unsuccessful responses or transport errors. Check durable delivery state before sending. Agree on handling ambiguous timeouts before adding retries. |

The batch proposal above replaces the earlier provisional per-paper boolean result because that result alone loses successful commits when a later paper fails. These entry points and durable state requirements still need team agreement.

The processing adapter should use the persisted filename lookup to avoid reprocessing and pass the discovered filenames to the batch interface in their existing sorted order. The batch implementation must recheck persisted state on every retry and recover its own previous commits. Use the same `batch_id` across retries, derived from Airflow's `dag_id` and `run_id`. Empty batches return an empty committed list and perform no LLM or notification work. An exception must propagate so HTML and notifications cannot consume a failed batch.

XCom carries only filenames and small status summaries. PDF bytes, Markdown, and full LLM responses stay outside XCom. `max_active_runs=1` prevents overlapping runs of this DAG, while SQLite uniqueness must protect persisted data.

Sofía and Gustavo must agree on durable batch and delivery tracking in the existing local SQLite database before real retries are enabled. If processing fails after some commits, a resumed batch must retain those records for its eventual HTML update and notification. Later DAG runs must also be able to recover abandoned pending deliveries. Successful HTML publication and notification need separate completion markers so either branch can retry independently. HTML reads only valid committed records. If a Discord request times out after the server accepted it, a local sent marker alone cannot guarantee duplicate-free delivery. Keep automatic notification retries disabled until the team agrees on recovery for that case. The current mocks implement none of this durable state.

Configuration already available is `RESEARCH_LIBRARY_DIR` (host bind source), `RESEARCH_LIBRARY_PATH` (container library root), `PAPERS_DB_PATH` (container SQLite path), and `AIRFLOW_PORT` (host UI port). Derive `papers/` and `output/index.html` from the container library root. Proposed integration settings are `LOCAL_LLM_BASE_URL`, `LOCAL_LLM_MODEL`, and secret `DISCORD_WEBHOOK_URL`. These names and any local-server credential need team confirmation and are not yet wired into Compose. Keep actual values in ignored local configuration and never return credentials through XCom.

### Sharing the scaffold

The intended commit contains `.gitignore`, `.env.example`, `compose.yaml`, `dags/paper_pipeline.py`, `data/.gitkeep`, and this README. Review and stage those files explicitly. Keep `.env`, environment variants, generated passwords, databases and their sidecar files, Airflow runtime folders, and local library copies out of Git. `.env.example` contains a blank host path. Compose uses a named volume for Airflow state, and the actual Google Drive path stays in `.env`.

### Verification on November 7, 2026

Checked the existing running Airflow 3.3.2 container. DAG import reported no errors, and Compose validation passed. Nine Python checks using its installed Airflow package passed, covering the real 11-file directory, extension matching, sorting, directory filtering, empty batches, configuration and filesystem errors, dependency/retry settings, and mock output gating.

Two full `airflow dags test paper_pipeline` runs succeeded, against the real library and a temporary empty library. Both recorded `scan_new_papers=success`, `process_papers=success`, `generate_html=skipped`, and `notify_discord=skipped`. Their persisted XComs contained 11 or zero filenames, respectively, and the pending or empty processing summary.

A temporary file in the mounted `output/` directory was successfully written, read, and deleted by the Airflow user. All 11 original PDF names, sizes, and modification times were unchanged afterward. The local `data/` directory still contained only `.gitkeep`.

During the initial checks, every original PDF returned `OSError: [Errno 5] Input/output error` through Docker, preventing content-hash verification. That readability issue was resolved in the follow-up below. Airflow emitted a missing-Graphviz warning during CLI checks. The import and execution checks completed successfully.

### Readability and collaboration follow-up

After the Drive `papers/` folder was made available offline, all 11 PDFs were read to EOF inside the running container in 1 MiB chunks, totaling 50,838,046 bytes. Zero files failed. A second complete chunked read after the tests confirmed identical SHA-256 hashes, filenames, sizes, and modification times.

The existing DAG and Compose configuration required no changes. Import checks, graph validation, case-insensitive discovery and empty-directory fixtures, error propagation, and mock output gating passed. Two fresh full DAG tests succeeded with 11 and zero PDFs. Persisted XComs contained only filenames and small pending/empty summaries, with both output tasks skipped. The DAG remained paused, and `data/` still contained only `.gitkeep`.

The sharing check found exactly the six intended scaffold files, an empty Git staging area, and no local host path, actual webhook URL, or sensitive value from `.env` in those files. Environment variants, generated password files, databases and SQLite sidecars, runtime folders, and local library copies are ignored. The branch remains `main`. Real integrations and agreement on batch/delivery recovery are still pending.
