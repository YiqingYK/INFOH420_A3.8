"""Batch entry point with durable partial-progress recovery."""

from pathlib import Path

from .database import batch_filenames, get_processed_filenames, store_paper
from .llm import extract_metadata, validate_metadata
from .pdf import pdf_to_markdown


def process_and_store_papers(
    filenames: list[str], papers_dir: Path, db_path: Path, batch_id: str
) -> dict:
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("batch_id must be a nonempty, stable identifier")
    papers_dir, db_path = Path(papers_dir), Path(db_path)
    # Validate every name before starting any work.
    for name in filenames:
        if (not isinstance(name, str) or not name or "/" in name or "\\" in name
                or Path(name).suffix.lower() != ".pdf"):
            raise ValueError(f"Expected a PDF basename: {name!r}")
    processed = get_processed_filenames(db_path)
    for name in dict.fromkeys(filenames):
        if name in processed:
            continue
        source = papers_dir / name
        if source.is_symlink() or not source.is_file():
            raise ValueError(f"Not a regular PDF: {name}")
        markdown = pdf_to_markdown(source)
        metadata = validate_metadata(extract_metadata(markdown), markdown)
        store_paper(db_path, name, metadata, markdown, batch_id)
        processed.add(name)
    committed = batch_filenames(db_path, batch_id)
    return {"status": "complete", "batch_id": batch_id, "processed_filenames": committed}
