"""Convert text PDFs without modifying the source files."""

from pathlib import Path


def pdf_to_markdown(path: Path) -> str:
    import pymupdf
    import pymupdf4llm

    with pymupdf.open(str(path)) as document:
        markdown = pymupdf4llm.to_markdown(document)
    if not isinstance(markdown, str) or not markdown.strip():
        raise ValueError(f"No text extracted from {path.name}; scanned PDFs need OCR")
    return markdown
