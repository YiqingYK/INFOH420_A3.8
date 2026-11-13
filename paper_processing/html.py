"""Render the paper catalog as a self-contained, searchable HTML page."""

import os
import tempfile
from collections.abc import Mapping, Sequence
from html import escape
from pathlib import Path
from urllib.parse import urlsplit


def _text(value: object) -> str:
    return value if isinstance(value, str) else ""


def _render_values(values: object) -> str:
    if not isinstance(values, (list, tuple)) or not values:
        return '<span class="muted">None</span>'

    items = []
    for value in values:
        text = _text(value)
        if not text:
            continue
        items.append(f"<li>{escape(text)}</li>")
    return f"<ul>{''.join(items)}</ul>" if items else '<span class="muted">None</span>'


def _render_urls(values: object) -> str:
    if not isinstance(values, (list, tuple)) or not values:
        return '<span class="muted">None</span>'

    items = []
    for value in values:
        text = _text(value)
        if not text:
            continue
        try:
            parsed = urlsplit(text)
            parsed.port
            is_http_url = (
                parsed.scheme.lower() in {"http", "https"}
                and bool(parsed.hostname)
                and parsed.username is None
                and parsed.password is None
                and not any(ord(character) < 32 for character in text)
            )
        except ValueError:
            is_http_url = False
        if is_http_url:
            items.append(
                f'<li><a href="{escape(text)}" target="_blank" '
                f'rel="noopener noreferrer">{escape(text)}</a></li>'
            )
        else:
            items.append(f"<li>{escape(text)}</li>")
    return f"<ul>{''.join(items)}</ul>" if items else '<span class="muted">None</span>'


def _render_paper(row: Mapping[str, object]) -> str:
    title = _text(row.get("title")) or "Untitled paper"
    filename = _text(row.get("filename"))
    summary = _text(row.get("summary"))
    category = _text(row.get("category"))
    keywords = row.get("keywords")
    searchable_values = [title, filename, summary]
    if isinstance(keywords, (list, tuple)):
        searchable_values.extend(_text(keyword) for keyword in keywords)
    searchable_text = " ".join(searchable_values)
    category_text = category or "None"

    return (
        f'<article class="paper-card" data-search="{escape(searchable_text)}" '
        f'data-category="{escape(category)}">'
        f'<p class="paper-category">{escape(category_text)}</p>'
        f"<h2>{escape(title)}</h2>"
        f'<p class="filename">{escape(filename) or "Filename unavailable"}</p>'
        f'<section><h3>Summary</h3><p>{escape(summary) or "No summary available."}</p></section>'
        f'<section class="keywords"><h3>Keywords</h3>{_render_values(keywords)}</section>'
        f'<section class="resources"><h3>Code resources</h3>{_render_urls(row.get("code_urls"))}</section>'
        f'<section class="resources"><h3>Dataset resources</h3>{_render_urls(row.get("dataset_urls"))}</section>'
        "</article>"
    )


def _render_page(papers: Sequence[Mapping[str, object]]) -> str:
    categories = sorted(
        {
            category
            for paper in papers
            if (category := _text(paper.get("category")))
        },
        key=str.casefold,
    )
    category_options = "".join(
        f'<option value="{escape(category)}">{escape(category)}</option>'
        for category in categories
    )
    paper_cards = "".join(_render_paper(paper) for paper in papers)
    empty_message = (
        '<p class="empty-state" id="empty-state">No papers found. Try another search or category.</p>'
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light">
  <title>The Research Archive</title>
  <style>

    :root {{ color-scheme: light; font-family: "Segoe UI", Arial, sans-serif; color: #343b35; background: #edeae1; --ink: #343b35; --muted: #676b61; --green: #405c48; --paper: #fffdf6; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; min-width: 320px; background-image: linear-gradient(#63765b0d 1px, transparent 1px), linear-gradient(90deg, #63765b0d 1px, transparent 1px); background-size: 24px 24px; }}
    .hero {{ padding: 0 max(1.5rem, calc((100% - 1180px) / 2)); border-bottom: 1px solid #ccc9ba; background: #f6f3e9; }}
    .topbar {{ display: flex; justify-content: space-between; align-items: center; gap: 1rem; padding: 1.3rem 0; border-bottom: 1px solid #d9d5c8; font: .68rem "Courier New", monospace; letter-spacing: .1em; }}
    .brand {{ display: flex; align-items: center; gap: .7rem; font-weight: bold; }}
    .crest {{ width: 21px; height: 25px; border: 1.5px solid var(--green); background: repeating-linear-gradient(transparent 0 5px, #80917a 5px 6px); box-shadow: 3px 3px 0 #d7ddcc; }}
    .status {{ color: var(--muted); font-size: .62rem; }}
    .hero-content {{ display: grid; grid-template-columns: 1fr 245px; align-items: center; gap: 3rem; padding: 3rem 0 3.5rem; }}
    .eyebrow {{ margin: 0 0 1rem; color: #72745f; font: .68rem "Courier New", monospace; letter-spacing: .12em; text-transform: uppercase; }}
    h1 {{ margin: 0; font: normal clamp(3rem, 6vw, 5.4rem)/1.03 Georgia, "Times New Roman", serif; letter-spacing: -.055em; }}
    h1 span {{ color: var(--green); font-style: italic; }}
    .intro {{ max-width: 500px; margin: 1.4rem 0 0; font-size: .95rem; line-height: 1.8; color: #62675d; }}
    .desk-note {{ position: relative; padding: 2rem 1.5rem 1.5rem; background: #f1df9e; box-shadow: 2px 6px 12px #514a351c; transform: rotate(3deg); color: #514c34; }}
    .desk-note::before {{ content: ""; position: absolute; width: 85px; height: 25px; background: #d1c5a099; top: -12px; left: calc(50% - 42px); transform: rotate(-6deg); }}
    .note-caption {{ display: block; font: .63rem "Courier New", monospace; letter-spacing: .06em; }}
    .desk-note strong {{ display: block; margin: .6rem 0; font: normal 3.8rem/1 Georgia, serif; }}
    .desk-note p {{ margin: .7rem 0 0; padding-top: .8rem; border-top: 1px solid #b6a76d; font: italic 1rem/1.5 Georgia, serif; }}
    main {{ width: min(1180px, calc(100% - 3rem)); margin: 2rem auto 3rem; }}
    .toolbar {{ display: grid; grid-template-columns: minmax(0, 1fr) minmax(220px, .4fr); gap: 1.5rem; padding: 1.3rem 1.5rem; border: 1px solid #d0cbbb; background: #f8f6ed; box-shadow: 0 2px 3px #423e3010; }}
    label {{ display: grid; gap: .6rem; color: #5e6756; font: bold .65rem "Courier New", monospace; text-transform: uppercase; letter-spacing: .08em; }}
    input, select {{ width: 100%; min-height: 46px; border: 1px solid #bcbfae; border-radius: 2px; padding: .8rem; color: var(--ink); background: #fffdf7; font: .9rem "Segoe UI", Arial, sans-serif; }}
    input::placeholder {{ color: #727668; }}
    input:focus-visible, select:focus-visible, a:focus-visible {{ outline: 3px solid #66866a; outline-offset: 3px; }}
    .collection-heading {{ display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: .7rem; margin: 2.5rem 0 1.5rem; border-bottom: 1px solid #c9c6b8; padding-bottom: .9rem; }}
    .collection-heading h2 {{ margin: 0; font: normal 1.6rem Georgia, serif; }}
    .result-count {{ margin: 0; color: var(--muted); font: .68rem "Courier New", monospace; }}
    .paper-list {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 340px), 1fr)); gap: 1.6rem; align-items: start; }}
    .paper-card {{ --tab: #d9e1cf; position: relative; min-width: 0; padding: 1.5rem; border: 1px solid #d6d0bf; background: var(--paper); box-shadow: 2px 3px 0 #e1ddcf, 2px 4px 0 #cbc6b6, 0 8px 16px #554e3510; transition: box-shadow .18s ease; }}
    .paper-card:nth-child(3n + 2) {{ --tab: #eadcc7; }}
    .paper-card:nth-child(3n) {{ --tab: #d9e2e6; }}
    .paper-card::before {{ content: "READING NOTE / PDF"; display: block; padding-bottom: .8rem; margin-bottom: 1.1rem; border-bottom: 1px solid #dedbcd; color: #777a69; font: .6rem "Courier New", monospace; letter-spacing: .1em; }}
    .paper-card::after {{ content: ""; position: absolute; top: -7px; right: 20px; width: 13px; height: 30px; border: 2px solid #899084; border-radius: 8px; transform: rotate(9deg); }}
    .paper-card:hover {{ box-shadow: 2px 3px 0 #e1ddcf, 2px 4px 0 #b9b49f, 0 10px 20px #554e3520; }}
    .paper-category {{ display: inline-block; max-width: 100%; margin: 0 0 1rem; padding: .35rem .6rem; background: var(--tab); color: #4d594c; font: .65rem/1.5 "Courier New", monospace; overflow-wrap: anywhere; }}
    .paper-card h2 {{ margin: 0; font: normal 1.5rem/1.3 Georgia, serif; letter-spacing: -.025em; overflow-wrap: anywhere; }}
    .filename {{ margin: .7rem 0 1.2rem; color: var(--muted); font: .67rem/1.7 "Courier New", monospace; overflow-wrap: anywhere; }}
    section {{ margin-top: 1.2rem; }}
    h3 {{ margin: 0 0 .6rem; color: #73765d; font: bold .62rem "Courier New", monospace; letter-spacing: .1em; text-transform: uppercase; }}
    section p, section ul {{ margin: 0; font-size: .88rem; line-height: 1.85; overflow-wrap: anywhere; }}
    ul {{ padding-left: 1.1rem; }}
    li + li {{ margin-top: .35rem; }}
    .keywords ul {{ display: flex; flex-wrap: wrap; gap: .4rem; padding: 0; list-style: none; }}
    .keywords li {{ margin: 0; padding: .1rem .35rem; background: #f0ecd6; color: #636347; font: .68rem/1.6 "Courier New", monospace; }}
    .resources {{ padding-top: 1rem; border-top: 1px dashed #ccc8b7; }}
    a {{ color: #3e644d; text-decoration-thickness: 1px; text-underline-offset: 3px; }}
    a:hover {{ color: #854c34; }}
    .muted {{ color: var(--muted); font: .7rem "Courier New", monospace; }}
    .empty-state {{ padding: 3rem 1.5rem; border: 1px dashed #a9ad98; background: #f8f6eb; text-align: center; font: italic 1.2rem/1.7 Georgia, serif; }}
    footer {{ display: flex; justify-content: space-between; flex-wrap: wrap; gap: 1rem; margin-top: 3rem; padding-top: 1.3rem; border-top: 1px solid #c9c6b8; color: #6d7262; font: .6rem/1.8 "Courier New", monospace; letter-spacing: .07em; }}
    [hidden] {{ display: none !important; }}
    @media (prefers-reduced-motion: reduce) {{ .paper-card {{ transition: none; }} }}
    @media (max-width: 700px) {{ .hero {{ padding: 0 1.25rem; }} .hero-content {{ grid-template-columns: 1fr; gap: 2rem; padding: 2rem 0; }} .desk-note {{ max-width: 260px; transform: none; }} h1 {{ font-size: clamp(3rem, 11vw, 4.5rem); }} main {{ width: calc(100% - 2.5rem); }} .toolbar {{ grid-template-columns: 1fr; gap: 1.2rem; padding: 1.1rem; }} .status {{ max-width: 110px; text-align: right; line-height: 1.5; }} }}
  </style>
</head>
<body>
  <header class="hero">
    <div class="topbar">
      <div class="brand"><span class="crest" aria-hidden="true"></span> FIELDNOTES / RESEARCH LIBRARY</div>
      <span class="status">PERSONAL RESEARCH LIBRARY</span>
    </div>
    <div class="hero-content">
      <div>
        <p class="eyebrow">Notes, references &amp; things worth keeping</p>
        <h1>A desk for<br><span>curious minds.</span></h1>
        <p class="intro">Your papers, gathered in one quiet corner. Browse your reading notes, follow a reference, and pick up where curiosity left off.</p>
      </div>
      <aside class="desk-note" aria-label="Collection size">
        <span class="note-caption">ON THE RESEARCH DESK</span>
        <strong>{len(papers):02d}</strong>
        <span class="note-caption">PAPERS FILED &amp; READY TO EXPLORE</span>
        <p>Good ideas start<br>with a little curiosity.</p>
      </aside>
    </div>
  </header>
  <main>
    <div class="toolbar" aria-label="Paper filters">
      <label for="paper-search">01 / Search the archive
        <input id="paper-search" type="search" placeholder="Search title, filename, summary, or keywords" autocomplete="off">
      </label>
      <label for="category-filter">02 / Choose a discipline
        <select id="category-filter">
          <option value="">All categories</option>
          {category_options}
        </select>
      </label>
    </div>
    <div class="collection-heading"><h2>The reading pile</h2><p class="result-count" id="result-count" aria-live="polite"></p></div>
    <div class="paper-list" id="paper-list">{paper_cards}</div>
    {empty_message}
    <footer><span>FIELDNOTES / RESEARCH LIBRARY &mdash; RESEARCH LIBRARY</span><span>KEEP READING. KEEP CONNECTING.</span></footer>
  </main>
  <script>
    const searchInput = document.getElementById("paper-search");
    const categoryFilter = document.getElementById("category-filter");
    const cards = Array.from(document.querySelectorAll(".paper-card"));
    const resultCount = document.getElementById("result-count");
    const emptyState = document.getElementById("empty-state");

    function updateResults() {{
      const query = searchInput.value.trim().toLocaleLowerCase();
      const category = categoryFilter.value;
      let visibleCount = 0;

      for (const card of cards) {{
        const matchesText = card.dataset.search.toLocaleLowerCase().includes(query);
        const matchesCategory = !category || card.dataset.category === category;
        const visible = matchesText && matchesCategory;
        card.hidden = !visible;
        if (visible) visibleCount += 1;
      }}

      resultCount.textContent = `Showing ${{visibleCount}} of ${{cards.length}} papers`;
      emptyState.hidden = visibleCount > 0;
    }}

    searchInput.addEventListener("input", updateResults);
    categoryFilter.addEventListener("change", updateResults);
    updateResults();
  </script>
</body>
</html>
"""


def write_paper_catalog(
    papers: Sequence[Mapping[str, object]], output_path: Path
) -> Path:
    """Write a standalone catalog page, replacing the previous page atomically."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=output_path.parent,
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            temporary_file.write(_render_page(papers))
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.replace(temporary_path, output_path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()
    return output_path
