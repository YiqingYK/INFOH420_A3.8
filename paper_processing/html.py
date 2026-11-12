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
        f'<section><h3>Keywords</h3>{_render_values(keywords)}</section>'
        f'<section><h3>Code URLs</h3>{_render_urls(row.get("code_urls"))}</section>'
        f'<section><h3>Dataset URLs</h3>{_render_urls(row.get("dataset_urls"))}</section>'
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
        '<p class="empty-state" id="empty-state">No papers match your filters.</p>'
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light">
  <title>Research Paper Library</title>
  <style>
    :root {{
      color-scheme: light;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: #172033;
      background: #f4f6fb;
      font-synthesis: none;
      text-rendering: optimizeLegibility;
      --accent: #3156a4;
      --muted: #64708a;
      --border: #e2e7f0;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; min-width: 320px; }}
    .hero {{
      padding: 3.5rem max(1.25rem, calc((100% - 1120px) / 2));
      color: white;
      background: linear-gradient(125deg, #172b52, #3156a4 68%, #5278bd);
    }}
    .eyebrow {{ margin: 0 0 .65rem; color: #c9d8fa; font-size: .78rem; font-weight: 700; letter-spacing: .13em; text-transform: uppercase; }}
    h1 {{ margin: 0; font-size: clamp(2rem, 5vw, 3.25rem); letter-spacing: -.04em; }}
    .hero p:last-child {{ max-width: 620px; margin: .8rem 0 0; color: #e0e8fa; line-height: 1.65; }}
    main {{ width: min(1120px, calc(100% - 2.5rem)); margin: 2rem auto 4rem; }}
    .toolbar {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(210px, .42fr);
      gap: 1rem;
      padding: 1.25rem;
      border: 1px solid var(--border);
      border-radius: 16px;
      background: white;
      box-shadow: 0 8px 26px rgb(34 51 84 / 6%);
    }}
    label {{ display: grid; gap: .45rem; color: #36435b; font-size: .86rem; font-weight: 650; }}
    input, select {{
      width: 100%;
      min-height: 46px;
      padding: .7rem .85rem;
      border: 1px solid #cbd3e1;
      border-radius: 9px;
      color: #172033;
      background: white;
      font: inherit;
    }}
    input:focus, select:focus, a:focus-visible {{ outline: 3px solid #94b5ff; outline-offset: 2px; }}
    .result-count {{ margin: 1.2rem .15rem; color: var(--muted); font-size: .9rem; }}
    .paper-list {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 340px), 1fr)); gap: 1rem; align-items: start; }}
    .paper-card {{
      min-width: 0;
      padding: 1.35rem;
      border: 1px solid var(--border);
      border-radius: 14px;
      background: white;
      box-shadow: 0 5px 20px rgb(34 51 84 / 5%);
    }}
    .paper-category {{ display: inline-block; margin: 0 0 .7rem; padding: .28rem .65rem; border-radius: 99px; color: #294d94; background: #eaf0ff; font-size: .76rem; font-weight: 700; }}
    h2 {{ margin: 0; font-size: 1.25rem; line-height: 1.4; overflow-wrap: anywhere; }}
    .filename {{ margin: .35rem 0 1.15rem; color: var(--muted); font-size: .87rem; overflow-wrap: anywhere; }}
    section {{ margin-top: 1rem; }}
    h3 {{ margin: 0 0 .35rem; color: #46536c; font-size: .77rem; letter-spacing: .07em; text-transform: uppercase; }}
    section p, section ul {{ margin: 0; color: #38445a; font-size: .92rem; line-height: 1.6; overflow-wrap: anywhere; }}
    ul {{ padding-left: 1.2rem; }}
    li + li {{ margin-top: .2rem; }}
    a {{ color: var(--accent); text-decoration-thickness: 1px; text-underline-offset: 3px; }}
    .muted, .empty-state {{ color: var(--muted); }}
    .empty-state {{ padding: 2rem; border: 1px dashed #cbd3e1; border-radius: 14px; text-align: center; background: white; }}
    [hidden] {{ display: none !important; }}
    @media (max-width: 620px) {{
      .hero {{ padding-top: 2.5rem; padding-bottom: 2.5rem; }}
      .toolbar {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <header class="hero">
    <p class="eyebrow">Research library</p>
    <h1>Explore the papers</h1>
    <p>Browse paper summaries, keywords, categories, and related code and dataset links.</p>
  </header>
  <main>
    <div class="toolbar" aria-label="Paper filters">
      <label for="paper-search">Search papers
        <input id="paper-search" type="search" placeholder="Search title, filename, summary, or keywords" autocomplete="off">
      </label>
      <label for="category-filter">Category
        <select id="category-filter">
          <option value="">All categories</option>
          {category_options}
        </select>
      </label>
    </div>
    <p class="result-count" id="result-count" aria-live="polite"></p>
    <div class="paper-list" id="paper-list">{paper_cards}</div>
    {empty_message}
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
