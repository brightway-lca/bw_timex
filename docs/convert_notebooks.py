"""Convert Jupyter notebooks in notebooks/ to Markdown for the docs.

For each notebook listed in NOTEBOOKS:
- Runs nbconvert (markdown output) into docs/content/examples/
- Strips ANSI escape sequences from every output cell
- Moves output images into a <stem>_files/ subdirectory
- Prepends YAML front-matter (icon + tags) required by Zensical

Run this script from the project root before building the docs:

    python docs/convert_notebooks.py
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
NOTEBOOKS_DIR = REPO_ROOT / "docs" / "content" / "examples"
OUTPUT_DIR = REPO_ROOT / "docs" / "content" / "examples"

# Map notebook stem → (Zensical icon, list of tags)
NOTEBOOK_META: dict[str, tuple[str, list[str]]] = {
    "example_electric_vehicle_premise": (
        "lucide/car-front",
        ["example", "premise"],
    ),
    "example_simple_dynamic_characterization": (
        "lucide/trending-up",
        ["example", "dynamic characterization"],
    ),
    "paper_case_study": (
        "lucide/file-text",
        ["case study", "paper"],
    ),
    "example_Importing_model_from_excel": (
        "lucide/table",
        ["example", "excel"],
    ),
}

NOTEBOOK_SOURCE_PATHS: dict[str, str] = {
    stem: f"docs/content/examples/{stem}.ipynb" for stem in NOTEBOOK_META
}

ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[mK]")
PANDAS_TABLE_STYLE = re.compile(
    r"<div>\s*<style scoped>.*?</style>\s*(<table.*?</table>)\s*</div>", re.DOTALL
)
NOTEBOOK_CODE_FENCE = re.compile(r"^```python$", re.MULTILINE)


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE.sub("", text)


def convert(
    notebook_path: Path, output_dir: Path, icon: str, tags: list[str]
) -> Path:
    """Convert *notebook_path* to Markdown in *output_dir*, strip ANSI codes,
    organise images into a <stem>_files/ sub-directory, and inject the Zensical
    icon and tags front-matter.  Returns the output file path."""
    try:
        from nbconvert.exporters import MarkdownExporter
    except ImportError:
        print(
            "nbconvert is not installed. "
            "Run: pip install nbconvert  (or add it to docs/requirements.txt)",
            file=sys.stderr,
        )
        sys.exit(1)

    stem = notebook_path.stem
    files_dir_name = f"{stem}_files"
    files_dir = output_dir / files_dir_name

    exporter = MarkdownExporter()
    body, resources = exporter.from_filename(str(notebook_path))

    # Strip ANSI codes
    body = strip_ansi(body)

    # Pandas exports DataFrames wrapped in a <div> with an inline <style scoped>
    # block. Unwrap down to the bare <table>, matching what the Markdown table
    # extension itself emits - the theme's own JS wraps every <table> in
    # .md-typeset__scrollwrap/.md-typeset__table at runtime, so pre-wrapping it
    # here would leave it double-wrapped and mis-aligned.
    body = PANDAS_TABLE_STYLE.sub(r"\1", body)

    # Keep dataframe tables readable on small screens without changing their
    # internal table layout.
    body = body.replace('<table border="1" class="dataframe">', '<table>')

    # Rewrite bare image refs  ![png](output_N_M.png)
    # → ![png](<stem>_files/output_N_M.png)
    body = re.sub(
        r"!\[([^\]]*)\]\((?!http)(output_[^)]+\.png)\)",
        rf"![\1]({files_dir_name}/\2)",
        body,
    )

    # Markdown-cell image attachments (e.g. ![image.png](image.png), pasted
    # into a cell rather than produced as a code-cell output) are exported by
    # nbconvert under their original filename rather than the output_N_M.png
    # convention above, so the regex above misses them. Every binary asset
    # nbconvert extracted is written under files_dir below; rewrite any bare
    # reference to one of those exact filenames to point there too.
    for output_filename in resources.get("outputs", {}):
        if output_filename.startswith("output_"):
            continue  # already handled above
        body = re.sub(
            rf"!\[([^\]]*)\]\((?!http){re.escape(output_filename)}\)",
            rf"![\1]({files_dir_name}/{output_filename})",
            body,
        )

    # Rewrite notebook-local data asset paths for rendered docs pages.
    # In notebooks, assets live at data/<file>; in the generated docs source,
    # they should remain relative to the examples section root. The builder
    # then rebases them correctly for /content/examples/<page>/ URLs.
    body = re.sub(r"src=(['\"])data/", r"src=\1data/", body)
    body = body.replace("](../data/", "](data/")

    source_path = NOTEBOOK_SOURCE_PATHS.get(stem)
    source_override = ""
    if source_path:
        source_override = (
            "\n"
            f'<div hidden data-source-edit-path="{source_path}" '
            f'data-source-view-path="{source_path}"></div>\n'
        )

    # Build YAML front-matter lines
    tags_yaml = "\n".join(f"  - {t}" for t in tags)
    frontmatter = f"---\nicon: {icon}\ntags:\n{tags_yaml}\n---\n\n"

    # Tag each Python code cell's fence with a class so the primary-color
    # code-cell highlight (see docs/stylesheets/extra.css) applies only to
    # notebook-derived pages, not regular Markdown pages with Python fences.
    # A wrapping <div> was used previously, but the theme bleeds code blocks
    # edge-to-edge via a direct-child selector (.md-content__inner>.highlight)
    # that a wrapper div breaks - tagging the fence itself avoids that.
    body = NOTEBOOK_CODE_FENCE.sub('```python { .notebook-cell }', body)
    body = frontmatter + source_override + body

    # Write markdown file
    md_path = output_dir / f"{stem}.md"
    md_path.write_text(body, encoding="utf-8")

    # Write image files into the _files/ subdirectory
    output_images = resources.get("outputs", {})
    if output_images:
        files_dir.mkdir(parents=True, exist_ok=True)
        for filename, data in output_images.items():
            (files_dir / filename).write_bytes(data)

    return md_path


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Clean up any stray image files left over from a previous run that wrote
    # images directly into OUTPUT_DIR (old behaviour of FilesWriter)
    for leftover in OUTPUT_DIR.glob("output_*.png"):
        leftover.unlink()

    for stem, (icon, tags) in NOTEBOOK_META.items():
        notebook_path = NOTEBOOKS_DIR / f"{stem}.ipynb"
        if not notebook_path.exists():
            print(f"WARNING: notebook not found: {notebook_path}", file=sys.stderr)
            continue
        out = convert(notebook_path, OUTPUT_DIR, icon, tags)
        print(f"Converted {notebook_path.name} → {out.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
