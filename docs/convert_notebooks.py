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

import json
import re
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
NOTEBOOKS_DIR = REPO_ROOT / "docs" / "content" / "examples"
OUTPUT_DIR = REPO_ROOT / "docs" / "content" / "examples"

# Map notebook stem → (Zensical icon, list of tags)
NOTEBOOK_META: dict[str, tuple[str, list[str]]] = {
    "example_electric_vehicle_premise_simple": (
        "lucide/car-front",
        ["example", "premise", "temporal distribution"],
    ),
    "example_electric_vehicle_premise": (
        "lucide/car-front",
        ["example", "premise", "dynamic characterization"],
    ),
    "example_simple_dynamic_characterization": (
        "lucide/trending-up",
        ["example", "dynamic characterization"],
    ),
    "paper_case_study": (
        "lucide/file-text",
        ["example", "paper", "premise"],
    ),
    "example_Importing_model_from_excel": (
        "lucide/table",
        ["example", "excel", "temporal distribution"],
    ),
}

NOTEBOOK_SOURCE_PATHS: dict[str, str] = {
    stem: f"docs/content/examples/{stem}.ipynb" for stem in NOTEBOOK_META
}

ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[mK]")
# Pandas wraps its <table> in <div><style scoped>...</style>...</div>, but the
# closing </div> isn't always present in nbconvert's markdown output (depends
# on the dataframe/output shape) - matching both tags in one regex silently
# fails to match when the closing tag is missing, leaving an unclosed <div>
# and an unscoped raw <style> block in the page, corrupting everything after
# it. Strip the opening preamble and any (optional) closing </div> separately
# instead, so each half is stripped independently of whether the other is
# present.
PANDAS_TABLE_STYLE_OPEN = re.compile(r"<div>\s*<style scoped>.*?</style>\s*", re.DOTALL)
# Large/truncated dataframes get a "<p>N rows x M columns</p>" notice between
# </table> and the wrapper's </div> - tolerate it so the </div> strip doesn't
# silently miss it (found the hard way: a missed strip here left an orphaned
# </div> that closed an unrelated ancestor early, corrupting page layout).
PANDAS_TABLE_STYLE_CLOSE = re.compile(
    r"(</table>\s*(?:<p>[\d,]+ rows [x×] [\d,]+ columns</p>\s*)?)</div>", re.DOTALL
)

# An nbconvert output line: the 4-space indent that makes Markdown treat it as
# a code block, plus something other than whitespace on it.
INDENTED_OUTPUT_LINE = re.compile(r"^ {4}.*\S")


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE.sub("", text)


def tighten_output_blocks(body: str) -> str:
    """Drop the blank lines nbconvert leaves between a cell's output blocks.

    Outputs of different types (a stream and an execute_result, say) can't be
    merged in the notebook, so nbconvert emits them as separate indented blocks
    separated by blank lines - and Markdown folds adjacent indented blocks into
    one code block, turning each separator into an empty line inside the output.
    Extracted images leave whitespace-only indented lines behind the same way,
    which render as an empty code block. Removing every blank line that sits
    between two output lines makes a cell's outputs one contiguous block, as in
    Jupyter.

    Fenced blocks are skipped, so indented content inside them (the Mermaid
    diagrams in the intro cells) is left untouched.
    """
    lines = body.split("\n")
    result: list[str] = []
    in_fence = False
    index = 0

    while index < len(lines):
        line = lines[index]

        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            result.append(line)
            index += 1
            continue

        if in_fence or line.strip():
            result.append(line)
            index += 1
            continue

        # A run of blank (or whitespace-only) lines: keep a single empty line,
        # unless output lines sit on both sides of it.
        end = index
        while end < len(lines) and not lines[end].strip():
            end += 1

        previous = result[-1] if result else ""
        following = lines[end] if end < len(lines) else ""
        if not (
            INDENTED_OUTPUT_LINE.match(previous)
            and INDENTED_OUTPUT_LINE.match(following)
        ):
            result.append("")
        index = end

    return "\n".join(result)


def merge_stream_outputs(notebook) -> None:
    """Merge consecutive stream outputs of a cell into a single output.

    Loguru (and anything else that flushes per line) produces one `stream`
    output per line. nbconvert renders each output as its own indented block,
    joined by a blank line, and Markdown then folds those adjacent indented
    blocks into one code block full of blank lines - the docs pages end up
    double-spaced wherever log messages appear. Concatenating the text of
    adjacent stream outputs (stdout and stderr alike, they render identically)
    collapses them into one block.
    """
    for cell in notebook.get("cells", []):
        outputs = cell.get("outputs")
        if not outputs:
            continue

        merged = []
        for output in outputs:
            if (
                output.get("output_type") == "stream"
                and merged
                and merged[-1].get("output_type") == "stream"
            ):
                previous = merged[-1]
                previous_text = previous["text"]
                if isinstance(previous_text, list):
                    previous_text = "".join(previous_text)
                text = output["text"]
                if isinstance(text, list):
                    text = "".join(text)
                if previous_text and not previous_text.endswith("\n"):
                    previous_text += "\n"
                previous["text"] = previous_text + text
            else:
                merged.append(output)

        cell["outputs"] = merged


def collapse_hidden_input_cells(body: str, notebook_path: Path) -> str:
    """Fold code cells tagged `hide-input` into a collapsed admonition.

    `jupyter.source_hidden` only collapses a cell inside JupyterLab; nbconvert
    renders it as an ordinary code block, so a setup cell the reader is meant to
    skip would dominate the page. Wrap those cells in a `???` block instead,
    which Zensical renders collapsed. The admonition title comes from the cell's
    `docs_summary` metadata.
    """
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))

    for cell in notebook["cells"]:
        metadata = cell.get("metadata", {})
        if cell["cell_type"] != "code" or "hide-input" not in metadata.get("tags", []):
            continue

        source = "".join(cell["source"])
        code_block = f"```python\n{source}\n```"
        if code_block not in body:
            print(
                f"WARNING: could not fold the hide-input cell of {notebook_path.name} "
                "- its exported code block was not found verbatim",
                file=sys.stderr,
            )
            continue

        title = metadata.get("docs_summary", "Show the code")
        indented = "\n".join(
            f"    {line}" if line.strip() else "" for line in code_block.split("\n")
        )
        body = body.replace(code_block, f'??? note "{title}"\n\n{indented}')

    return body


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

    import nbformat

    notebook = nbformat.read(str(notebook_path), as_version=4)
    merge_stream_outputs(notebook)

    exporter = MarkdownExporter()
    # from_notebook_node instead of from_filename so the merged outputs above
    # are what gets exported. Leaving resources["unique_key"] unset keeps the
    # extracted image names at output_<cell>_<index>.png, as before.
    body, resources = exporter.from_notebook_node(
        notebook, resources={"metadata": {"path": str(notebook_path.parent)}}
    )

    # Strip ANSI codes
    body = strip_ansi(body)

    # Squeeze out the blank lines between a cell's output blocks. Runs before
    # the hide-input folding below, whose indented admonition bodies rely on
    # their own blank lines.
    body = tighten_output_blocks(body)

    # Fold cells the notebook collapses in Jupyter (e.g. bulk setup code)
    body = collapse_hidden_input_cells(body, notebook_path)

    # Pandas exports DataFrames wrapped in a <div> with an inline <style scoped>
    # block. Unwrap down to the bare <table>, matching what the Markdown table
    # extension itself emits - the theme's own JS wraps every <table> in
    # .md-typeset__scrollwrap/.md-typeset__table at runtime, so pre-wrapping it
    # here would leave it double-wrapped and mis-aligned.
    body = PANDAS_TABLE_STYLE_OPEN.sub("", body)
    body = PANDAS_TABLE_STYLE_CLOSE.sub(r"\1", body)

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

    # Do NOT wrap the body or tag fences to scope the primary-color code-cell
    # highlight (see docs/stylesheets/extra.css) - both were tried and both
    # corrupt Zensical's rendering: a wrapping <div markdown> breaks
    # md_in_html's block nesting around the raw <table>, and tagging fences
    # with superfences' inline "{ .class }" attribute syntax corrupts
    # Zensical's renderer entirely (confirmed broken on mobile, live).
    # Scoping is done via JS instead - see docs/javascripts/source-overrides.js.
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
