"""Move generated case-study pages to their public documentation paths.

The notebook converter mirrors the notebook directory below docs/content/examples.
Case-study notebooks live in notebooks/examples/, which would otherwise produce
an unwanted docs/content/examples/examples/ path. Keep the notebook layout as-is
and normalize the generated pages after conversion instead.
"""

from pathlib import Path
import shutil


ROOT = Path(__file__).resolve().parent / "content" / "examples"
CASE_STUDIES = (
    "electric_vehicle_premise",
    "electric_vehicle_premise_detailed",
    "paper_case_study",
)


def main() -> None:
    generated = ROOT / "examples"
    if not generated.exists():
        return

    for stem in CASE_STUDIES:
        source = generated / f"{stem}.md"
        destination = ROOT / f"{stem}.md"
        if source.exists():
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(source, destination)

        source_files = generated / f"{stem}_files"
        destination_files = ROOT / f"{stem}_files"
        if source_files.exists():
            if destination_files.exists():
                shutil.rmtree(destination_files)
            shutil.move(source_files, destination_files)

        if destination.exists():
            body = destination.read_text(encoding="utf-8")
            # The generated pages moved one directory upwards. Adjust links to
            # shared example assets and sibling tutorial/advanced pages.
            body = body.replace("../data/", "data/")
            body = body.replace("../tutorials/", "tutorials/")
            body = body.replace("../advanced/", "advanced/")
            destination.write_text(body, encoding="utf-8")

    if generated.exists() and not any(generated.iterdir()):
        generated.rmdir()


if __name__ == "__main__":
    main()
