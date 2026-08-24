"""Add stable aliases for case-study documentation URLs after the site build."""

from pathlib import Path


SITE = Path("site")
PREFIX = "/projects/bw-timex/en/latest/content/examples"
CASE_STUDIES = (
    "electric_vehicle_premise",
    "electric_vehicle_premise_detailed",
    "paper_case_study",
)


def write_redirect(path: Path, target: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "<!doctype html>\n"
        '<html><head>\n'
        f'<meta http-equiv="refresh" content="0; url={target}">\n'
        f'<link rel="canonical" href="{target}">\n'
        "</head><body>\n"
        f'<p>This page has moved to <a href="{target}">{target}</a>.</p>\n'
        "</body></html>\n",
        encoding="utf-8",
    )


def main() -> None:
    if not SITE.exists():
        raise SystemExit("site directory does not exist; run zensical build first")

    # Stable .html URL cited by the paper.
    paper_index = SITE / "content" / "examples" / "paper_case_study" / "index.html"
    if paper_index.exists():
        write_redirect(
            SITE / "content" / "examples" / "paper_case_study.html",
            f"{PREFIX}/paper_case_study/",
        )

    # Keep old double-"examples" URLs working for existing bookmarks.
    for stem in CASE_STUDIES:
        current = SITE / "content" / "examples" / stem / "index.html"
        if current.exists():
            write_redirect(
                SITE / "content" / "examples" / "examples" / stem / "index.html",
                f"{PREFIX}/{stem}/",
            )


if __name__ == "__main__":
    main()
