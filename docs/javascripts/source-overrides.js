function applyNotebookSourceOverrides() {
  const override = document.querySelector("[data-source-edit-path][data-source-view-path]");
  if (!override) return;

  // Mark this page as notebook-derived so the primary-color code-cell
  // highlight (see docs/stylesheets/extra.css) applies only here, without
  // touching the Markdown source itself (see docs/convert_notebooks.py for
  // why that's risky - an unrelated regex bug there once corrupted page
  // layout, which earlier looked like it was caused by this scoping choice).
  const content = document.querySelector(".md-content__inner");
  if (content) {
    content.classList.add("notebook-page");
  }

  const editPath = override.getAttribute("data-source-edit-path");
  const viewPath = override.getAttribute("data-source-view-path");
  if (!editPath || !viewPath) return;

  const editButton = document.querySelector('.md-content__button[rel="edit"]');
  if (editButton) {
    editButton.href = `https://github.com/brightway-lca/bw_timex/edit/main/${editPath}`;
  }

  const viewButton = document.querySelector(
    '.md-content__button:not([rel="edit"])[title="View source of this page"]'
  );
  if (viewButton) {
    viewButton.href = `https://github.com/brightway-lca/bw_timex/blob/main/${viewPath}`;
  }
}

document.addEventListener("DOMContentLoaded", applyNotebookSourceOverrides);
document$.subscribe(applyNotebookSourceOverrides);
