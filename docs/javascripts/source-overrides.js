function applyNotebookSourceOverrides() {
  const override = document.querySelector("[data-source-edit-url][data-source-view-url]");
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

  // Full URLs, built at conversion time - they carry the git ref the docs were
  // built from, so a branch or PR preview links into that branch rather than
  // into main, where a renamed notebook may not exist (yet).
  const editUrl = override.getAttribute("data-source-edit-url");
  const viewUrl = override.getAttribute("data-source-view-url");
  if (!editUrl || !viewUrl) return;

  const editButton = document.querySelector('.md-content__button[rel="edit"]');
  if (editButton) {
    editButton.href = editUrl;
  }

  const viewButton = document.querySelector(
    '.md-content__button:not([rel="edit"])[title="View source of this page"]'
  );
  if (viewButton) {
    viewButton.href = viewUrl;
  }
}

// Loguru writes "<timestamp> | <LEVEL> | <module>:<function>:<line> - <message>".
const LOG_LINE = /^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[.,]\d+)(\s*\|\s*)([A-Z]+)(\s*\|\s*)([^\s|]+:[^\s|]+:\d+)(\s*-\s*)([\s\S]*)$/;
// Python's warnings module writes "<file>:<line>: <WarningClass>: <message>".
const WARNING_LINE = /^(\s*)(\S+):(\d+):(\s*)(\w*(?:Warning|Error)):(\s*)([\s\S]*)$/;

const LOG_LEVEL_MODIFIERS = {
  TRACE: "debug",
  DEBUG: "debug",
  INFO: "info",
  SUCCESS: "success",
  WARNING: "warning",
  ERROR: "error",
  CRITICAL: "error",
};

function escapeHtml(text) {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function highlightOutputLine(line) {
  const log = LOG_LINE.exec(line);
  if (log) {
    const [, time, sep1, level, sep2, origin, sep3, message] = log;
    const modifier = LOG_LEVEL_MODIFIERS[level];
    if (modifier) {
      return (
        `<span class="nb-log-time">${escapeHtml(time)}</span>${escapeHtml(sep1)}` +
        `<span class="nb-log-level nb-log-level--${modifier}">${escapeHtml(level)}</span>` +
        `${escapeHtml(sep2)}<span class="nb-log-origin">${escapeHtml(origin)}</span>` +
        `${escapeHtml(sep3)}<span class="nb-log-message">${escapeHtml(message)}</span>`
      );
    }
  }

  const warning = WARNING_LINE.exec(line);
  if (warning) {
    const [, indent, file, lineNumber, gap1, kind, gap2, message] = warning;
    const modifier = kind.endsWith("Error") ? "error" : "warning";
    return (
      `${escapeHtml(indent)}<span class="nb-log-origin">${escapeHtml(file)}:${lineNumber}</span>` +
      `:${escapeHtml(gap1)}` +
      `<span class="nb-log-level nb-log-level--${modifier}">${escapeHtml(kind)}</span>:` +
      `${escapeHtml(gap2)}<span class="nb-log-message">${escapeHtml(message)}</span>`
    );
  }

  return escapeHtml(line);
}

// Pygments' text lexer emits no tokens, so cell outputs render as one flat
// block of grey. Colour the parts of a log line the reader actually scans for
// (level, origin) in the same spirit as the code cells above, but with a
// quieter palette - see docs/stylesheets/extra.css.
function highlightNotebookOutputs() {
  const blocks = document.querySelectorAll(
    ".md-content__inner.notebook-page .language-text.highlight > pre > code"
  );

  blocks.forEach((block) => {
    if (block.dataset.nbLogHighlighted) return;
    block.dataset.nbLogHighlighted = "true";

    const text = block.textContent;
    if (!LOG_LINE.test(text.split("\n")[0]) && !/\n\d{4}-\d{2}-\d{2} /.test(text)) {
      // Nothing log-shaped in here (dataframe reprs, plain prints, ...) -
      // leave the block exactly as the theme rendered it.
      if (!WARNING_LINE.test(text)) return;
    }

    block.innerHTML = text.split("\n").map(highlightOutputLine).join("\n");
  });
}

function applyNotebookEnhancements() {
  applyNotebookSourceOverrides();
  highlightNotebookOutputs();
}

document.addEventListener("DOMContentLoaded", applyNotebookEnhancements);
document$.subscribe(applyNotebookEnhancements);
