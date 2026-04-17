import { ContentReport, DownstreamWorkbook } from "./validators/content.js";
import { DriftReport } from "./validators/schema-drift.js";
import { FormulaCheckReport } from "./validators/formula-check.js";

export interface CombinedReport {
  generatedAt: string;
  content: ContentReport;
  schemaDrift: DriftReport;
}

export function toJsonReport(
  contentReport: ContentReport,
  driftReport: DriftReport,
  formulaReport?: FormulaCheckReport
): string {
  return JSON.stringify(
    { generatedAt: new Date().toISOString(), content: contentReport, schemaDrift: driftReport, formulaCheck: formulaReport },
    null, 2
  );
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/** Render a link with an external-link icon. Always underlined and blue. */
function externalLink(url: string, text: string, extraClass = ""): string {
  return `<a href="${escapeHtml(url)}" target="_blank" rel="noopener" class="ext-link ${extraClass}">${escapeHtml(text)}<svg class="ext-icon" viewBox="0 0 12 12" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M1 11L11 1M11 1H4.5M11 1V7.5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg></a>`;
}

function groupByFolder(workbooks: DownstreamWorkbook[]): Map<string, DownstreamWorkbook[]> {
  const map = new Map<string, DownstreamWorkbook[]>();
  for (const wb of workbooks) {
    const folder = wb.folder || "Root";
    if (!map.has(folder)) map.set(folder, []);
    map.get(folder)!.push(wb);
  }
  return map;
}

function renderWorkbookList(wbs: DownstreamWorkbook[]): string {
  const byFolder = groupByFolder(wbs);
  return Array.from(byFolder.entries()).map(([folder, items]) => {
    const links = items.map((wb) => {
      const link = wb.url ? externalLink(wb.url, wb.name) : escapeHtml(wb.name);
      return `<li>${link}</li>`;
    }).join("\n");
    return `<div class="folder-group"><div class="folder-name">📁 ${escapeHtml(folder)}</div><ul>${links}</ul></div>`;
  }).join("\n");
}

function renderContentSection(contentReport: ContentReport): string {
  // Build model name lookup for dependency labels
  const nameById = new Map(contentReport.models.map((m) => [m.modelId, m.modelName]));

  const rows = contentReport.models.map((model, idx) => {
    const directCount = model.downstreamWorkbooks.length;
    const transitiveCount = model.transitiveWorkbooks.length;
    const totalCount = directCount + transitiveCount;
    const detailId = `content-detail-${idx}`;

    // Upstream models this model depends on
    const upstreamIds = contentReport.modelDependencies[model.modelId] ?? [];
    const upstreamHtml = upstreamIds.length > 0
      ? `<div class="dep-chain">Sources: ${upstreamIds.map((id) => {
          const name = nameById.get(id) ?? id;
          return `<span class="dep-badge">${escapeHtml(name)}</span>`;
        }).join(" → ")}</div>`
      : "";

    let detailHtml = "";
    if (directCount > 0 || transitiveCount > 0) {
      detailHtml = `<tr id="${detailId}" class="detail-row hidden"><td colspan="5">
        <div class="folder-container">
          ${directCount > 0 ? `<div class="wb-section-label">Direct (${directCount})</div>${renderWorkbookList(model.downstreamWorkbooks)}` : ""}
          ${transitiveCount > 0 ? `<div class="wb-section-label wb-section-transitive">Via downstream models (${transitiveCount})</div>${renderWorkbookList(model.transitiveWorkbooks)}` : ""}
        </div>
      </td></tr>`;
    }

    const toggleBtn = totalCount > 0
      ? `<button class="toggle-btn" onclick="toggle('${detailId}')">▶ Show workbooks</button>`
      : "";

    const modelCell = model.modelUrl
      ? externalLink(model.modelUrl, model.modelName, "model-link")
      : `<span class="model-name-plain">${escapeHtml(model.modelName)}</span>`;

    const transitiveCell = transitiveCount > 0
      ? `<span class="transitive-count" title="Via downstream model chain">+${transitiveCount} transitive</span>`
      : "";

    return `
    <tr>
      <td>${modelCell}${upstreamHtml}</td>
      <td class="${directCount > 0 ? "count-nonzero" : ""}">${directCount}</td>
      <td class="count-transitive">${transitiveCell}</td>
      <td>${toggleBtn}</td>
    </tr>
    ${detailHtml}`;
  }).join("\n");

  // Show dependency graph summary if any exist
  const depCount = Object.keys(contentReport.modelDependencies).length;
  const depSummary = depCount > 0
    ? `<p class="dep-summary">⛓ ${depCount} model(s) depend on other models — transitive blast radius shown above.</p>`
    : "";

  return `
  <section>
    <h2>Content Validation — Downstream Blast Radius</h2>
    ${depSummary}
    <table>
      <thead><tr><th>Model Name</th><th>Direct Workbooks</th><th>Transitive</th><th></th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </section>`;
}

function renderDriftSection(driftReport: DriftReport, sessionId?: string): string {
  const modelBlocks = driftReport.models.map((model) => {
    const totalMissing = model.tables.reduce((sum, t) => sum + t.missingColumns.length, 0);
    const statusBadge = totalMissing === 0
      ? `<span class="badge badge-ok">✓ OK</span>`
      : `<span class="badge badge-error">⚠ ${totalMissing} missing column${totalMissing !== 1 ? "s" : ""}</span>`;

    const fixBtn = totalMissing > 0 && sessionId
      ? `<button class="fix-btn" onclick="fixDrift('${escapeHtml(model.modelId)}', '${escapeHtml(model.modelName)}', ${totalMissing})">
           Fix: Remove ${totalMissing} missing column${totalMissing !== 1 ? "s" : ""}
         </button>`
      : "";

    const titleHtml = model.modelUrl
      ? externalLink(model.modelUrl, model.modelName, "model-title-link")
      : `<span class="model-title">${escapeHtml(model.modelName)}</span>`;

    const tableRows = model.tables.map((t) => {
      const missingHtml = t.missingColumns.length > 0
        ? t.missingColumns.map((c) => `<span class="missing-col">${escapeHtml(c)}</span>`).join(" ")
        : `<span class="ok-text">none</span>`;

      // Debug detail — shown when there are missing columns to help diagnose comparison issues
      const debugHtml = t.missingColumns.length > 0 ? (() => {
        const refAll = t.referencedColumns.map(c => `<code>${escapeHtml(c)}</code>`).join(", ");
        const whAll  = t.actualColumns.map(c => `<code>${escapeHtml(c)}</code>`).join(", ");
        return `<details class="drift-debug"><summary>🔍 Debug: compare column names</summary>
          <div class="drift-debug-body">
            <div><strong>Spec extracted (${t.referencedColumns.length}):</strong> ${refAll}</div>
            <div><strong>Warehouse returned (${t.actualColumns.length}):</strong> ${whAll || '<em>none</em>'}</div>
          </div>
        </details>`;
      })() : "";

      return `
      <tr>
        <td><code>${escapeHtml(t.tableName)}</code>${debugHtml}</td>
        <td class="count-small">${t.referencedColumns.length}</td>
        <td class="count-small">${t.actualColumns.length}</td>
        <td>${missingHtml}</td>
      </tr>`;
    }).join("\n");

    return `
    <div class="model-card ${totalMissing > 0 ? "model-card-error" : ""}" id="drift-card-${escapeHtml(model.modelId)}">
      <div class="model-header">
        ${titleHtml}
        <code class="model-id">${escapeHtml(model.modelId)}</code>
        ${statusBadge}
        ${fixBtn}
      </div>
      ${model.tables.length > 0
        ? `<table class="inner-table">
          <thead><tr><th>Table</th><th>Referenced Cols</th><th>Warehouse Cols</th><th>Missing Columns</th></tr></thead>
          <tbody>${tableRows}</tbody>
        </table>`
        : `<p class="no-elements">No direct warehouse table connections.</p>`}
    </div>`;
  }).join("\n");

  return `
  <section>
    <h2>Schema Drift Validation</h2>
    ${modelBlocks}
  </section>`;
}

function renderFormulaSection(formulaReport: FormulaCheckReport, sessionId?: string): string {
  const modelsWithIssues = formulaReport.models.filter((m) => m.totalBroken > 0);

  if (modelsWithIssues.length === 0) {
    return `
  <section>
    <h2>Broken Formula References</h2>
    <p class="no-elements" style="background:#f0fff4;border-radius:8px;padding:14px 18px;color:#276749;">✓ No broken formula references detected across all models.</p>
  </section>`;
  }

  const modelBlocks = modelsWithIssues.map((model) => {
    const titleHtml = model.modelUrl
      ? externalLink(model.modelUrl, model.modelName, "model-title-link")
      : `<span class="model-title">${escapeHtml(model.modelName)}</span>`;

    const elementBlocks = model.elements.map((el) => {
      const rows = el.brokenColumns.map((col) => {
        const colLabel = col.columnName
          ? `<strong>${escapeHtml(col.columnName)}</strong>`
          : `<code class="model-id">${escapeHtml(col.columnId)}</code>`;
        const typeTag = col.isMetric
          ? `<span class="formula-type-badge">metric</span>`
          : `<span class="formula-type-badge formula-type-col">column</span>`;

        const refBadges = col.brokenRefs.map((r) => {
          const fixBtnHtml = r.suggestion && sessionId
            ? `<button class="fix-formula-btn"
                 onclick="fixFormula(${JSON.stringify(model.modelId)}, ${JSON.stringify(model.modelName)}, ${JSON.stringify(el.elementId)}, ${JSON.stringify(col.columnId)}, ${JSON.stringify(r.ref)}, ${JSON.stringify(r.suggestion)})"
               >→ ${escapeHtml(r.suggestion)}</button>`
            : r.suggestion
              ? `<span class="formula-suggestion" title="Re-run with session to enable fix">→ ${escapeHtml(r.suggestion)}</span>`
              : `<span class="formula-no-suggestion">no suggestion</span>`;

          return `<span class="broken-ref-group">
            <span class="missing-col">[${escapeHtml(r.ref)}]</span>
            ${fixBtnHtml}
          </span>`;
        }).join(" ");

        return `
        <tr>
          <td>${colLabel} ${typeTag}</td>
          <td><code class="formula-code">${escapeHtml(col.formula)}</code></td>
          <td>${refBadges}</td>
        </tr>`;
      }).join("\n");

      return `
      <div class="formula-element">
        <div class="formula-element-name">📋 ${escapeHtml(el.elementName ?? el.elementId)}</div>
        <table class="inner-table">
          <thead><tr><th>Column / Metric</th><th>Formula</th><th>Broken References → Suggestion</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;
    }).join("\n");

    return `
    <div class="model-card model-card-error" id="formula-card-${escapeHtml(model.modelId)}">
      <div class="model-header">
        ${titleHtml}
        <code class="model-id">${escapeHtml(model.modelId)}</code>
        <span class="badge badge-error">⚠ ${model.totalBroken} broken reference${model.totalBroken !== 1 ? "s" : ""}</span>
      </div>
      ${elementBlocks}
    </div>`;
  }).join("\n");

  return `
  <section>
    <h2>Broken Formula References</h2>
    <p class="dep-summary" style="border-color:#fed7d7;background:#fff5f5;color:#9b2c2c;">
      ⚠ ${modelsWithIssues.length} model${modelsWithIssues.length !== 1 ? "s" : ""} with broken column references in formulas.
      These occur when a referenced column has been renamed or removed.
    </p>
    ${modelBlocks}
  </section>`;
}

export function toHtmlReport(
  contentReport: ContentReport,
  driftReport: DriftReport,
  options?: { sessionId?: string; formulaReport?: FormulaCheckReport }
): string {
  const generatedAt = new Date().toISOString();
  const sessionId = options?.sessionId;
  const formulaReport = options?.formulaReport;

  const fixScript = sessionId ? `
    var SESSION_ID = ${JSON.stringify(sessionId)};

    function fixDrift(modelId, modelName, count) {
      if (!confirm('Remove ' + count + ' missing column(s) from "' + modelName + '"?\\n\\nThis updates the data model in Sigma to remove columns that no longer exist in the warehouse. This action cannot be undone.')) return;
      var card = document.getElementById('drift-card-' + modelId);
      var btn = card ? card.querySelector('.fix-btn') : null;
      if (btn) { btn.disabled = true; btn.textContent = 'Fixing…'; }

      fetch('/api/fix', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionId: SESSION_ID, modelId: modelId }),
      })
      .then(function(r) { return r.json(); })
      .then(function(result) {
        if (!card) return;
        if (result.success) {
          var removed = result.removals.flatMap(function(r) { return r.columnsRemoved; });
          card.classList.remove('model-card-error');
          card.innerHTML = '<div class="model-header">' +
            '<span class="model-title" style="color:#276749">✓ Fixed: ' + escapeEl(modelName) + '</span>' +
            '<span class="badge badge-ok">Removed ' + removed.length + ' column' + (removed.length !== 1 ? 's' : '') + '</span>' +
            '</div>' +
            '<p class="no-elements" style="color:#4a5568">Removed: ' + removed.map(escapeEl).join(', ') + '</p>';
        } else {
          if (btn) { btn.disabled = false; btn.textContent = 'Retry fix'; }
          alert('Fix failed: ' + (result.error || 'Unknown error'));
        }
      })
      .catch(function(e) {
        if (btn) { btn.disabled = false; btn.textContent = 'Retry fix'; }
        alert('Fix failed: ' + e.message);
      });
    }

    function escapeEl(s) {
      return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    function fixFormula(modelId, modelName, elementId, columnId, brokenRef, newRef) {
      if (!confirm('Replace [' + brokenRef + '] with [' + newRef + '] in "' + modelName + '"?')) return;
      var btn = event.target;
      btn.disabled = true;
      btn.textContent = 'Fixing…';

      fetch('/api/fix-formula', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionId: SESSION_ID, modelId, elementId, columnId, brokenRef, newRef }),
      })
      .then(function(r) { return r.json(); })
      .then(function(result) {
        if (result.success) {
          var group = btn.closest('.broken-ref-group');
          if (group) {
            group.innerHTML = '<span class="badge-ok" style="border-radius:4px;padding:1px 8px;font-size:0.78rem;">' +
              '✓ [' + escapeEl(newRef) + ']</span>';
          }
        } else {
          btn.disabled = false;
          btn.textContent = '→ ' + escapeEl(newRef);
          alert('Fix failed: ' + (result.error || 'Unknown error'));
        }
      })
      .catch(function(e) {
        btn.disabled = false;
        btn.textContent = '→ ' + escapeEl(newRef);
        alert('Fix failed: ' + e.message);
      });
    }
  ` : '';

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Sigma CI Report</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f5f7fa; color: #1a202c; margin: 0; padding: 24px; }
    h1 { font-size: 1.8rem; margin-bottom: 4px; }
    .subtitle { color: #718096; font-size: 0.9rem; margin-bottom: 32px; }
    section { margin-bottom: 40px; }
    h2 { font-size: 1.25rem; border-bottom: 2px solid #e2e8f0; padding-bottom: 8px; }
    table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    th { background: #edf2f7; text-align: left; padding: 10px 14px; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; color: #4a5568; }
    td { padding: 10px 14px; border-top: 1px solid #e2e8f0; vertical-align: top; }
    tr:hover td { background: #fafafa; }
    .detail-row:hover td { background: #fff; }
    .hidden { display: none; }
    /* External links — always visible as links */
    .ext-link { color: #2b6cb0; text-decoration: underline; text-decoration-color: rgba(43,108,176,0.4); text-underline-offset: 2px; display: inline-flex; align-items: center; gap: 3px; font-weight: 500; }
    .ext-link:hover { color: #1a56db; text-decoration-color: #1a56db; }
    .ext-icon { width: 10px; height: 10px; flex-shrink: 0; opacity: 0.75; }
    .model-title-link { font-weight: 700; font-size: 1rem; }
    .model-name-plain { font-weight: 600; }
    .toggle-btn { background: #ebf4ff; color: #2b6cb0; border: 1px solid #bee3f8; border-radius: 4px; padding: 4px 10px; cursor: pointer; font-size: 0.8rem; white-space: nowrap; }
    .toggle-btn:hover { background: #bee3f8; }
    .fix-btn { background: #fff; color: #9b2c2c; border: 1px solid #fc8181; border-radius: 4px; padding: 4px 12px; cursor: pointer; font-size: 0.8rem; font-weight: 600; margin-left: 8px; white-space: nowrap; transition: background 0.15s; }
    .fix-btn:hover:not(:disabled) { background: #fff5f5; }
    .fix-btn:disabled { opacity: 0.6; cursor: not-allowed; }
    .count-nonzero { font-weight: 700; color: #c05621; }
    .count-small { color: #718096; font-size: 0.85rem; }
    .count-transitive { color: #718096; font-size: 0.82rem; }
    .transitive-count { background: #fef3c7; color: #92400e; border-radius: 4px; padding: 1px 7px; font-size: 0.78rem; font-weight: 600; }
    .dep-chain { margin-top: 4px; font-size: 0.75rem; color: #718096; }
    .dep-badge { background: #e9d8fd; color: #553c9a; border-radius: 4px; padding: 1px 6px; font-size: 0.75rem; }
    .dep-summary { color: #553c9a; background: #faf5ff; border: 1px solid #e9d8fd; border-radius: 6px; padding: 8px 14px; font-size: 0.85rem; margin-bottom: 12px; }
    .wb-section-label { font-size: 0.78rem; font-weight: 600; color: #4a5568; text-transform: uppercase; letter-spacing: 0.04em; padding: 6px 0 3px; }
    .wb-section-transitive { color: #92400e; margin-top: 10px; }
    .folder-container { padding: 8px 0; }
    .folder-group { margin-bottom: 12px; }
    .folder-name { font-weight: 600; color: #4a5568; margin-bottom: 4px; font-size: 0.9rem; }
    .folder-group ul { margin: 0; padding-left: 20px; }
    .folder-group li { margin: 4px 0; font-size: 0.9rem; }
    .model-card { background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 16px; overflow: hidden; }
    .model-card-error { border-left: 4px solid #fc8181; }
    .model-header { display: flex; align-items: center; gap: 12px; padding: 14px 18px; background: #f7fafc; border-bottom: 1px solid #e2e8f0; flex-wrap: wrap; }
    .model-title { font-weight: 700; font-size: 1rem; }
    .model-id { color: #718096; font-size: 0.8rem; }
    .badge { border-radius: 9999px; padding: 2px 10px; font-size: 0.78rem; font-weight: 600; margin-left: auto; white-space: nowrap; }
    .badge-ok { background: #c6f6d5; color: #276749; }
    .badge-error { background: #fed7d7; color: #9b2c2c; }
    .inner-table { width: 100%; border-collapse: collapse; }
    .inner-table th { background: #f7fafc; font-size: 0.75rem; padding: 8px 14px; }
    .inner-table td { padding: 8px 14px; border-top: 1px solid #e2e8f0; }
    .missing-col { display: inline-block; background: #fed7d7; color: #9b2c2c; border-radius: 4px; padding: 1px 6px; font-family: monospace; font-size: 0.82rem; margin: 1px 2px; }
    .ok-text { color: #38a169; font-size: 0.85rem; }
    .no-elements { color: #718096; padding: 12px 18px; margin: 0; font-size: 0.9rem; }
    .drift-debug { margin-top: 6px; font-size: 0.78rem; color: #718096; }
    .drift-debug summary { cursor: pointer; color: #4a5568; font-weight: 500; }
    .drift-debug-body { margin-top: 6px; display: flex; flex-direction: column; gap: 4px; background: #f7fafc; border-radius: 4px; padding: 8px; }
    code { font-family: "SFMono-Regular", Consolas, monospace; font-size: 0.85em; }
    .formula-element { padding: 12px 18px 4px; border-top: 1px solid #e2e8f0; }
    .formula-element-name { font-weight: 600; font-size: 0.9rem; color: #4a5568; margin-bottom: 8px; }
    .formula-code { background: #f7fafc; border: 1px solid #e2e8f0; border-radius: 4px; padding: 2px 6px; font-size: 0.78rem; word-break: break-all; }
    .formula-type-badge { background: #bee3f8; color: #2b6cb0; border-radius: 4px; padding: 1px 6px; font-size: 0.72rem; font-weight: 600; margin-left: 4px; }
    .formula-type-col { background: #e9d8fd; color: #553c9a; }
    .broken-ref-group { display: inline-flex; align-items: center; gap: 4px; margin: 2px; }
    .fix-formula-btn { background: #f0fff4; color: #276749; border: 1px solid #9ae6b4; border-radius: 4px; padding: 2px 10px; cursor: pointer; font-size: 0.78rem; font-weight: 600; white-space: nowrap; }
    .fix-formula-btn:hover:not(:disabled) { background: #c6f6d5; }
    .fix-formula-btn:disabled { opacity: 0.6; cursor: not-allowed; }
    .formula-suggestion { color: #276749; font-size: 0.78rem; font-style: italic; }
    .formula-no-suggestion { color: #a0aec0; font-size: 0.78rem; font-style: italic; }
  </style>
</head>
<body>
  <h1>Sigma CI Report</h1>
  <div class="subtitle">Generated ${escapeHtml(generatedAt)}</div>
  ${renderContentSection(contentReport)}
  ${renderDriftSection(driftReport, sessionId)}
  ${formulaReport ? renderFormulaSection(formulaReport, sessionId) : ""}
  <script>
    function toggle(id) {
      var el = document.getElementById(id);
      var btn = el ? el.previousElementSibling.querySelector('.toggle-btn') : null;
      if (!el) return;
      if (el.classList.contains('hidden')) { el.classList.remove('hidden'); if (btn) btn.textContent = '▼ Hide workbooks'; }
      else { el.classList.add('hidden'); if (btn) btn.textContent = '▶ Show workbooks'; }
    }
    ${fixScript}
  </script>
</body>
</html>`;
}

export function toTextReport(contentReport: ContentReport, driftReport: DriftReport): string {
  const lines: string[] = [];
  const sep = "─".repeat(60);

  lines.push("SIGMA CI REPORT");
  lines.push(`Generated: ${new Date().toISOString()}`);
  lines.push(sep);
  lines.push("");
  lines.push("CONTENT VALIDATION — DOWNSTREAM BLAST RADIUS");
  lines.push("");

  for (const model of contentReport.models) {
    const directCount = model.downstreamWorkbooks.length;
    const transitiveCount = model.transitiveWorkbooks.length;
    lines.push(`  Model: ${model.modelName} (${model.modelId})`);
    if (model.modelUrl) lines.push(`  URL: ${model.modelUrl}`);
    const upstreamIds = contentReport.modelDependencies?.[model.modelId] ?? [];
    if (upstreamIds.length > 0) lines.push(`  Depends on: ${upstreamIds.join(", ")}`);
    lines.push(`  Direct workbooks: ${directCount}  Transitive: ${transitiveCount}`);
    if (directCount > 0) {
      const byFolder = groupByFolder(model.downstreamWorkbooks);
      for (const [folder, wbs] of byFolder.entries()) {
        lines.push(`    📁 ${folder}`);
        for (const wb of wbs) {
          lines.push(`      • ${wb.name}`);
          if (wb.url) lines.push(`        ${wb.url}`);
        }
      }
    }
    lines.push("");
  }

  lines.push(sep);
  lines.push("");
  lines.push("SCHEMA DRIFT VALIDATION");
  lines.push("");

  for (const model of driftReport.models) {
    const totalMissing = model.tables.reduce((sum, t) => sum + t.missingColumns.length, 0);
    const status = totalMissing === 0 ? "OK" : `${totalMissing} MISSING`;
    lines.push(`  Model: ${model.modelName} (${model.modelId}) — ${status}`);
    for (const t of model.tables) {
      if (t.missingColumns.length > 0) {
        lines.push(`    Table: ${t.tableName}`);
        lines.push(`    Missing: ${t.missingColumns.join(", ")}`);
      }
    }
    lines.push("");
  }

  return lines.join("\n");
}
