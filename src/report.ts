import { ContentReport, DownstreamWorkbook } from "./validators/content.js";
import { DriftReport, DriftTable } from "./validators/schema-drift.js";
import { FormulaCheckReport, FormulaElementResult } from "./validators/formula-check.js";

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

// ── Shared helpers ─────────────────────────────────────────────────────────────

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

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

// ── Unified per-model data structure ──────────────────────────────────────────

interface UnifiedModelRow {
  modelId: string;
  modelName: string;
  modelUrl?: string;
  directWorkbooks: DownstreamWorkbook[];
  transitiveWorkbooks: DownstreamWorkbook[];
  upstreamModelIds: string[];
  driftTables: DriftTable[];
  totalMissingCols: number;
  formulaElements: FormulaElementResult[];
  totalBrokenRefs: number;
  isHealthy: boolean;
}

function mergeReports(
  contentReport: ContentReport,
  driftReport: DriftReport,
  formulaReport: FormulaCheckReport | undefined
): { rows: UnifiedModelRow[]; nameById: Map<string, string> } {
  const nameById = new Map(contentReport.models.map((m) => [m.modelId, m.modelName]));
  const driftByModel   = new Map(driftReport.models.map((m) => [m.modelId, m]));
  const formulaByModel = new Map((formulaReport?.models ?? []).map((m) => [m.modelId, m]));

  const rows: UnifiedModelRow[] = contentReport.models.map((cm) => {
    const dm = driftByModel.get(cm.modelId);
    const fm = formulaByModel.get(cm.modelId);
    const totalMissingCols = dm?.tables.reduce((s, t) => s + t.missingColumns.length, 0) ?? 0;
    const totalBrokenRefs  = fm?.totalBroken ?? 0;
    return {
      modelId: cm.modelId,
      modelName: cm.modelName,
      modelUrl: cm.modelUrl,
      directWorkbooks: cm.downstreamWorkbooks,
      transitiveWorkbooks: cm.transitiveWorkbooks,
      upstreamModelIds: contentReport.modelDependencies[cm.modelId] ?? [],
      driftTables: dm?.tables ?? [],
      totalMissingCols,
      formulaElements: fm?.elements ?? [],
      totalBrokenRefs,
      isHealthy: totalMissingCols === 0 && totalBrokenRefs === 0,
    };
  });

  return { rows, nameById };
}

// ── Summary bar ────────────────────────────────────────────────────────────────

function renderSummaryBar(rows: UnifiedModelRow[]): string {
  const totalModels   = rows.length;
  const healthyModels = rows.filter((r) => r.isHealthy).length;
  const issueModels   = totalModels - healthyModels;
  const wbIds         = new Set(
    rows.flatMap((r) => [...r.directWorkbooks, ...r.transitiveWorkbooks].map((w) => w.workbookId))
  );
  const totalMissing  = rows.reduce((s, r) => s + r.totalMissingCols, 0);
  const totalBroken   = rows.reduce((s, r) => s + r.totalBrokenRefs, 0);

  const stat = (value: number | string, label: string, cls = "") =>
    `<div class="stat-item ${cls}"><span class="stat-value">${value}</span><span class="stat-label">${escapeHtml(label)}</span></div>`;

  return `
  <div class="summary-bar">
    ${stat(totalModels, "models scanned")}
    <div class="stat-divider"></div>
    ${stat(healthyModels, "healthy", healthyModels === totalModels ? "stat-ok" : "")}
    ${stat(issueModels, "with issues", issueModels > 0 ? "stat-error" : "")}
    <div class="stat-divider"></div>
    ${stat(wbIds.size, "workbooks downstream")}
    ${stat(totalMissing, "missing cols", totalMissing > 0 ? "stat-warn" : "")}
    ${stat(totalBroken, "broken refs", totalBroken > 0 ? "stat-warn" : "")}
  </div>`;
}

// ── Overview table ─────────────────────────────────────────────────────────────

function renderOverviewTable(rows: UnifiedModelRow[]): string {
  const tableRows = rows.map((r) => {
    const statusDot = r.isHealthy
      ? `<span class="status-dot dot-ok" title="Healthy"></span>`
      : `<span class="status-dot dot-error" title="Has issues"></span>`;

    const modelCell = r.modelUrl
      ? externalLink(r.modelUrl, r.modelName, "model-link")
      : `<span class="model-name-plain">${escapeHtml(r.modelName)}</span>`;

    const wbCount = r.directWorkbooks.length + r.transitiveWorkbooks.length;
    const wbCell  = wbCount > 0
      ? `<span class="ov-count">${wbCount}</span>`
      : `<span class="ov-zero">—</span>`;

    const driftCell = r.totalMissingCols > 0
      ? `<span class="badge badge-error">${r.totalMissingCols} missing</span>`
      : `<span class="badge badge-ok">✓ OK</span>`;

    const formulaCell = r.totalBrokenRefs > 0
      ? `<span class="badge badge-error">${r.totalBrokenRefs} broken</span>`
      : `<span class="badge badge-ok">✓ OK</span>`;

    return `<tr data-model-id="${escapeHtml(r.modelId)}" class="overview-row" title="Jump to details">
      <td class="td-status">${statusDot}</td>
      <td>${modelCell}</td>
      <td class="td-center">${wbCell}</td>
      <td class="td-center">${driftCell}</td>
      <td class="td-center">${formulaCell}</td>
    </tr>`;
  }).join("\n");

  return `
  <section>
    <h2>Model Overview</h2>
    <table class="overview-table">
      <thead>
        <tr>
          <th style="width:28px"></th>
          <th>Model</th>
          <th class="th-center">Workbooks</th>
          <th class="th-center">Schema Drift</th>
          <th class="th-center">Formula Check</th>
        </tr>
      </thead>
      <tbody>${tableRows}</tbody>
    </table>
  </section>`;
}

// ── Per-model detail card ──────────────────────────────────────────────────────

function renderModelDetailCard(
  row: UnifiedModelRow,
  nameById: Map<string, string>,
  sessionId?: string
): string {
  const cardId     = `model-detail-${row.modelId}`;
  const borderClass = row.isHealthy ? "" : "model-card-error";

  const titleHtml = row.modelUrl
    ? externalLink(row.modelUrl, row.modelName, "model-title-link")
    : `<span class="model-title">${escapeHtml(row.modelName)}</span>`;

  const healthBadge = row.isHealthy
    ? `<span class="badge badge-ok">✓ Healthy</span>`
    : `<span class="badge badge-error">⚠ Issues found</span>`;

  const upstreamHtml = row.upstreamModelIds.length > 0
    ? `<div class="dep-chain">Sources: ${row.upstreamModelIds.map((id) => {
        const name = nameById.get(id) ?? id;
        return `<span class="dep-badge">${escapeHtml(name)}</span>`;
      }).join(" → ")}</div>`
    : "";

  // ── Panel 1: Downstream Workbooks ──
  const totalWbs = row.directWorkbooks.length + row.transitiveWorkbooks.length;
  const wbsPanel = totalWbs === 0
    ? `<p class="no-elements">No downstream workbooks.</p>`
    : `<div class="detail-sub-panel">
        ${row.directWorkbooks.length > 0
          ? `<div class="wb-section-label">Direct (${row.directWorkbooks.length})</div>${renderWorkbookList(row.directWorkbooks)}`
          : ""}
        ${row.transitiveWorkbooks.length > 0
          ? `<div class="wb-section-label wb-section-transitive">Via downstream models (${row.transitiveWorkbooks.length})</div>${renderWorkbookList(row.transitiveWorkbooks)}`
          : ""}
      </div>`;

  // ── Panel 2: Schema Drift ──
  const driftFixBtn = row.totalMissingCols > 0 && sessionId
    ? `<button class="fix-btn"
         data-model-id="${escapeHtml(row.modelId)}"
         data-model-name="${escapeHtml(row.modelName)}"
         data-count="${row.totalMissingCols}"
       >Fix: Remove ${row.totalMissingCols} missing col${row.totalMissingCols !== 1 ? "s" : ""}</button>`
    : "";

  const driftRows = row.driftTables.map((t) => {
    const missingHtml = t.missingColumns.length > 0
      ? t.missingColumns.map((c) => `<span class="missing-col">${escapeHtml(c)}</span>`).join(" ")
      : `<span class="ok-text">none</span>`;

    const debugHtml = t.missingColumns.length > 0 ? (() => {
      const refAll = t.referencedColumns.map((c) => `<code>${escapeHtml(c)}</code>`).join(", ");
      const whAll  = t.actualColumns.map((c) => `<code>${escapeHtml(c)}</code>`).join(", ");
      return `<details class="drift-debug"><summary>🔍 Debug</summary>
        <div class="drift-debug-body">
          <div><strong>Spec extracted (${t.referencedColumns.length}):</strong> ${refAll}</div>
          <div><strong>Warehouse returned (${t.actualColumns.length}):</strong> ${whAll || "<em>none</em>"}</div>
        </div>
      </details>`;
    })() : "";

    return `<tr>
      <td><code>${escapeHtml(t.tableName)}</code>${debugHtml}</td>
      <td class="count-small">${t.referencedColumns.length}</td>
      <td class="count-small">${t.actualColumns.length}</td>
      <td>${missingHtml}</td>
    </tr>`;
  }).join("\n");

  const driftPanel = row.driftTables.length === 0
    ? `<p class="no-elements">No direct warehouse table connections.</p>`
    : `<table class="inner-table">
        <thead><tr><th>Table</th><th>Referenced</th><th>Warehouse</th><th>Missing Columns</th></tr></thead>
        <tbody>${driftRows}</tbody>
      </table>`;

  // ── Panel 3: Formula Errors ──
  const formulaBlocks = row.formulaElements.map((el) => {
    const colRows = el.brokenColumns.map((col) => {
      const colLabel = col.columnName
        ? `<strong>${escapeHtml(col.columnName)}</strong>`
        : `<code class="model-id">${escapeHtml(col.columnId)}</code>`;
      const typeTag = col.isMetric
        ? `<span class="formula-type-badge">metric</span>`
        : `<span class="formula-type-badge formula-type-col">column</span>`;

      const refBadges = col.brokenRefs.map((r) => {
        const fixBtnHtml = r.suggestion && sessionId
          ? `<button class="fix-formula-btn"
               data-model-id="${escapeHtml(row.modelId)}"
               data-model-name="${escapeHtml(row.modelName)}"
               data-element-id="${escapeHtml(el.elementId)}"
               data-column-id="${escapeHtml(col.columnId)}"
               data-broken-ref="${escapeHtml(r.ref)}"
               data-suggestion="${escapeHtml(r.suggestion)}"
             >→ ${escapeHtml(r.suggestion)}</button>`
          : r.suggestion
            ? `<span class="formula-suggestion" title="Re-run with session to enable fix">→ ${escapeHtml(r.suggestion)}</span>`
            : `<span class="formula-no-suggestion">no suggestion</span>`;

        return `<span class="broken-ref-group">
          <span class="missing-col">[${escapeHtml(r.ref)}]</span>
          ${fixBtnHtml}
        </span>`;
      }).join(" ");

      return `<tr>
        <td>${colLabel} ${typeTag}</td>
        <td><code class="formula-code">${escapeHtml(col.formula)}</code></td>
        <td>${refBadges}</td>
      </tr>`;
    }).join("\n");

    return `<div class="formula-element">
      <div class="formula-element-name">📋 ${escapeHtml(el.elementName ?? el.elementId)}</div>
      <table class="inner-table">
        <thead><tr><th>Column / Metric</th><th>Formula</th><th>Broken Refs → Suggestion</th></tr></thead>
        <tbody>${colRows}</tbody>
      </table>
    </div>`;
  }).join("\n");

  const formulaPanel = row.formulaElements.length === 0
    ? `<p class="no-elements" style="color:#276749">✓ No broken formula references.</p>`
    : formulaBlocks;

  return `
  <div class="model-card ${borderClass}" id="${escapeHtml(cardId)}">
    <div class="model-header">
      ${titleHtml}
      ${upstreamHtml}
      <code class="model-id">${escapeHtml(row.modelId)}</code>
      ${healthBadge}
      ${driftFixBtn}
    </div>

    <div class="detail-panels">

      <div class="detail-panel">
        <div class="detail-panel-title">
          <span class="panel-icon">📊</span> Downstream Workbooks
          ${totalWbs > 0
            ? `<span class="panel-count">${totalWbs}</span>`
            : `<span class="panel-count panel-count-ok">none</span>`}
        </div>
        ${wbsPanel}
      </div>

      <div class="detail-panel">
        <div class="detail-panel-title">
          <span class="panel-icon">🔍</span> Schema Drift
          ${row.totalMissingCols > 0
            ? `<span class="panel-count panel-count-error">${row.totalMissingCols} missing</span>`
            : `<span class="panel-count panel-count-ok">✓ clean</span>`}
        </div>
        ${driftPanel}
      </div>

      <div class="detail-panel">
        <div class="detail-panel-title">
          <span class="panel-icon">🔧</span> Formula Check
          ${row.totalBrokenRefs > 0
            ? `<span class="panel-count panel-count-error">${row.totalBrokenRefs} broken</span>`
            : `<span class="panel-count panel-count-ok">✓ clean</span>`}
        </div>
        ${formulaPanel}
      </div>

    </div>
  </div>`;
}

function renderDetailSection(
  rows: UnifiedModelRow[],
  nameById: Map<string, string>,
  sessionId?: string
): string {
  const cards = rows.map((r) => renderModelDetailCard(r, nameById, sessionId)).join("\n");
  return `<section><h2>Model Details</h2>${cards}</section>`;
}

// ── Main HTML report ───────────────────────────────────────────────────────────

export function toHtmlReport(
  contentReport: ContentReport,
  driftReport: DriftReport,
  options?: { sessionId?: string; formulaReport?: FormulaCheckReport }
): string {
  const generatedAt   = new Date().toISOString();
  const sessionId     = options?.sessionId;
  const formulaReport = options?.formulaReport;

  const { rows, nameById } = mergeReports(contentReport, driftReport, formulaReport);

  const fixScript = sessionId ? `
    var SESSION_ID = ${JSON.stringify(sessionId)};

    function escapeEl(s) {
      return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    function fixDrift(modelId, modelName, count) {
      if (!confirm('Remove ' + count + ' missing column(s) from "' + modelName + '"?\\n\\nThis updates the data model in Sigma. This action cannot be undone.')) return;
      var card = document.getElementById('model-detail-' + modelId);
      var btn  = card ? card.querySelector('.fix-btn') : null;
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
          var panel = card.querySelector('.detail-panel:nth-child(2) .detail-panel-title .panel-count');
          if (panel) { panel.className = 'panel-count panel-count-ok'; panel.textContent = '✓ fixed'; }
          if (btn) btn.remove();
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

    function fixFormula(btn, modelId, modelName, elementId, columnId, brokenRef, newRef) {
      if (!confirm('Replace [' + brokenRef + '] with [' + newRef + '] in "' + modelName + '"?')) return;
      btn.disabled = true;
      btn.textContent = 'Fixing…';

      fetch('/api/fix-formula', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sessionId: SESSION_ID, modelId: modelId, elementId: elementId, columnId: columnId, brokenRef: brokenRef, newRef: newRef }),
      })
      .then(function(r) { return r.json(); })
      .then(function(result) {
        if (result.success) {
          var group = btn.closest('.broken-ref-group');
          if (group) {
            group.innerHTML = '<span class="badge-ok" style="border-radius:4px;padding:1px 8px;font-size:0.78rem;">✓ [' + escapeEl(newRef) + ']</span>';
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

    document.addEventListener('click', function(e) {
      var fBtn = e.target.closest('.fix-formula-btn');
      if (fBtn) {
        e.stopPropagation();
        fixFormula(fBtn, fBtn.dataset.modelId, fBtn.dataset.modelName, fBtn.dataset.elementId, fBtn.dataset.columnId, fBtn.dataset.brokenRef, fBtn.dataset.suggestion);
        return;
      }
      var dBtn = e.target.closest('.fix-btn');
      if (dBtn) {
        e.stopPropagation();
        fixDrift(dBtn.dataset.modelId, dBtn.dataset.modelName, parseInt(dBtn.dataset.count, 10));
        return;
      }
    });
  ` : "";

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Sigma CI Report</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f5f7fa; color: #1a202c; margin: 0; padding: 0; }

    /* ── Page shell ── */
    .page-header { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); color: white; padding: 28px 40px 24px; }
    .page-header h1 { font-size: 1.6rem; margin: 0 0 4px; display: flex; align-items: center; gap: 10px; }
    .header-logo { background: rgba(255,255,255,0.15); border-radius: 8px; padding: 4px 10px; font-size: 1.1rem; font-weight: 800; letter-spacing: -1px; }
    .page-header .subtitle { color: rgba(255,255,255,0.6); font-size: 0.86rem; margin: 0; }
    .page-body { max-width: 1200px; margin: 0 auto; padding: 32px 40px 60px; }

    /* ── Typography / sections ── */
    section { margin-bottom: 40px; }
    h2 { font-size: 1.1rem; font-weight: 700; color: #2d3748; border-bottom: 2px solid #e2e8f0; padding-bottom: 8px; margin-bottom: 16px; text-transform: uppercase; letter-spacing: 0.04em; }

    /* ── Summary bar ── */
    .summary-bar { display: flex; flex-wrap: wrap; align-items: center; gap: 0; background: #fff; border-radius: 10px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 32px; overflow: hidden; }
    .stat-item   { display: flex; flex-direction: column; align-items: center; padding: 18px 24px; flex: 1; min-width: 100px; }
    .stat-value  { font-size: 2rem; font-weight: 800; color: #1a202c; line-height: 1; }
    .stat-label  { font-size: 0.7rem; color: #a0aec0; text-transform: uppercase; letter-spacing: 0.06em; margin-top: 5px; text-align: center; }
    .stat-ok   .stat-value { color: #38a169; }
    .stat-error .stat-value { color: #e53e3e; }
    .stat-warn  .stat-value { color: #dd6b20; }
    .stat-divider { width: 1px; background: #e2e8f0; align-self: stretch; margin: 12px 0; }

    /* ── Overview table ── */
    .overview-table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .overview-table th { background: #2d3748; color: rgba(255,255,255,0.85); text-align: left; padding: 10px 16px; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.08em; }
    .overview-table td { padding: 11px 16px; border-top: 1px solid #e2e8f0; vertical-align: middle; }
    .overview-row { cursor: pointer; transition: background 0.1s; }
    .overview-row:hover td { background: #f0f4ff; }
    .td-status  { width: 32px; padding-right: 0; }
    .td-center  { text-align: center; }
    .th-center  { text-align: center; }
    .status-dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; }
    .dot-ok     { background: #48bb78; box-shadow: 0 0 0 3px #c6f6d5; }
    .dot-error  { background: #fc8181; box-shadow: 0 0 0 3px #fed7d7; }
    .ov-count   { font-weight: 600; color: #2b6cb0; }
    .ov-zero    { color: #cbd5e0; }

    /* ── Model cards ── */
    .model-card { background: #fff; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); margin-bottom: 20px; overflow: hidden; border-left: 4px solid #e2e8f0; }
    .model-card-error { border-left-color: #fc8181; }
    .model-header { display: flex; align-items: center; gap: 12px; padding: 16px 20px; background: #f7fafc; border-bottom: 1px solid #e2e8f0; flex-wrap: wrap; }
    .model-title  { font-weight: 700; font-size: 1rem; }
    .model-id     { color: #718096; font-size: 0.78rem; font-family: "SFMono-Regular", Consolas, monospace; }
    .model-title-link { font-weight: 700; font-size: 1rem; }
    .model-name-plain { font-weight: 700; font-size: 1rem; }

    /* ── Badges ── */
    .badge        { border-radius: 9999px; padding: 3px 10px; font-size: 0.75rem; font-weight: 600; white-space: nowrap; display: inline-block; }
    .badge-ok     { background: #c6f6d5; color: #22543d; }
    .badge-error  { background: #fed7d7; color: #742a2a; }
    .badge-ml     { margin-left: auto; }

    /* ── Fix buttons ── */
    .fix-btn { background: #fff; color: #9b2c2c; border: 1px solid #fc8181; border-radius: 6px; padding: 5px 14px; cursor: pointer; font-size: 0.8rem; font-weight: 600; margin-left: auto; white-space: nowrap; transition: background 0.15s; }
    .fix-btn:hover:not(:disabled) { background: #fff5f5; }
    .fix-btn:disabled { opacity: 0.6; cursor: not-allowed; }
    .fix-formula-btn { background: #f0fff4; color: #276749; border: 1px solid #9ae6b4; border-radius: 4px; padding: 2px 10px; cursor: pointer; font-size: 0.78rem; font-weight: 600; white-space: nowrap; }
    .fix-formula-btn:hover:not(:disabled) { background: #c6f6d5; }
    .fix-formula-btn:disabled { opacity: 0.6; cursor: not-allowed; }

    /* ── Detail panels inside each model card ── */
    .detail-panels { }
    .detail-panel  { border-top: 1px solid #e2e8f0; }
    .detail-panel-title { display: flex; align-items: center; gap: 8px; padding: 10px 20px; font-size: 0.83rem; font-weight: 600; color: #4a5568; background: #fbfcfd; border-bottom: 1px solid #f0f0f0; }
    .panel-icon    { font-size: 0.95rem; }
    .panel-count   { margin-left: auto; font-size: 0.75rem; font-weight: 600; background: #edf2f7; color: #4a5568; border-radius: 9999px; padding: 2px 9px; }
    .panel-count-ok    { background: #c6f6d5; color: #22543d; }
    .panel-count-error { background: #fed7d7; color: #742a2a; }

    /* ── Tables ── */
    table { width: 100%; border-collapse: collapse; }
    th    { background: #edf2f7; text-align: left; padding: 8px 14px; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; color: #4a5568; }
    td    { padding: 9px 14px; border-top: 1px solid #e2e8f0; vertical-align: top; }
    tr:hover td { background: #fafafa; }
    .inner-table  { width: 100%; border-collapse: collapse; }
    .inner-table th { background: #f7fafc; font-size: 0.72rem; padding: 7px 14px; }
    .inner-table td { padding: 8px 14px; border-top: 1px solid #edf2f7; }
    .count-small  { color: #718096; font-size: 0.85rem; text-align: right; }

    /* ── Workbook lists ── */
    .detail-sub-panel { padding: 12px 20px 16px; }
    .wb-section-label { font-size: 0.74rem; font-weight: 700; color: #4a5568; text-transform: uppercase; letter-spacing: 0.06em; padding: 4px 0 3px; }
    .wb-section-transitive { color: #92400e; margin-top: 10px; }
    .folder-group { margin-bottom: 12px; }
    .folder-name  { font-weight: 600; color: #4a5568; margin-bottom: 4px; font-size: 0.88rem; }
    .folder-group ul { margin: 0; padding-left: 20px; }
    .folder-group li { margin: 4px 0; font-size: 0.88rem; }

    /* ── Column / formula display ── */
    .missing-col    { display: inline-block; background: #fed7d7; color: #742a2a; border-radius: 4px; padding: 1px 6px; font-family: monospace; font-size: 0.82rem; margin: 1px 2px; }
    .ok-text        { color: #38a169; font-size: 0.85rem; }
    .no-elements    { color: #718096; padding: 12px 20px; margin: 0; font-size: 0.88rem; }
    .formula-element { padding: 12px 20px 4px; border-top: 1px solid #edf2f7; }
    .formula-element-name { font-weight: 600; font-size: 0.88rem; color: #4a5568; margin-bottom: 8px; }
    .formula-code   { background: #f7fafc; border: 1px solid #e2e8f0; border-radius: 4px; padding: 2px 6px; font-size: 0.78rem; word-break: break-all; font-family: "SFMono-Regular", Consolas, monospace; }
    .formula-type-badge { background: #bee3f8; color: #2b6cb0; border-radius: 4px; padding: 1px 6px; font-size: 0.7rem; font-weight: 600; margin-left: 4px; }
    .formula-type-col   { background: #e9d8fd; color: #553c9a; }
    .broken-ref-group   { display: inline-flex; align-items: center; gap: 4px; margin: 2px; }
    .formula-suggestion { color: #276749; font-size: 0.78rem; font-style: italic; }
    .formula-no-suggestion { color: #a0aec0; font-size: 0.78rem; font-style: italic; }

    /* ── Dep chain ── */
    .dep-chain  { font-size: 0.74rem; color: #718096; }
    .dep-badge  { background: #e9d8fd; color: #553c9a; border-radius: 4px; padding: 1px 6px; font-size: 0.74rem; }

    /* ── Debug details ── */
    .drift-debug summary { cursor: pointer; color: #4a5568; font-size: 0.78rem; font-weight: 500; margin-top: 4px; }
    .drift-debug-body { margin-top: 6px; display: flex; flex-direction: column; gap: 4px; background: #f7fafc; border-radius: 4px; padding: 8px; font-size: 0.75rem; color: #718096; }

    /* ── Links ── */
    .ext-link { color: #2b6cb0; text-decoration: underline; text-decoration-color: rgba(43,108,176,0.4); text-underline-offset: 2px; display: inline-flex; align-items: center; gap: 3px; font-weight: 500; }
    .ext-link:hover { color: #1a56db; text-decoration-color: #1a56db; }
    .ext-icon { width: 10px; height: 10px; flex-shrink: 0; opacity: 0.75; }

    code { font-family: "SFMono-Regular", Consolas, monospace; font-size: 0.85em; }
  </style>
</head>
<body>
  <div class="page-header">
    <h1><span class="header-logo">Σ</span> Sigma CI Report</h1>
    <p class="subtitle">Generated ${escapeHtml(generatedAt)}</p>
  </div>
  <div class="page-body">
    ${renderSummaryBar(rows)}
    ${renderOverviewTable(rows)}
    ${renderDetailSection(rows, nameById, sessionId)}
  </div>
  <script>
    function scrollToModel(modelId) {
      var el = document.getElementById('model-detail-' + modelId);
      if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    document.addEventListener('click', function(e) {
      var row = e.target.closest('.overview-row');
      if (row && !e.target.closest('a') && !e.target.closest('button')) {
        scrollToModel(row.dataset.modelId);
      }
    });

    ${fixScript}
  </script>
</body>
</html>`;
}

// ── Plain text report (unchanged) ─────────────────────────────────────────────

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
