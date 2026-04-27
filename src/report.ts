import { ContentReport, DownstreamWorkbook } from "./validators/content.js";
import { DriftReport, DriftTable } from "./validators/schema-drift.js";
import { FormulaCheckReport, FormulaElementResult } from "./validators/formula-check.js";
import { DirectSourceReport, DirectSourceWorkbook, DirectSourceElement } from "./validators/workbook-direct-source.js";

export type MemberMap = Map<string, { name: string; email: string }>;

export interface CombinedReport {
  generatedAt: string;
  content: ContentReport;
  schemaDrift: DriftReport;
}

export function toJsonReport(
  contentReport: ContentReport,
  driftReport: DriftReport,
  formulaReport?: FormulaCheckReport,
  directSourceReport?: DirectSourceReport
): string {
  return JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      content: contentReport,
      schemaDrift: driftReport,
      formulaCheck: formulaReport,
      directSourceWorkbooks: directSourceReport,
    },
    null,
    2
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
    return `<div class="folder-group"><div class="folder-name">${escapeHtml(folder)}</div><ul>${links}</ul></div>`;
  }).join("\n");
}

// ── Unified per-model data structure ──────────────────────────────────────────

interface UnifiedModelRow {
  modelId: string;
  modelName: string;
  modelUrl?: string;
  path?: string;
  ownerId?: string;
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
      path: cm.path,
      ownerId: cm.ownerId,
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

// ── Owner / folder helpers ────────────────────────────────────────────────────

function ownerDisplay(ownerId: string | undefined, memberMap: MemberMap | undefined): string {
  if (!ownerId) return "—";
  const m = memberMap?.get(ownerId);
  if (!m) return "—";
  return m.name || m.email || "—";
}

function folderDisplay(path: string | undefined): string {
  return path ? escapeHtml(path) : "—";
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
  const divider = `<div class="stat-divider"></div>`;

  return `
  <div class="summary-bar">
    ${stat(totalModels, "models scanned")}
    ${divider}
    ${stat(healthyModels, "healthy", healthyModels === totalModels ? "stat-ok" : "")}
    ${stat(issueModels, "with issues", issueModels > 0 ? "stat-error" : "")}
    ${divider}
    ${stat(wbIds.size, "downstream workbooks")}
    ${stat(totalMissing, "missing cols", totalMissing > 0 ? "stat-warn" : "")}
    ${stat(totalBroken, "broken formula refs", totalBroken > 0 ? "stat-warn" : "")}
  </div>`;
}

function renderWorkbooksSummaryBar(directReport: DirectSourceReport | undefined): string {
  if (!directReport) return "";

  const totalScanned   = directReport.totalWorkbooksScanned;
  const directWbCount  = directReport.workbooks.length;
  const customSqlCount = directReport.totalCustomSqlElements;
  const driftCols      = directReport.totalMissingColumns;

  const stat = (value: number | string, label: string, cls = "") =>
    `<div class="stat-item ${cls}"><span class="stat-value">${value}</span><span class="stat-label">${escapeHtml(label)}</span></div>`;
  const divider = `<div class="stat-divider"></div>`;

  return `
  <div class="summary-bar">
    ${stat(totalScanned, "workbooks scanned")}
    ${divider}
    ${stat(directWbCount, "direct-source workbooks", directWbCount > 0 ? "stat-warn" : "stat-ok")}
    ${stat(customSqlCount, "custom SQL elements", customSqlCount > 0 ? "stat-warn" : "")}
    ${stat(driftCols, "warehouse cols dropped", driftCols > 0 ? "stat-error" : "")}
  </div>`;
}

// ── Overview table ─────────────────────────────────────────────────────────────

function renderOverviewTable(rows: UnifiedModelRow[], memberMap: MemberMap | undefined): string {
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
      : `<span class="badge badge-ok">Clean</span>`;

    const formulaCell = r.totalBrokenRefs > 0
      ? `<span class="badge badge-error">${r.totalBrokenRefs} broken</span>`
      : `<span class="badge badge-ok">Clean</span>`;

    return `<tr data-model-id="${escapeHtml(r.modelId)}" class="overview-row" title="Jump to details">
      <td class="td-status">${statusDot}</td>
      <td>${modelCell}</td>
      <td class="td-meta">${folderDisplay(r.path)}</td>
      <td class="td-meta">${escapeHtml(ownerDisplay(r.ownerId, memberMap))}</td>
      <td class="td-center">${wbCell}</td>
      <td class="td-center">${driftCell}</td>
      <td class="td-center">${formulaCell}</td>
    </tr>`;
  }).join("\n");

  return `
  <section>
    <h2>Data Models</h2>
    <table class="overview-table">
      <thead>
        <tr>
          <th style="width:28px"></th>
          <th>Model</th>
          <th>Folder</th>
          <th>Owner</th>
          <th class="th-center">Downstream Workbooks</th>
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
  sessionId?: string,
  memberMap?: MemberMap
): string {
  const cardId      = `model-detail-${row.modelId}`;
  const borderClass = row.isHealthy ? "" : "model-card-error";

  const titleHtml = row.modelUrl
    ? externalLink(row.modelUrl, row.modelName, "model-title-link")
    : `<span class="model-title">${escapeHtml(row.modelName)}</span>`;

  const healthBadge = row.isHealthy
    ? `<span class="badge badge-ok">Healthy</span>`
    : `<span class="badge badge-error">Issues found</span>`;

  const metaHtml = `<div class="model-meta"><span class="meta-item"><span class="meta-label">Folder</span>${folderDisplay(row.path)}</span><span class="meta-item"><span class="meta-label">Owner</span>${escapeHtml(ownerDisplay(row.ownerId, memberMap))}</span></div>`;

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
       >Remove ${row.totalMissingCols} missing col${row.totalMissingCols !== 1 ? "s" : ""} from spec</button>`
    : "";

  const driftRows = row.driftTables.map((t) => {
    const missingHtml = t.missingColumns.length > 0
      ? t.missingColumns.map((c) => `<span class="tag tag-error">${escapeHtml(c)}</span>`).join(" ")
      : `<span class="ok-text">none</span>`;

    const debugHtml = t.missingColumns.length > 0 ? (() => {
      const refAll = t.referencedColumns.map((c) => `<code>${escapeHtml(c)}</code>`).join(", ");
      const whAll  = t.actualColumns.map((c) => `<code>${escapeHtml(c)}</code>`).join(", ");
      return `<details class="drift-debug"><summary>Debug</summary>
        <div class="drift-debug-body">
          <div><strong>Spec references (${t.referencedColumns.length}):</strong> ${refAll}</div>
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
        <thead><tr><th>Table</th><th class="th-num">Referenced</th><th class="th-num">Warehouse</th><th>Missing Columns</th></tr></thead>
        <tbody>${driftRows}</tbody>
      </table>`;

  // ── Panel 3: Formula Check (display only — no fix buttons) ──
  const formulaBlocks = row.formulaElements.map((el) => {
    const colRows = el.brokenColumns.map((col) => {
      const colLabel = col.columnName
        ? `<strong>${escapeHtml(col.columnName)}</strong>`
        : `<code class="model-id">${escapeHtml(col.columnId)}</code>`;
      const typeTag = col.isMetric
        ? `<span class="tag tag-blue">metric</span>`
        : `<span class="tag tag-purple">column</span>`;

      const refBadges = col.brokenRefs.map((r) => {
        const suggestionHtml = r.suggestion
          ? `<span class="formula-suggestion" title="Similarity: ${(r.similarity * 100).toFixed(0)}%">→ ${escapeHtml(r.suggestion)}</span>`
          : `<span class="formula-no-suggestion">no suggestion</span>`;

        return `<span class="broken-ref-group">
          <span class="tag tag-error">[${escapeHtml(r.ref)}]</span>
          ${suggestionHtml}
        </span>`;
      }).join(" ");

      return `<tr>
        <td>${colLabel} ${typeTag}</td>
        <td><code class="formula-code">${escapeHtml(col.formula)}</code></td>
        <td>${refBadges}</td>
      </tr>`;
    }).join("\n");

    return `<div class="formula-element">
      <div class="formula-element-name">${escapeHtml(el.elementName ?? el.elementId)}</div>
      <table class="inner-table">
        <thead><tr><th>Column / Metric</th><th>Formula</th><th>Broken Refs</th></tr></thead>
        <tbody>${colRows}</tbody>
      </table>
    </div>`;
  }).join("\n");

  const formulaPanel = row.formulaElements.length === 0
    ? `<p class="no-elements ok-text">No broken formula references.</p>`
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
    ${metaHtml}

    <div class="detail-panels">

      <div class="detail-panel">
        <div class="detail-panel-title">
          <svg class="panel-svg" viewBox="0 0 16 16" fill="none"><rect x="2" y="2" width="5" height="5" rx="1" fill="currentColor" opacity=".7"/><rect x="9" y="2" width="5" height="5" rx="1" fill="currentColor" opacity=".7"/><rect x="2" y="9" width="5" height="5" rx="1" fill="currentColor" opacity=".4"/><rect x="9" y="9" width="5" height="5" rx="1" fill="currentColor" opacity=".4"/></svg>
          Downstream Workbooks
          ${totalWbs > 0
            ? `<span class="panel-count">${totalWbs}</span>`
            : `<span class="panel-count panel-count-ok">none</span>`}
        </div>
        ${wbsPanel}
      </div>

      <div class="detail-panel">
        <div class="detail-panel-title">
          <svg class="panel-svg" viewBox="0 0 16 16" fill="none"><path d="M8 2v4M8 10v4M2 8h4M10 8h4" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/><circle cx="8" cy="8" r="2.5" fill="currentColor" opacity=".7"/></svg>
          Schema Drift
          ${row.totalMissingCols > 0
            ? `<span class="panel-count panel-count-error">${row.totalMissingCols} missing</span>`
            : `<span class="panel-count panel-count-ok">Clean</span>`}
        </div>
        ${driftPanel}
      </div>

      <div class="detail-panel">
        <div class="detail-panel-title">
          <svg class="panel-svg" viewBox="0 0 16 16" fill="none"><path d="M3 4h10M3 8h7M3 12h5" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>
          Formula Check
          ${row.totalBrokenRefs > 0
            ? `<span class="panel-count panel-count-error">${row.totalBrokenRefs} broken</span>`
            : `<span class="panel-count panel-count-ok">Clean</span>`}
        </div>
        ${formulaPanel}
      </div>

    </div>
  </div>`;
}

function renderDetailSection(
  rows: UnifiedModelRow[],
  nameById: Map<string, string>,
  sessionId?: string,
  memberMap?: MemberMap
): string {
  const cards = rows.map((r) => renderModelDetailCard(r, nameById, sessionId, memberMap)).join("\n");
  return `<section><h2>Model Details</h2>${cards}</section>`;
}

// ── Direct-source workbooks section ───────────────────────────────────────────

function renderDirectSourceElement(elem: DirectSourceElement): string {
  const name = elem.elementName ?? elem.elementId;

  if (elem.sourceKind === "custom-sql") {
    const sql = elem.sqlDefinition ?? "";
    const truncated = sql.length > 120 ? sql.slice(0, 120) + "…" : sql;
    return `<tr>
      <td><span class="elem-name">${escapeHtml(name)}</span></td>
      <td colspan="3"><code class="sql-preview">${escapeHtml(truncated)}</code></td>
    </tr>`;
  }

  const driftBadge = elem.missingColumns.length > 0
    ? `<span class="badge badge-error">${elem.missingColumns.length} col${elem.missingColumns.length !== 1 ? "s" : ""} dropped from warehouse</span>`
    : elem.actualColumns.length === 0
    ? `<span class="badge badge-warn">Schema not synced</span>`
    : `<span class="badge badge-ok">Clean</span>`;

  const missingHtml = elem.missingColumns.length > 0
    ? elem.missingColumns.map((c) => `<span class="tag tag-error">${escapeHtml(c)}</span>`).join(" ")
    : "";

  return `<tr>
    <td><span class="elem-name">${escapeHtml(name)}</span></td>
    <td><code>${escapeHtml(elem.tableName)}</code></td>
    <td class="td-center">${driftBadge}</td>
    <td>${missingHtml}</td>
  </tr>`;
}

function renderDirectSourceCard(wb: DirectSourceWorkbook, memberMap: MemberMap | undefined): string {
  const titleHtml = wb.workbookUrl
    ? externalLink(wb.workbookUrl, wb.workbookName, "model-title-link")
    : `<span class="model-title">${escapeHtml(wb.workbookName)}</span>`;

  const govBadge = `<span class="badge badge-warn">Bypasses data model</span>`;
  const sqlBadge = wb.hasCustomSql ? `<span class="badge badge-warn">Custom SQL</span>` : ``;
  const driftBadge = wb.hasDrift ? `<span class="badge badge-error">Warehouse cols dropped</span>` : ``;
  const metaHtml = `<div class="model-meta"><span class="meta-item"><span class="meta-label">Folder</span>${folderDisplay(wb.path)}</span><span class="meta-item"><span class="meta-label">Owner</span>${escapeHtml(ownerDisplay(wb.ownerId, memberMap))}</span></div>`;

  const warehouseElems = wb.elements.filter((e) => e.sourceKind === "warehouse-table");
  const customSqlElems = wb.elements.filter((e) => e.sourceKind === "custom-sql");

  const warehousePanel = warehouseElems.length > 0 ? `
      <div class="detail-panel">
        <div class="detail-panel-title">
          <svg class="panel-svg" viewBox="0 0 16 16" fill="none"><path d="M8 2C4.686 2 2 4.686 2 8s2.686 6 6 6 6-2.686 6-6-2.686-6-6-6z" stroke="currentColor" stroke-width="1.5"/><path d="M8 7v4M8 5.5v.5" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>
          Direct Warehouse Elements
          <span class="panel-count panel-count-warn">${warehouseElems.length}</span>
        </div>
        <table class="inner-table">
          <thead><tr><th>Element</th><th>Warehouse Table</th><th class="th-center">Drift</th><th>Columns Dropped from Warehouse</th></tr></thead>
          <tbody>${warehouseElems.map(renderDirectSourceElement).join("\n")}</tbody>
        </table>
      </div>` : ``;

  const sqlPanel = customSqlElems.length > 0 ? `
      <div class="detail-panel">
        <div class="detail-panel-title">
          <svg class="panel-svg" viewBox="0 0 16 16" fill="none"><path d="M3 4h10M3 8h7M3 12h5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>
          Custom SQL Elements
          <span class="panel-count panel-count-warn">${customSqlElems.length}</span>
        </div>
        <table class="inner-table">
          <thead><tr><th>Element</th><th colspan="3">SQL Definition</th></tr></thead>
          <tbody>${customSqlElems.map(renderDirectSourceElement).join("\n")}</tbody>
        </table>
      </div>` : ``;

  return `
  <div class="model-card model-card-warn">
    <div class="model-header">
      ${titleHtml}
      <code class="model-id">${escapeHtml(wb.workbookId)}</code>
      ${govBadge}
      ${sqlBadge}
      ${driftBadge}
    </div>
    ${metaHtml}
    <div class="detail-panels">
      ${warehousePanel}
      ${sqlPanel}
    </div>
  </div>`;
}

function renderDirectSourceSection(report: DirectSourceReport | undefined, memberMap: MemberMap | undefined): string {
  if (!report || report.workbooks.length === 0) {
    return `
    <section>
      <h2>Direct Warehouse Access</h2>
      <p class="empty-state">No workbooks with elements sourced directly from the warehouse. All workbooks route through data models.</p>
    </section>`;
  }

  const cards = report.workbooks.map((wb) => renderDirectSourceCard(wb, memberMap)).join("\n");
  return `
  <section>
    <h2>Direct Warehouse Access <span class="section-badge section-badge-warn">${report.workbooks.length} workbook${report.workbooks.length !== 1 ? "s" : ""}</span></h2>
    <p class="section-desc">These workbooks have elements sourced directly from the warehouse, bypassing the data model layer. <strong>Drift</strong> is flagged when the warehouse table has dropped a column the workbook element still references — those columns will error at query time.</p>
    ${cards}
  </section>`;
}

// ── Main HTML report ───────────────────────────────────────────────────────────

export function toHtmlReport(
  contentReport: ContentReport,
  driftReport: DriftReport,
  options?: { sessionId?: string; formulaReport?: FormulaCheckReport; directSourceReport?: DirectSourceReport; memberMap?: MemberMap }
): string {
  const generatedAt        = new Date().toISOString();
  const sessionId          = options?.sessionId;
  const formulaReport      = options?.formulaReport;
  const directSourceReport = options?.directSourceReport;
  const memberMap          = options?.memberMap;

  const { rows, nameById } = mergeReports(contentReport, driftReport, formulaReport);

  const fixScript = sessionId ? `
    var SESSION_ID = ${JSON.stringify(sessionId)};

    function fixDrift(modelId, modelName, count) {
      if (!confirm('Remove ' + count + ' missing column(s) from "' + modelName + '"?\\n\\nThis updates the data model spec in Sigma. Cannot be undone.')) return;
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
          if (panel) { panel.className = 'panel-count panel-count-ok'; panel.textContent = 'Fixed'; }
          if (btn) btn.remove();
        } else {
          if (btn) { btn.disabled = false; btn.textContent = 'Retry'; }
          alert('Fix failed: ' + (result.error || 'Unknown error'));
        }
      })
      .catch(function(e) {
        if (btn) { btn.disabled = false; btn.textContent = 'Retry'; }
        alert('Fix failed: ' + e.message);
      });
    }

    document.addEventListener('click', function(e) {
      var dBtn = e.target.closest('.fix-btn');
      if (dBtn) {
        e.stopPropagation();
        fixDrift(dBtn.dataset.modelId, dBtn.dataset.modelName, parseInt(dBtn.dataset.count, 10));
      }
    });
  ` : "";

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Sigma Sentinel Report</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, sans-serif; background: #f4f6f9; color: #111827; margin: 0; padding: 0; font-size: 14px; line-height: 1.5; }

    /* ── Page shell ── */
    .page-header { background: #0f172a; color: white; padding: 24px 40px 22px; display: flex; align-items: center; gap: 14px; }
    .header-logo svg { display: block; }
    .header-text h1 { font-size: 1.2rem; font-weight: 700; margin: 0 0 2px; letter-spacing: -0.01em; }
    .header-text p { color: rgba(255,255,255,0.45); font-size: 0.78rem; margin: 0; }
    .page-body { max-width: 1180px; margin: 0 auto; padding: 28px 36px 60px; }

    /* ── Sections ── */
    section { margin-bottom: 36px; }
    h2 { font-size: 0.72rem; font-weight: 700; color: #6b7280; text-transform: uppercase; letter-spacing: 0.08em; border-bottom: 1px solid #e5e7eb; padding-bottom: 8px; margin-bottom: 16px; display: flex; align-items: center; gap: 10px; }
    .section-badge { font-size: 0.68rem; font-weight: 700; border-radius: 9999px; padding: 2px 9px; text-transform: none; letter-spacing: 0; }
    .section-badge-warn { background: #fef3c7; color: #92400e; }
    .section-desc { color: #6b7280; font-size: 0.82rem; margin: -8px 0 16px; line-height: 1.6; }
    .empty-state { color: #9ca3af; font-size: 0.85rem; padding: 20px 0; }

    /* ── Summary bar ── */
    .summary-bar { display: flex; flex-wrap: wrap; align-items: stretch; background: #fff; border-radius: 10px; box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 0 0 1px rgba(0,0,0,0.05); margin-bottom: 28px; overflow: hidden; }
    .stat-item   { display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 16px 20px; flex: 1; min-width: 90px; }
    .stat-value  { font-size: 1.8rem; font-weight: 800; color: #111827; line-height: 1; letter-spacing: -0.02em; }
    .stat-label  { font-size: 0.65rem; color: #9ca3af; text-transform: uppercase; letter-spacing: 0.06em; margin-top: 4px; text-align: center; }
    .stat-ok   .stat-value { color: #16a34a; }
    .stat-error .stat-value { color: #dc2626; }
    .stat-warn  .stat-value { color: #d97706; }
    .stat-divider { width: 1px; background: #f3f4f6; align-self: stretch; margin: 12px 0; }

    /* ── Overview table ── */
    .overview-table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 0 0 1px rgba(0,0,0,0.05); }
    .overview-table th { background: #f9fafb; color: #374151; text-align: left; padding: 9px 16px; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.07em; border-bottom: 1px solid #e5e7eb; font-weight: 600; }
    .overview-table td { padding: 10px 16px; border-top: 1px solid #f3f4f6; vertical-align: middle; }
    .overview-row { cursor: pointer; transition: background 0.1s; }
    .overview-row:hover td { background: #f9fafb; }
    .td-status  { width: 28px; padding-right: 0; }
    .td-center  { text-align: center; }
    .th-center  { text-align: center !important; }
    .th-num     { text-align: right !important; width: 90px; }
    .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; }
    .dot-ok     { background: #22c55e; box-shadow: 0 0 0 3px #dcfce7; }
    .dot-error  { background: #ef4444; box-shadow: 0 0 0 3px #fee2e2; }
    .ov-count   { font-weight: 600; color: #1d4ed8; }
    .ov-zero    { color: #d1d5db; }

    /* ── Model cards ── */
    .model-card { background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 0 0 1px rgba(0,0,0,0.05); margin-bottom: 16px; overflow: hidden; border-left: 3px solid #e5e7eb; }
    .model-card-error { border-left-color: #ef4444; }
    .model-card-warn  { border-left-color: #f59e0b; }
    .model-header { display: flex; align-items: center; gap: 10px; padding: 14px 18px; background: #fafafa; border-bottom: 1px solid #f3f4f6; flex-wrap: wrap; }
    .model-title  { font-weight: 700; font-size: 0.95rem; color: #111827; }
    .model-id     { color: #9ca3af; font-size: 0.72rem; font-family: "SFMono-Regular", Consolas, monospace; }
    .model-title-link { font-weight: 700; font-size: 0.95rem; }
    .model-meta   { display: flex; gap: 20px; padding: 6px 18px; background: #f9fafb; border-bottom: 1px solid #f3f4f6; }
    .meta-item    { font-size: 0.75rem; color: #6b7280; }
    .meta-label   { font-weight: 600; color: #9ca3af; text-transform: uppercase; font-size: 0.65rem; letter-spacing: 0.04em; margin-right: 5px; }
    .td-meta      { font-size: 0.8rem; color: #6b7280; white-space: nowrap; }
    .model-name-plain { font-weight: 700; font-size: 0.95rem; color: #111827; }

    /* ── Badges & tags ── */
    .badge        { border-radius: 9999px; padding: 3px 10px; font-size: 0.7rem; font-weight: 600; white-space: nowrap; display: inline-block; }
    .badge-ok     { background: #dcfce7; color: #166534; }
    .badge-error  { background: #fee2e2; color: #991b1b; }
    .badge-warn   { background: #fef3c7; color: #92400e; }
    .tag          { display: inline-block; border-radius: 4px; padding: 1px 6px; font-family: "SFMono-Regular", Consolas, monospace; font-size: 0.75rem; margin: 1px 2px; }
    .tag-error    { background: #fee2e2; color: #991b1b; }
    .tag-blue     { background: #dbeafe; color: #1e40af; font-family: inherit; font-size: 0.7rem; }
    .tag-purple   { background: #ede9fe; color: #5b21b6; font-family: inherit; font-size: 0.7rem; }

    /* ── Fix button (schema drift only) ── */
    .fix-btn { background: #fff; color: #991b1b; border: 1px solid #fca5a5; border-radius: 6px; padding: 4px 12px; cursor: pointer; font-size: 0.75rem; font-weight: 600; margin-left: auto; white-space: nowrap; transition: background 0.15s; }
    .fix-btn:hover:not(:disabled) { background: #fef2f2; }
    .fix-btn:disabled { opacity: 0.5; cursor: not-allowed; }

    /* ── Detail panels ── */
    .detail-panel  { border-top: 1px solid #f3f4f6; }
    .detail-panel-title { display: flex; align-items: center; gap: 7px; padding: 9px 18px; font-size: 0.75rem; font-weight: 600; color: #374151; background: #fafafa; border-bottom: 1px solid #f3f4f6; }
    .panel-svg     { width: 14px; height: 14px; color: #9ca3af; flex-shrink: 0; }
    .panel-count   { margin-left: auto; font-size: 0.7rem; font-weight: 600; background: #f3f4f6; color: #6b7280; border-radius: 9999px; padding: 1px 8px; }
    .panel-count-ok    { background: #dcfce7; color: #166534; }
    .panel-count-error { background: #fee2e2; color: #991b1b; }
    .panel-count-warn  { background: #fef3c7; color: #92400e; }

    /* ── Tables ── */
    table { width: 100%; border-collapse: collapse; }
    th    { background: #f9fafb; text-align: left; padding: 8px 14px; font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.06em; color: #6b7280; border-bottom: 1px solid #e5e7eb; font-weight: 600; }
    td    { padding: 8px 14px; border-top: 1px solid #f3f4f6; vertical-align: top; }
    tr:last-child td { border-bottom: none; }
    .inner-table th { background: #fafafa; font-size: 0.65rem; padding: 6px 14px; border-bottom: 1px solid #f0f0f0; }
    .inner-table td { padding: 7px 14px; border-top: 1px solid #f5f5f5; }
    .count-small  { color: #9ca3af; font-size: 0.82rem; text-align: right; }

    /* ── Workbook lists ── */
    .detail-sub-panel { padding: 12px 18px 14px; }
    .wb-section-label { font-size: 0.68rem; font-weight: 700; color: #6b7280; text-transform: uppercase; letter-spacing: 0.06em; padding: 4px 0 3px; }
    .wb-section-transitive { color: #92400e; margin-top: 10px; }
    .folder-group { margin-bottom: 10px; }
    .folder-name  { font-weight: 600; color: #374151; margin-bottom: 3px; font-size: 0.82rem; }
    .folder-group ul { margin: 0; padding-left: 16px; }
    .folder-group li { margin: 3px 0; font-size: 0.82rem; color: #374151; }

    /* ── Column / formula display ── */
    .ok-text        { color: #16a34a; font-size: 0.82rem; }
    .no-elements    { color: #9ca3af; padding: 12px 18px; margin: 0; font-size: 0.82rem; }
    .formula-element { padding: 10px 18px 4px; border-top: 1px solid #f3f4f6; }
    .formula-element-name { font-weight: 600; font-size: 0.82rem; color: #374151; margin-bottom: 6px; }
    .formula-code   { background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 4px; padding: 2px 5px; font-size: 0.73rem; word-break: break-all; font-family: "SFMono-Regular", Consolas, monospace; color: #1f2937; }
    .broken-ref-group   { display: inline-flex; align-items: center; gap: 4px; margin: 2px; }
    .formula-suggestion { color: #166534; font-size: 0.73rem; background: #dcfce7; border-radius: 3px; padding: 1px 5px; }
    .formula-no-suggestion { color: #9ca3af; font-size: 0.73rem; font-style: italic; }

    /* ── Direct source ── */
    .elem-name { font-weight: 500; color: #111827; }
    .sql-preview { font-size: 0.72rem; color: #374151; white-space: pre-wrap; word-break: break-all; }

    /* ── Dep chain ── */
    .dep-chain  { font-size: 0.7rem; color: #6b7280; }
    .dep-badge  { background: #ede9fe; color: #5b21b6; border-radius: 4px; padding: 1px 6px; font-size: 0.7rem; }

    /* ── Debug details ── */
    .drift-debug summary { cursor: pointer; color: #6b7280; font-size: 0.73rem; margin-top: 3px; user-select: none; }
    .drift-debug-body { margin-top: 4px; display: flex; flex-direction: column; gap: 4px; background: #f9fafb; border-radius: 4px; padding: 7px 10px; font-size: 0.72rem; color: #6b7280; }

    /* ── Links ── */
    .ext-link { color: #1d4ed8; text-decoration: none; font-weight: 500; }
    .ext-link:hover { text-decoration: underline; }
    .ext-icon { width: 9px; height: 9px; display: inline-block; vertical-align: middle; position: relative; top: -1px; margin-left: 3px; opacity: 0.6; }

    code { font-family: "SFMono-Regular", Consolas, monospace; font-size: 0.85em; }
  </style>
</head>
<body>
  <div class="page-header">
    <div class="header-logo">
      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 30 35" width="28" height="33" fill="none">
        <path d="M15 1.5 L27 6 L27 20 C27 28 21 33 15 35 C9 33 3 28 3 20 L3 6 Z"
              fill="rgba(255,255,255,0.12)" stroke="rgba(255,255,255,0.35)" stroke-width="1.2" stroke-linejoin="round"/>
        <text x="15" y="23" text-anchor="middle" fill="white" font-size="14" font-weight="800"
              font-family="Georgia,'Times New Roman',serif">&#x3A3;</text>
      </svg>
    </div>
    <div class="header-text">
      <h1>Sigma Sentinel</h1>
      <p>Generated ${escapeHtml(generatedAt)}</p>
    </div>
  </div>
  <div class="page-body">
    ${renderSummaryBar(rows)}
    ${renderWorkbooksSummaryBar(directSourceReport)}
    ${renderOverviewTable(rows, memberMap)}
    ${renderDirectSourceSection(directSourceReport, memberMap)}
    ${renderDetailSection(rows, nameById, sessionId, memberMap)}
  </div>
  <script>
    document.addEventListener('click', function(e) {
      var row = e.target.closest('.overview-row');
      if (row && !e.target.closest('a') && !e.target.closest('button')) {
        var el = document.getElementById('model-detail-' + row.dataset.modelId);
        if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    });
    ${fixScript}
  </script>
</body>
</html>`;
}

// ── Plain text report ─────────────────────────────────────────────────────────

export function toTextReport(contentReport: ContentReport, driftReport: DriftReport): string {
  const lines: string[] = [];
  const sep = "─".repeat(60);

  lines.push("SIGMA SENTINEL REPORT");
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
        lines.push(`    ${folder}`);
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
