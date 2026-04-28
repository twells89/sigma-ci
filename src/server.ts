import express from "express";
import { randomUUID } from "crypto";
import { SigmaClient } from "./sigma-client.js";
import { runContentValidation } from "./validators/content.js";
import { runSchemaDriftValidation } from "./validators/schema-drift.js";
import { applyDriftFix } from "./validators/drift-fix.js";
import { runFormulaCheck } from "./validators/formula-check.js";
import { runWorkbookDirectSourceCheck } from "./validators/workbook-direct-source.js";
import { toHtmlReport, MemberMap } from "./report.js";

const app = express();
app.use(express.json());

// ─── Session store (in-memory, 1-hour TTL) ────────────────────────────────────

interface Session {
  clientId: string;
  clientSecret: string;
  baseUrl?: string;
  createdAt: number;
}

const sessions = new Map<string, Session>();

// ─── Report store (in-memory, 2-hour TTL) ─────────────────────────────────────
// Storing HTML here lets the browser load the report as a normal GET request
// instead of using document.write(), which is unreliable for large documents.

interface StoredReport {
  html: string;
  createdAt: number;
}

const reports = new Map<string, StoredReport>();

// ─── Job store (in-memory, 30-min TTL) ────────────────────────────────────────
// Each validation run gets a jobId.  The client polls /api/status/:jobId every
// second rather than using SSE, which Render's proxy buffers until res.end().

interface Job {
  status: "running" | "done" | "error";
  steps: string[];
  redirectUrl?: string;
  error?: string;
  createdAt: number;
}

const jobs = new Map<string, Job>();

setInterval(() => {
  const now = Date.now();
  for (const [id, s] of sessions) {
    if (now - s.createdAt > 3_600_000) sessions.delete(id);
  }
  for (const [id, r] of reports) {
    if (now - r.createdAt > 7_200_000) reports.delete(id);
  }
  for (const [id, j] of jobs) {
    if (now - j.createdAt > 1_800_000) jobs.delete(id);
  }
}, 600_000);

// ─── Landing page ─────────────────────────────────────────────────────────────

const LANDING_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Sigma Sentinel</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f0f2f5;
      min-height: 100vh;
      margin: 0;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 40px 16px 64px;
      color: #1a202c;
    }
    .page-shell {
      width: 100%;
      max-width: 1080px;
      border-radius: 14px;
      box-shadow: 0 4px 32px rgba(0,0,0,0.11);
      overflow: hidden;
      background: #fff;
    }

    /* ── Header (full width) ── */
    .page-header {
      background: linear-gradient(135deg, #0c0c1e 0%, #1a0f40 55%, #2d1b6b 100%);
      padding: 28px 40px 24px;
      color: white;
      position: relative;
      overflow: hidden;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 24px;
    }
    .page-header::before {
      content: '';
      position: absolute;
      width: 340px; height: 340px;
      background: radial-gradient(circle, rgba(139,92,246,0.15) 0%, transparent 65%);
      top: -120px; right: -60px;
      pointer-events: none;
    }
    .header-left { display: flex; align-items: center; gap: 14px; position: relative; }
    @keyframes sentinel-glow {
      0%, 100% { filter: drop-shadow(0 0 0px rgba(167,139,250,0)); }
      50%       { filter: drop-shadow(0 0 7px rgba(167,139,250,0.45)); }
    }
    .logo-svg { animation: sentinel-glow 4s ease-in-out infinite; flex-shrink: 0; }
    .header-title { margin: 0; font-size: 1.5rem; font-weight: 700; letter-spacing: -0.3px; }
    .header-subtitle { margin: 3px 0 0; font-size: 0.83rem; opacity: 0.6; line-height: 1.5; }
    .header-badge {
      position: relative;
      background: rgba(255,255,255,0.08);
      border: 1px solid rgba(255,255,255,0.15);
      border-radius: 8px;
      padding: 10px 18px;
      font-size: 0.78rem;
      color: rgba(255,255,255,0.7);
      line-height: 1.6;
      flex-shrink: 0;
      white-space: nowrap;
    }
    .header-badge strong { color: #c4b5fd; display: block; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 2px; }

    /* ── Two-column body ── */
    .page-body {
      display: grid;
      grid-template-columns: 400px 1fr;
      min-height: 560px;
    }

    /* ── Left: form pane ── */
    .form-pane {
      padding: 32px 32px 36px;
      border-right: 1px solid #e8edf2;
    }
    .pane-title {
      font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
      letter-spacing: 0.07em; color: #718096; margin: 0 0 18px;
    }
    label { display: block; font-size: 0.79rem; font-weight: 600; color: #4a5568; margin-bottom: 5px; text-transform: uppercase; letter-spacing: 0.04em; }
    .field { margin-bottom: 15px; }
    input[type="text"], input[type="password"], select {
      width: 100%; padding: 9px 12px;
      border: 1px solid #e2e8f0; border-radius: 7px;
      font-size: 0.87rem; background: #f8fafc; color: #1a202c;
      outline: none; transition: border-color 0.15s, box-shadow 0.15s;
      appearance: none; -webkit-appearance: none;
    }
    select {
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='8' viewBox='0 0 12 8'%3E%3Cpath d='M1 1l5 5 5-5' stroke='%23718096' stroke-width='1.5' fill='none' stroke-linecap='round'/%3E%3C/svg%3E");
      background-repeat: no-repeat;
      background-position: right 12px center;
      padding-right: 32px;
      cursor: pointer;
    }
    input:focus, select:focus { border-color: #7c3aed; background: #fff; box-shadow: 0 0 0 3px rgba(124,58,237,0.11); }
    .region-hint { font-size: 0.72rem; color: #a0aec0; margin-top: 5px; }
    .region-hint a { color: #7c3aed; }
    .divider { border: none; border-top: 1px solid #e2e8f0; margin: 18px 0; }
    button[type="submit"] {
      width: 100%; padding: 11px;
      background: linear-gradient(135deg, #1a1a2e 0%, #4c1d95 100%);
      color: white; border: none; border-radius: 8px;
      font-size: 0.92rem; font-weight: 600; cursor: pointer;
      transition: opacity 0.15s;
    }
    button[type="submit"]:hover:not(:disabled) { opacity: 0.87; }
    button[type="submit"]:disabled { opacity: 0.5; cursor: not-allowed; }

    /* ── Progress ── */
    #progress { display: none; margin-top: 20px; }
    .progress-header { display: flex; align-items: center; gap: 8px; font-size: 0.81rem; font-weight: 600; color: #4a5568; margin-bottom: 11px; }
    .spinner { width: 13px; height: 13px; border: 2px solid #e2e8f0; border-top-color: #7c3aed; border-radius: 50%; animation: spin 0.8s linear infinite; flex-shrink: 0; }
    @keyframes spin { to { transform: rotate(360deg); } }
    .steps { list-style: none; margin: 0 0 12px; padding: 0; display: flex; flex-direction: column; background: #f8fafc; border-radius: 8px; border: 1px solid #e2e8f0; overflow: hidden; }
    .step { display: flex; align-items: center; gap: 9px; padding: 8px 13px; font-size: 0.82rem; color: #a0aec0; border-bottom: 1px solid #e2e8f0; transition: background 0.2s, color 0.2s; }
    .step:last-child { border-bottom: none; }
    .step.active { color: #1a202c; background: #fff; font-weight: 600; }
    .step.done   { color: #276749; background: #f0fff4; }
    .step.error  { color: #9b2c2c; background: #fff5f5; }
    .step-num    { width: 19px; height: 19px; border-radius: 50%; background: #e2e8f0; color: #718096; font-size: 0.68rem; font-weight: 700; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }
    .step.active .step-num { background: #7c3aed; color: white; }
    .step-spinner { width: 12px; height: 12px; border: 2px solid #d6bcfa; border-top-color: #7c3aed; border-radius: 50%; animation: spin 0.8s linear infinite; }
    .step-label  { flex: 1; }
    .step-detail { font-size: 0.71rem; color: #a0aec0; margin-left: auto; white-space: nowrap; }
    .step.active .step-detail { color: #7c3aed; }
    #log { background: #0f0f1a; border-radius: 7px; padding: 10px 13px; max-height: 160px; overflow-y: auto; font-family: "SFMono-Regular", Consolas, monospace; font-size: 0.72rem; color: #718096; line-height: 1.65; }
    #log .line::before { content: '› '; color: #7c3aed; }
    #error-msg { display: none; background: #fff5f5; border: 1px solid #fc8181; border-radius: 8px; padding: 11px 15px; color: #c53030; font-size: 0.83rem; margin-top: 14px; line-height: 1.5; }

    /* ── Right: docs pane ── */
    .docs-pane {
      padding: 32px 36px 36px;
      background: #fafbfc;
    }

    /* Section headings */
    .doc-section { margin-bottom: 28px; }
    .doc-section:last-child { margin-bottom: 0; }
    .doc-section-title {
      font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
      letter-spacing: 0.07em; color: #718096;
      margin: 0 0 12px;
      display: flex; align-items: center; gap: 8px;
    }
    .doc-section-title::after {
      content: ''; flex: 1; height: 1px; background: #e8edf2;
    }

    /* ── Feature cards ── */
    .feature-cards { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
    .feature-card {
      background: #fff;
      border: 1px solid #e8edf2;
      border-radius: 9px;
      padding: 14px 15px 15px;
    }
    .feature-icon {
      width: 32px; height: 32px; border-radius: 8px;
      display: flex; align-items: center; justify-content: center;
      font-size: 1rem; margin-bottom: 9px;
    }
    .feature-icon.blast  { background: #ede9fe; }
    .feature-icon.drift  { background: #fef3c7; }
    .feature-icon.formula { background: #dcfce7; }
    .feature-name { font-size: 0.82rem; font-weight: 700; color: #1a202c; margin: 0 0 4px; }
    .feature-desc { font-size: 0.76rem; color: #718096; line-height: 1.5; margin: 0; }

    /* ── API table ── */
    .api-table { width: 100%; border-collapse: collapse; font-size: 0.78rem; }
    .api-table th {
      text-align: left; padding: 6px 10px;
      font-size: 0.7rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em;
      color: #a0aec0; border-bottom: 1px solid #e8edf2;
    }
    .api-table td { padding: 7px 10px; border-bottom: 1px solid #f0f4f8; vertical-align: top; }
    .api-table tr:last-child td { border-bottom: none; }
    .api-table tr:hover td { background: #f7f9fc; }
    .method { font-family: "SFMono-Regular", Consolas, monospace; font-size: 0.68rem; font-weight: 700; padding: 2px 6px; border-radius: 4px; white-space: nowrap; }
    .method.get  { background: #dbeafe; color: #1e40af; }
    .method.post { background: #dcfce7; color: #166534; }
    .method.put  { background: #fef9c3; color: #854d0e; }
    .endpoint { font-family: "SFMono-Regular", Consolas, monospace; font-size: 0.72rem; color: #4a5568; word-break: break-all; }
    .endpoint-desc { color: #4a5568; line-height: 1.45; }

    /* ── CI snippet ── */
    .ci-snippet {
      background: #0f0f1a;
      border-radius: 9px;
      overflow: hidden;
    }
    .ci-snippet-bar {
      background: #1a1a2e;
      padding: 7px 14px;
      display: flex; align-items: center; justify-content: space-between;
      border-bottom: 1px solid rgba(255,255,255,0.06);
    }
    .ci-snippet-label { font-size: 0.71rem; color: #718096; font-family: "SFMono-Regular", Consolas, monospace; }
    .ci-snippet pre {
      margin: 0; padding: 14px 16px;
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 0.72rem; line-height: 1.7; color: #a0aec0;
      overflow-x: auto; white-space: pre;
    }
    .ci-snippet .kw  { color: #c4b5fd; }
    .ci-snippet .str { color: #86efac; }
    .ci-snippet .cmt { color: #4a5568; }
    .ci-snippet .key { color: #93c5fd; }

    /* ── Permissions note ── */
    .permissions-note {
      margin-top: 10px;
      background: #fff;
      border: 1px solid #e8edf2;
      border-radius: 8px;
      padding: 11px 14px;
      font-size: 0.76rem;
      color: #4a5568;
      line-height: 1.55;
    }
    .permissions-note strong { color: #1a202c; }

    /* ── Responsive ── */
    @media (max-width: 800px) {
      .page-body { grid-template-columns: 1fr; }
      .form-pane { border-right: none; border-bottom: 1px solid #e8edf2; }
      .feature-cards { grid-template-columns: 1fr; }
      .page-header { flex-direction: column; align-items: flex-start; }
      .header-badge { display: none; }
    }
  </style>
</head>
<body>
  <div class="page-shell">

    <!-- ── Full-width header ── -->
    <header class="page-header">
      <div class="header-left">
        <svg class="logo-svg" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 30 35" width="34" height="40" fill="none">
          <path d="M15 1.5 L27 6 L27 20 C27 28 21 33 15 35 C9 33 3 28 3 20 L3 6 Z"
                fill="rgba(255,255,255,0.17)" stroke="rgba(255,255,255,0.42)" stroke-width="1.2" stroke-linejoin="round"/>
          <text x="15" y="22.5" text-anchor="middle" fill="white" font-size="14" font-weight="800"
                font-family="Georgia,'Times New Roman',serif">&#x3A3;</text>
        </svg>
        <div>
          <h1 class="header-title">Sigma Sentinel</h1>
          <p class="header-subtitle">Always-on validation for Sigma data models — schema drift, formula integrity, and blast radius.</p>
        </div>
      </div>
      <div class="header-badge">
        <strong>Checks</strong>
        Blast radius &nbsp;·&nbsp; Schema drift &nbsp;·&nbsp; Formula refs
      </div>
    </header>

    <div class="page-body">

      <!-- ── Left: credential form + progress ── -->
      <div class="form-pane">
        <p class="pane-title">Connect your org</p>
        <form id="form">
          <div class="field">
            <label for="clientId">Client ID</label>
            <input id="clientId" type="text" placeholder="Your Sigma Client ID" required autocomplete="off" />
          </div>
          <div class="field">
            <label for="clientSecret">Client Secret</label>
            <input id="clientSecret" type="password" placeholder="Your Sigma Client Secret" required autocomplete="off" />
          </div>
          <div class="field">
            <label for="baseUrl">API Region</label>
            <select id="baseUrl">
              <optgroup label="GCP">
                <option value="https://api.sigmacomputing.com">api.sigmacomputing.com — GCP US</option>
                <option value="https://api.sa.gcp.sigmacomputing.com">api.sa.gcp.sigmacomputing.com — GCP KSA</option>
              </optgroup>
              <optgroup label="AWS">
                <option value="https://aws-api.sigmacomputing.com" selected>aws-api.sigmacomputing.com — AWS US West</option>
                <option value="https://api.us-a.aws.sigmacomputing.com">api.us-a.aws.sigmacomputing.com — AWS US East</option>
                <option value="https://api.ca.aws.sigmacomputing.com">api.ca.aws.sigmacomputing.com — AWS Canada</option>
                <option value="https://api.eu.aws.sigmacomputing.com">api.eu.aws.sigmacomputing.com — AWS Europe</option>
                <option value="https://api.au.aws.sigmacomputing.com">api.au.aws.sigmacomputing.com — AWS Australia / APAC</option>
                <option value="https://api.uk.aws.sigmacomputing.com">api.uk.aws.sigmacomputing.com — AWS UK</option>
              </optgroup>
              <optgroup label="Azure">
                <option value="https://api.us.azure.sigmacomputing.com">api.us.azure.sigmacomputing.com — Azure US</option>
                <option value="https://api.eu.azure.sigmacomputing.com">api.eu.azure.sigmacomputing.com — Azure Europe</option>
                <option value="https://api.ca.azure.sigmacomputing.com">api.ca.azure.sigmacomputing.com — Azure Canada</option>
                <option value="https://api.uk.azure.sigmacomputing.com">api.uk.azure.sigmacomputing.com — Azure UK</option>
              </optgroup>
            </select>
            <p class="region-hint">API base URL for your org — not the app URL. <a href="https://help.sigmacomputing.com/reference/get-started-sigma-api#identify-your-api-request-url" target="_blank" rel="noopener">How to find yours ↗</a></p>
          </div>
          <hr class="divider" />
          <button type="submit" id="runBtn">Run Validation</button>
        </form>

        <div id="progress">
          <div class="progress-header">
            <div class="spinner"></div>
            <span>Validation in progress…</span>
          </div>
          <ol class="steps">
            <li class="step" id="step-0"><span class="step-num">1</span><span class="step-label">Authenticate</span><span class="step-detail" id="step-detail-0"></span></li>
            <li class="step" id="step-1"><span class="step-num">2</span><span class="step-label">Discover models</span><span class="step-detail" id="step-detail-1"></span></li>
            <li class="step" id="step-2"><span class="step-num">3</span><span class="step-label">Blast radius</span><span class="step-detail" id="step-detail-2"></span></li>
            <li class="step" id="step-3"><span class="step-num">4</span><span class="step-label">Schema drift</span><span class="step-detail" id="step-detail-3"></span></li>
            <li class="step" id="step-4"><span class="step-num">5</span><span class="step-label">Formula check</span><span class="step-detail" id="step-detail-4"></span></li>
            <li class="step" id="step-5"><span class="step-num">6</span><span class="step-label">Generate report</span><span class="step-detail" id="step-detail-5"></span></li>
          </ol>
          <div id="log"></div>
        </div>
        <div id="error-msg"></div>
      </div>

      <!-- ── Right: documentation pane ── -->
      <div class="docs-pane">

        <!-- What it checks -->
        <div class="doc-section">
          <h2 class="doc-section-title">What Sentinel checks</h2>
          <div class="feature-cards">
            <div class="feature-card">
              <div class="feature-icon blast">💥</div>
              <p class="feature-name">Blast Radius</p>
              <p class="feature-desc">Traces which workbooks — direct and transitive — would be affected if a data model changes. Surfaces model-to-model dependencies so you know the full impact before merging.</p>
            </div>
            <div class="feature-card">
              <div class="feature-icon drift">🏔️</div>
              <p class="feature-name">Schema Drift</p>
              <p class="feature-desc">Compares the columns your models reference against Sigma's live warehouse schema cache. Before checking, it syncs the cache for every referenced table so stale metadata can't cause false positives.</p>
            </div>
            <div class="feature-card">
              <div class="feature-icon formula">🔗</div>
              <p class="feature-name">Formula Integrity</p>
              <p class="feature-desc">Resolves every column and metric reference in your model formulas against the authoritative columns API. Broken references are flagged with suggested fixes where a close match exists.</p>
            </div>
          </div>
        </div>

        <!-- APIs used -->
        <div class="doc-section">
          <h2 class="doc-section-title">APIs used</h2>
          <table class="api-table">
            <thead>
              <tr>
                <th>Method</th>
                <th>Endpoint</th>
                <th>Purpose</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td><span class="method get">GET</span></td>
                <td class="endpoint">/v2/dataModels</td>
                <td class="endpoint-desc">List all data models in the org</td>
              </tr>
              <tr>
                <td><span class="method get">GET</span></td>
                <td class="endpoint">/v2/dataModels/{id}/lineage</td>
                <td class="endpoint-desc">Build model-to-model dependency graph and detect upstream connections</td>
              </tr>
              <tr>
                <td><span class="method get">GET</span></td>
                <td class="endpoint">/v2/dataModels/{id}/spec</td>
                <td class="endpoint-desc">Read element structure, column formulas, and SQL sources</td>
              </tr>
              <tr>
                <td><span class="method get">GET</span></td>
                <td class="endpoint">/v2/dataModels/{id}/columns</td>
                <td class="endpoint-desc">Resolve formula references against authoritative column names</td>
              </tr>
              <tr>
                <td><span class="method get">GET</span></td>
                <td class="endpoint">/v2/workbooks</td>
                <td class="endpoint-desc">Enumerate workbooks for blast radius and direct-source analysis</td>
              </tr>
              <tr>
                <td><span class="method get">GET</span></td>
                <td class="endpoint">/v2/workbooks/{id}/sources</td>
                <td class="endpoint-desc">Identify each workbook's source type to map blast radius and pre-filter direct-source candidates</td>
              </tr>
              <tr>
                <td><span class="method get">GET</span></td>
                <td class="endpoint">/v2/workbooks/{id}/lineage</td>
                <td class="endpoint-desc">Fetch element-level lineage for workbooks with direct warehouse or custom SQL sources</td>
              </tr>
              <tr>
                <td><span class="method get">GET</span></td>
                <td class="endpoint">/v2/connections/tables/{id}/columns</td>
                <td class="endpoint-desc">Fetch warehouse column lists for schema drift comparison</td>
              </tr>
              <tr>
                <td><span class="method post">POST</span></td>
                <td class="endpoint">/v2/connections/{id}/sync</td>
                <td class="endpoint-desc">Refresh Sigma's schema cache before drift check to prevent stale-metadata false positives</td>
              </tr>
              <tr>
                <td><span class="method put">PUT</span></td>
                <td class="endpoint">/v2/dataModels/{id}/spec</td>
                <td class="endpoint-desc">Write back fixed specs when applying one-click drift repairs</td>
              </tr>
            </tbody>
          </table>
          <div class="permissions-note">
            <strong>Required permissions:</strong> The client credential needs read access to data models, workbooks, and connections. Write access is only required if you use the one-click drift fix feature.
          </div>
        </div>

        <!-- CI integration -->
        <div class="doc-section">
          <h2 class="doc-section-title">GitHub Actions integration</h2>
          <div class="ci-snippet">
            <div class="ci-snippet-bar">
              <span class="ci-snippet-label">.github/workflows/sigma-sentinel.yml</span>
            </div>
            <pre><span class="key">name:</span> <span class="str">Sigma Sentinel</span>
<span class="key">on:</span>
  <span class="key">pull_request:</span>
  <span class="key">schedule:</span>
    <span class="cmt"># Run daily at 08:00 UTC</span>
    - <span class="key">cron:</span> <span class="str">'0 8 * * *'</span>

<span class="key">jobs:</span>
  <span class="key">validate:</span>
    <span class="key">runs-on:</span> <span class="str">ubuntu-latest</span>
    <span class="key">steps:</span>
      - <span class="key">uses:</span> <span class="str">actions/checkout@v4</span>
      - <span class="key">name:</span> <span class="str">Run Sigma Sentinel</span>
        <span class="key">uses:</span> <span class="str">twells89/sigma-ci@main</span>
        <span class="key">with:</span>
          <span class="key">sigma-client-id:</span>     <span class="str">\${{ secrets.SIGMA_CLIENT_ID }}</span>
          <span class="key">sigma-client-secret:</span> <span class="str">\${{ secrets.SIGMA_CLIENT_SECRET }}</span>
          <span class="key">fail-on-drift:</span>       <span class="str">'true'</span>
          <span class="key">fail-on-formula-errors:</span> <span class="str">'false'</span></pre>
          </div>
        </div>

      </div><!-- /.docs-pane -->
    </div><!-- /.page-body -->
  </div><!-- /.page-shell -->

  <script>
    const form       = document.getElementById('form');
    const runBtn     = document.getElementById('runBtn');
    const progressEl = document.getElementById('progress');
    const logEl      = document.getElementById('log');
    const errorEl    = document.getElementById('error-msg');

    const STEP_KEYWORDS = [
      ['authenticat'],
      ['fetching data model', 'found ', 'data model'],
      ['blast', 'content', 'workbook', 'scanning'],
      ['drift', 'schema'],
      ['formula'],
      ['report', 'generating'],
    ];
    let currentStep = -1;

    function advanceStep(text) {
      const lower = text.toLowerCase();
      for (let i = currentStep + 1; i < STEP_KEYWORDS.length; i++) {
        if (STEP_KEYWORDS[i].some(kw => lower.includes(kw))) {
          if (currentStep >= 0) {
            const prev = document.getElementById('step-' + currentStep);
            if (prev) {
              prev.classList.remove('active');
              prev.classList.add('done');
              const n = prev.querySelector('.step-num');
              if (n) n.textContent = '✓';
            }
          }
          const el = document.getElementById('step-' + i);
          if (el) {
            el.classList.add('active');
            const numEl = el.querySelector('.step-num');
            if (numEl) { numEl.innerHTML = '<span class="step-spinner"></span>'; numEl.style.background = 'transparent'; }
          }
          currentStep = i;
          const mDetail = text.match(/model (\\d+\\/\\d+)/i) || text.match(/(\\d+ .+found)/i);
          if (mDetail) {
            const det = document.getElementById('step-detail-' + i);
            if (det) det.textContent = mDetail[1];
          }
          break;
        }
      }
      if (currentStep >= 0 && STEP_KEYWORDS[currentStep].some(kw => lower.includes(kw))) {
        const mDetail = text.match(/model (\\d+\\/\\d+)/i) || text.match(/(\\d+ .+found)/i);
        if (mDetail) {
          const det = document.getElementById('step-detail-' + currentStep);
          if (det) det.textContent = mDetail[1];
        }
      }
    }

    function addLog(text) {
      const line = document.createElement('div');
      line.className = 'line';
      line.textContent = text;
      logEl.appendChild(line);
      logEl.scrollTop = logEl.scrollHeight;
    }

    function markStepError() {
      if (currentStep >= 0) {
        const el = document.getElementById('step-' + currentStep);
        if (el) {
          el.classList.remove('active');
          el.classList.add('error');
          const numEl = el.querySelector('.step-num');
          if (numEl) { numEl.innerHTML = '✗'; numEl.style.background = '#fc8181'; numEl.style.color = '#fff'; }
        }
      }
    }

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      runBtn.disabled = true;
      runBtn.textContent = 'Validating…';
      progressEl.style.display = 'block';
      errorEl.style.display = 'none';
      logEl.innerHTML = '';
      currentStep = -1;
      for (let i = 0; i < 6; i++) {
        const el = document.getElementById('step-' + i);
        if (el) {
          el.className = 'step';
          const numEl = el.querySelector('.step-num');
          if (numEl) { numEl.innerHTML = (i + 1).toString(); numEl.style.background = ''; numEl.style.color = ''; }
          const det = document.getElementById('step-detail-' + i);
          if (det) det.textContent = '';
        }
      }

      const body = {
        clientId: document.getElementById('clientId').value,
        clientSecret: document.getElementById('clientSecret').value,
        baseUrl: document.getElementById('baseUrl').value,
      };

      let pollTimer = null;

      function abort(msg) {
        if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
        markStepError();
        errorEl.textContent = 'Error: ' + msg;
        errorEl.style.display = 'block';
        runBtn.disabled = false;
        runBtn.textContent = 'Run Validation';
      }

      try {
        const resp = await fetch('/api/validate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        if (!resp.ok) throw new Error(await resp.text());
        const { jobId } = await resp.json();

        let lastStepCount = 0;
        pollTimer = setInterval(async () => {
          try {
            const sr = await fetch('/api/status/' + jobId);
            if (!sr.ok) return;
            const job = await sr.json();
            for (let i = lastStepCount; i < (job.steps || []).length; i++) {
              advanceStep(job.steps[i]);
              addLog(job.steps[i]);
            }
            lastStepCount = (job.steps || []).length;
            if (job.status === 'done' && job.redirectUrl) {
              clearInterval(pollTimer);
              window.location.href = job.redirectUrl;
            } else if (job.status === 'error') {
              abort(job.error || 'Unknown error');
            }
          } catch (_) {}
        }, 1000);

      } catch (err) {
        abort(err.message);
      }
    });
  </script>
</body>
</html>`;

// ─── Routes ───────────────────────────────────────────────────────────────────

app.get("/", (_req, res) => {
  res.send(LANDING_HTML);
});

// POST /api/validate — starts a background job, returns { jobId } immediately.
// The client polls /api/status/:jobId every second for progress.
// This avoids SSE entirely: Render's proxy buffers streaming responses until
// the connection closes, making real-time SSE progress impossible there.
app.post("/api/validate", (req, res) => {
  const { clientId, clientSecret, baseUrl } = req.body as {
    clientId?: string;
    clientSecret?: string;
    baseUrl?: string;
  };

  if (!clientId || !clientSecret) {
    res.status(400).json({ error: "clientId and clientSecret are required." });
    return;
  }

  const jobId = randomUUID();
  const job: Job = { status: "running", steps: [], createdAt: Date.now() };
  jobs.set(jobId, job);

  // Fire-and-forget validation in the background
  void (async () => {
    const addStep = (msg: string) => { job.steps.push(msg); };
    const client = new SigmaClient({ clientId, clientSecret, baseUrl });
    try {
      addStep("Authenticating with Sigma…");
      await client.authenticate();

      addStep("Fetching data models…");
      const models = await client.listDataModels();
      addStep(`Found ${models.length} data model(s). Starting validation…`);

      const modelIds = models.map((m) => m.dataModelId);
      const modelUrlMap = new Map(models.map((m) => [m.dataModelId, m.url ?? ""]));

      addStep("Running content validation (blast radius + dependency graph)…");
      const contentReport = await runContentValidation(client, models, modelUrlMap);

      addStep("Running schema drift validation…");
      const driftReport = await runSchemaDriftValidation(client, modelIds, modelUrlMap, addStep);

      addStep("Checking formula references…");
      const formulaReport = await runFormulaCheck(client, models, modelUrlMap, addStep);
      client.clearCaches();

      addStep("Scanning workbooks for direct warehouse and custom SQL sources…");
      const directSourceReport = await runWorkbookDirectSourceCheck(client, modelUrlMap, addStep);

      addStep("Resolving member names…");
      const members = await client.listMembers();
      const memberMap: MemberMap = new Map(members.map((m) => [
        m.memberId,
        { name: [m.firstName, m.lastName].filter(Boolean).join(" "), email: m.email ?? "" },
      ]));

      const sessionId = randomUUID();
      sessions.set(sessionId, { clientId, clientSecret, baseUrl, createdAt: Date.now() });

      addStep("Generating report…");
      const html = toHtmlReport(contentReport, driftReport, { sessionId, formulaReport, directSourceReport, memberMap });

      const reportId = randomUUID();
      reports.set(reportId, { html, createdAt: Date.now() });

      job.redirectUrl = `/r/${reportId}`;
      job.status = "done";
    } catch (err) {
      job.error = (err as Error).message;
      job.status = "error";
    }
  })();

  res.json({ jobId });
});

// GET /api/status/:jobId — returns current job state for the polling client
app.get("/api/status/:jobId", (req, res) => {
  const job = jobs.get(req.params["jobId"] ?? "");
  if (!job) {
    res.status(404).json({ error: "Job not found or expired." });
    return;
  }
  res.json({
    status: job.status,
    steps: job.steps,
    redirectUrl: job.redirectUrl ?? null,
    error: job.error ?? null,
  });
});

app.get("/r/:reportId", (req, res) => {
  const report = reports.get(req.params["reportId"] ?? "");
  if (!report) {
    res.status(404).send("Report not found or expired. Please re-run validation.");
    return;
  }
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.send(report.html);
});

app.post("/api/fix", async (req, res) => {
  const { sessionId, modelId } = req.body as { sessionId?: string; modelId?: string };

  if (!sessionId || !modelId) {
    res.status(400).json({ success: false, error: "sessionId and modelId are required." });
    return;
  }

  const session = sessions.get(sessionId);
  if (!session) {
    res.status(401).json({ success: false, error: "Session expired. Please re-run validation." });
    return;
  }

  const client = new SigmaClient({
    clientId: session.clientId,
    clientSecret: session.clientSecret,
    baseUrl: session.baseUrl,
  });

  try {
    await client.authenticate();
    const result = await applyDriftFix(client, modelId);
    res.json(result);
  } catch (err) {
    res.status(500).json({ success: false, error: (err as Error).message });
  }
});

app.post("/api/fix-formula", async (req, res) => {
  const { sessionId, modelId, elementId, columnId, brokenRef, newRef } = req.body as {
    sessionId?: string;
    modelId?: string;
    elementId?: string;
    columnId?: string;
    brokenRef?: string;
    newRef?: string;
  };

  if (!sessionId || !modelId || !elementId || !columnId || !brokenRef || !newRef) {
    res.status(400).json({ success: false, error: "Missing required fields." });
    return;
  }

  const session = sessions.get(sessionId);
  if (!session) {
    res.status(401).json({ success: false, error: "Session expired. Please re-run validation." });
    return;
  }

  const client = new SigmaClient({
    clientId: session.clientId,
    clientSecret: session.clientSecret,
    baseUrl: session.baseUrl,
  });

  try {
    await client.authenticate();
    const spec = await client.getDataModelSpec(modelId);

    let applied = false;
    for (const page of spec.pages ?? []) {
      for (const element of page.elements ?? []) {
        if (element.id !== elementId) continue;

        // Search columns and metrics
        const allItems = [
          ...(element.columns ?? []),
          ...(element.metrics ?? []),
        ];

        for (const item of allItems) {
          if (item.id !== columnId) continue;
          if (!item.formula) continue;

          // Case-insensitive replacement of [brokenRef] → [newRef]
          const pattern = new RegExp(
            `\\[${brokenRef.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\]`,
            "gi"
          );
          const updated = item.formula.replace(pattern, `[${newRef}]`);
          if (updated !== item.formula) {
            item.formula = updated;
            applied = true;
          }
        }
      }
    }

    if (!applied) {
      res.json({ success: false, error: "Reference not found in formula — may already be fixed." });
      return;
    }

    await client.updateDataModelSpec(modelId, spec);
    res.json({ success: true, brokenRef, newRef });
  } catch (err) {
    res.status(500).json({ success: false, error: (err as Error).message });
  }
});

// ─── Start ────────────────────────────────────────────────────────────────────

const PORT = process.env["PORT"] ? parseInt(process.env["PORT"]) : 3000;
app.listen(PORT, () => {
  console.log(`Sigma Sentinel running on http://localhost:${PORT}`);
});
