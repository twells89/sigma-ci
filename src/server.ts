import express from "express";
import { randomUUID } from "crypto";
import { SigmaClient } from "./sigma-client.js";
import { runContentValidation } from "./validators/content.js";
import { runSchemaDriftValidation } from "./validators/schema-drift.js";
import { applyDriftFix } from "./validators/drift-fix.js";
import { runFormulaCheck } from "./validators/formula-check.js";
import { toHtmlReport } from "./report.js";

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

setInterval(() => {
  const now = Date.now();
  for (const [id, s] of sessions) {
    if (now - s.createdAt > 3_600_000) sessions.delete(id);
  }
  for (const [id, r] of reports) {
    if (now - r.createdAt > 7_200_000) reports.delete(id);
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
      align-items: flex-start;
      justify-content: center;
      padding: 48px 16px;
      color: #1a202c;
    }
    .card {
      background: #fff;
      border-radius: 12px;
      box-shadow: 0 4px 24px rgba(0,0,0,0.10);
      width: 100%;
      max-width: 520px;
      overflow: hidden;
    }
    .card-header {
      background: linear-gradient(135deg, #0c0c1e 0%, #1a0f40 55%, #2d1b6b 100%);
      padding: 26px 36px 22px;
      color: white;
      position: relative;
      overflow: hidden;
    }
    .card-header::before {
      content: '';
      position: absolute;
      width: 220px; height: 220px;
      background: radial-gradient(circle, rgba(139,92,246,0.18) 0%, transparent 65%);
      top: -70px; right: -50px;
      pointer-events: none;
    }
    .logo { display: flex; align-items: center; gap: 12px; margin-bottom: 6px; position: relative; }
    .logo-svg { flex-shrink: 0; }
    @keyframes sentinel-glow {
      0%, 100% { filter: drop-shadow(0 0 0px rgba(167,139,250,0)); }
      50%       { filter: drop-shadow(0 0 6px rgba(167,139,250,0.4)); }
    }
    .logo-svg { animation: sentinel-glow 4s ease-in-out infinite; }
    h1 { font-size: 1.4rem; font-weight: 700; margin: 0; letter-spacing: -0.3px; }
    .subtitle { font-size: 0.84rem; margin: 0; opacity: 0.65; line-height: 1.5; }
    .card-body { padding: 28px 36px 32px; }

    /* ── Form ── */
    label { display: block; font-size: 0.8rem; font-weight: 600; color: #4a5568; margin-bottom: 5px; text-transform: uppercase; letter-spacing: 0.04em; }
    .field { margin-bottom: 16px; }
    input[type="text"], input[type="password"], select {
      width: 100%; padding: 9px 13px;
      border: 1px solid #e2e8f0; border-radius: 7px;
      font-size: 0.88rem; background: #f8fafc; color: #1a202c;
      outline: none; transition: border-color 0.15s, box-shadow 0.15s;
      appearance: none; -webkit-appearance: none;
    }
    select {
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='8' viewBox='0 0 12 8'%3E%3Cpath d='M1 1l5 5 5-5' stroke='%23718096' stroke-width='1.5' fill='none' stroke-linecap='round'/%3E%3C/svg%3E");
      background-repeat: no-repeat;
      background-position: right 13px center;
      padding-right: 34px;
      cursor: pointer;
    }
    input:focus, select:focus { border-color: #7c3aed; background: #fff; box-shadow: 0 0 0 3px rgba(124,58,237,0.12); }
    .region-hint { font-size: 0.73rem; color: #a0aec0; margin-top: 5px; }
    button[type="submit"] {
      width: 100%; padding: 12px;
      background: linear-gradient(135deg, #1a1a2e 0%, #4c1d95 100%);
      color: white; border: none; border-radius: 8px;
      font-size: 0.93rem; font-weight: 600; cursor: pointer;
      transition: opacity 0.15s;
      margin-top: 4px;
    }
    button[type="submit"]:hover:not(:disabled) { opacity: 0.88; }
    button[type="submit"]:disabled { opacity: 0.5; cursor: not-allowed; }
    .divider { border: none; border-top: 1px solid #e2e8f0; margin: 0 0 20px; }

    /* ── Progress area ── */
    #progress { display: none; margin-top: 22px; }
    .progress-header {
      display: flex; align-items: center; gap: 8px;
      font-size: 0.82rem; font-weight: 600; color: #4a5568;
      margin-bottom: 12px;
    }
    .spinner {
      width: 14px; height: 14px; border: 2px solid #e2e8f0;
      border-top-color: #7c3aed; border-radius: 50%;
      animation: spin 0.8s linear infinite; flex-shrink: 0;
    }
    @keyframes spin { to { transform: rotate(360deg); } }

    /* ── Step tracker ── */
    .steps { list-style: none; margin: 0 0 14px; padding: 0; display: flex; flex-direction: column; gap: 2px; background: #f8fafc; border-radius: 8px; border: 1px solid #e2e8f0; overflow: hidden; }
    .step  {
      display: flex; align-items: center; gap: 10px;
      padding: 9px 14px;
      font-size: 0.83rem; color: #a0aec0;
      border-bottom: 1px solid #e2e8f0; transition: background 0.2s, color 0.2s;
    }
    .step:last-child { border-bottom: none; }
    .step.active { color: #1a202c; background: #fff; font-weight: 600; }
    .step.done   { color: #276749; background: #f0fff4; }
    .step.error  { color: #9b2c2c; background: #fff5f5; }
    .step-icon   { width: 22px; height: 22px; display: flex; align-items: center; justify-content: center; font-size: 0.85rem; flex-shrink: 0; }
    .step-num    { width: 20px; height: 20px; border-radius: 50%; background: #e2e8f0; color: #718096; font-size: 0.7rem; font-weight: 700; display: flex; align-items: center; justify-content: center; }
    .step.active .step-num { background: #7c3aed; color: white; }
    .step-spinner { width: 13px; height: 13px; border: 2px solid #d6bcfa; border-top-color: #7c3aed; border-radius: 50%; animation: spin 0.8s linear infinite; }
    .step-label  { flex: 1; }
    .step-detail { font-size: 0.73rem; color: #a0aec0; margin-left: auto; white-space: nowrap; }
    .step.active .step-detail { color: #7c3aed; }

    /* ── Log ── */
    #log {
      background: #0f0f1a; border-radius: 8px;
      padding: 12px 14px; max-height: 200px; overflow-y: auto;
      font-family: "SFMono-Regular", Consolas, monospace; font-size: 0.74rem;
      color: #718096; line-height: 1.65;
    }
    #log .line::before { content: '› '; color: #7c3aed; }
    #error-msg {
      display: none; background: #fff5f5; border: 1px solid #fc8181;
      border-radius: 8px; padding: 12px 16px; color: #c53030;
      font-size: 0.84rem; margin-top: 16px; line-height: 1.5;
    }
  </style>
</head>
<body>
  <div class="card">
    <div class="card-header">
      <div class="logo">
        <svg class="logo-svg" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 30 35" width="30" height="35" fill="none">
          <path d="M15 1.5 L27 6 L27 20 C27 28 21 33 15 35 C9 33 3 28 3 20 L3 6 Z"
                fill="rgba(255,255,255,0.17)" stroke="rgba(255,255,255,0.42)" stroke-width="1.2" stroke-linejoin="round"/>
          <text x="15" y="22.5" text-anchor="middle" fill="white" font-size="14" font-weight="800"
                font-family="Georgia,'Times New Roman',serif">&#x3A3;</text>
        </svg>
        <h1>Sigma Sentinel</h1>
      </div>
      <p class="subtitle">Always-on monitoring for Sigma data models — schema drift, formula integrity, and blast radius.</p>
    </div>

    <div class="card-body">
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
          <p class="region-hint">This is the API base URL for your org, not the app URL. <a href="https://help.sigmacomputing.com/reference/get-started-sigma-api#identify-your-api-request-url" target="_blank" rel="noopener" style="color:#7c3aed">How to find yours ↗</a></p>
        </div>

        <hr class="divider" />
        <button type="submit" id="runBtn">Run Validation</button>
      </form>

      <div id="progress">
        <div class="progress-header">
          <div class="spinner"></div>
          <span id="progress-label">Validation in progress…</span>
        </div>
        <ol class="steps" id="steps-list">
          <li class="step" id="step-0">
            <span class="step-icon"><span class="step-num">1</span></span>
            <span class="step-label">Authenticate</span>
            <span class="step-detail" id="step-detail-0"></span>
          </li>
          <li class="step" id="step-1">
            <span class="step-icon"><span class="step-num">2</span></span>
            <span class="step-label">Discover models</span>
            <span class="step-detail" id="step-detail-1"></span>
          </li>
          <li class="step" id="step-2">
            <span class="step-icon"><span class="step-num">3</span></span>
            <span class="step-label">Blast radius</span>
            <span class="step-detail" id="step-detail-2"></span>
          </li>
          <li class="step" id="step-3">
            <span class="step-icon"><span class="step-num">4</span></span>
            <span class="step-label">Schema drift</span>
            <span class="step-detail" id="step-detail-3"></span>
          </li>
          <li class="step" id="step-4">
            <span class="step-icon"><span class="step-num">5</span></span>
            <span class="step-label">Formula check</span>
            <span class="step-detail" id="step-detail-4"></span>
          </li>
          <li class="step" id="step-5">
            <span class="step-icon"><span class="step-num">6</span></span>
            <span class="step-label">Generate report</span>
            <span class="step-detail" id="step-detail-5"></span>
          </li>
        </ol>
        <div id="log"></div>
      </div>
      <div id="error-msg"></div>
    </div>
  </div>

  <script>
    const form       = document.getElementById('form');
    const runBtn     = document.getElementById('runBtn');
    const progressEl = document.getElementById('progress');
    const logEl      = document.getElementById('log');
    const errorEl    = document.getElementById('error-msg');

    // Step keyword matching — ordered to match the <ol> above
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
          // Mark previous step done
          if (currentStep >= 0) {
            const prev = document.getElementById('step-' + currentStep);
            if (prev) {
              prev.classList.remove('active');
              prev.classList.add('done');
              prev.querySelector('.step-num').textContent = '✓';
            }
          }
          // Activate new step
          const el = document.getElementById('step-' + i);
          if (el) {
            el.classList.add('active');
            const numEl = el.querySelector('.step-num');
            numEl.innerHTML = '<span class="step-spinner"></span>';
            numEl.style.background = 'transparent';
          }
          currentStep = i;
          // Extract a short detail string from text like "model 3/12"
          const mDetail = text.match(/model (\\d+\\/\\d+)/i) || text.match(/(\\d+ .+found)/i);
          if (mDetail) {
            const det = document.getElementById('step-detail-' + i);
            if (det) det.textContent = mDetail[1];
          }
          break;
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
          const numEl = el.querySelector('.step-num') || el.querySelector('.step-spinner')?.parentElement;
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
      // Reset steps
      for (let i = 0; i < 6; i++) {
        const el = document.getElementById('step-' + i);
        if (el) {
          el.className = 'step';
          const numEl = el.querySelector('.step-num') || el.querySelector('.step-spinner')?.parentElement;
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

      try {
        const resp = await fetch('/api/validate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });

        if (!resp.ok) throw new Error(await resp.text());

        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });
          const events = buf.split('\\n\\n');
          buf = events.pop();

          for (const block of events) {
            const eventLine = block.match(/^event: (.+)/m);
            const dataLine  = block.match(/^data: (.+)/m);
            if (!eventLine || !dataLine) continue;
            const eventType = eventLine[1].trim();
            const data = JSON.parse(dataLine[1]);

            if (eventType === 'progress') {
              advanceStep(data);
              addLog(data);
            } else if (eventType === 'redirect') {
              window.location.href = data;
              return;
            } else if (eventType === 'error') {
              markStepError();
              throw new Error(data);
            }
          }
        }
      } catch (err) {
        errorEl.textContent = 'Error: ' + err.message;
        errorEl.style.display = 'block';
        runBtn.disabled = false;
        runBtn.textContent = 'Run Validation';
      }
    });
  </script>
</body>
</html>`;

// ─── Routes ───────────────────────────────────────────────────────────────────

app.get("/", (_req, res) => {
  res.send(LANDING_HTML);
});

app.post("/api/validate", async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  const send = (event: string, data: unknown) => {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  };

  const { clientId, clientSecret, baseUrl } = req.body as {
    clientId?: string;
    clientSecret?: string;
    baseUrl?: string;
  };

  if (!clientId || !clientSecret) {
    send("error", "clientId and clientSecret are required.");
    res.end();
    return;
  }

  const client = new SigmaClient({ clientId, clientSecret, baseUrl });

  try {
    send("progress", "Authenticating with Sigma…");
    await client.authenticate();

    send("progress", "Fetching data models…");
    const models = await client.listDataModels();
    send("progress", `Found ${models.length} data model(s). Starting validation…`);

    const modelIds = models.map((m) => m.dataModelId);
    const modelUrlMap = new Map(models.map((m) => [m.dataModelId, m.url ?? ""]));

    send("progress", "Running content validation (blast radius + dependency graph)…");

    const [contentReport, driftReport] = await Promise.all([
      runContentValidation(client, models, modelUrlMap),
      runSchemaDriftValidation(client, modelIds, modelUrlMap, (msg) => send("progress", msg)),
    ]);

    send("progress", "Checking formula references…");
    const formulaReport = await runFormulaCheck(
      client, models, modelUrlMap, (msg) => send("progress", msg)
    );

    // Store session so fix buttons can call back
    const sessionId = randomUUID();
    sessions.set(sessionId, { clientId, clientSecret, baseUrl, createdAt: Date.now() });

    send("progress", "Generating report…");
    const html = toHtmlReport(contentReport, driftReport, { sessionId, formulaReport });

    // Store the HTML and send a redirect URL rather than the full HTML over SSE.
    // document.write() is unreliable for large documents; a normal GET request is not.
    const reportId = randomUUID();
    reports.set(reportId, { html, createdAt: Date.now() });
    send("redirect", `/r/${reportId}`);
  } catch (err) {
    send("error", (err as Error).message);
  }

  res.end();
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
