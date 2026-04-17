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

setInterval(() => {
  const now = Date.now();
  for (const [id, s] of sessions) {
    if (now - s.createdAt > 3_600_000) sessions.delete(id);
  }
}, 600_000);

// ─── Landing page ─────────────────────────────────────────────────────────────

const LANDING_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Sigma CI</title>
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
      padding: 40px 48px;
      width: 100%;
      max-width: 520px;
    }
    .logo { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }
    .logo-icon {
      width: 36px; height: 36px; border-radius: 8px;
      background: linear-gradient(135deg, #7B2FBE, #9F5FE8);
      display: flex; align-items: center; justify-content: center;
      color: white; font-weight: 700; font-size: 1.1rem;
    }
    h1 { font-size: 1.5rem; font-weight: 700; margin: 0; }
    .subtitle { color: #718096; font-size: 0.9rem; margin: 6px 0 32px; line-height: 1.5; }
    label { display: block; font-size: 0.85rem; font-weight: 600; color: #4a5568; margin-bottom: 6px; }
    .field { margin-bottom: 20px; }
    .optional { font-weight: 400; color: #a0aec0; }
    input[type="text"], input[type="password"] {
      width: 100%; padding: 10px 14px;
      border: 1px solid #e2e8f0; border-radius: 8px;
      font-size: 0.9rem; background: #f7fafc;
      outline: none; transition: border-color 0.15s;
    }
    input:focus { border-color: #7B2FBE; background: #fff; box-shadow: 0 0 0 3px rgba(123,47,190,0.1); }
    button[type="submit"] {
      width: 100%; padding: 12px;
      background: linear-gradient(135deg, #7B2FBE, #9F5FE8);
      color: white; border: none; border-radius: 8px;
      font-size: 0.95rem; font-weight: 600; cursor: pointer;
      transition: opacity 0.15s;
      margin-top: 4px;
    }
    button[type="submit"]:hover:not(:disabled) { opacity: 0.9; }
    button[type="submit"]:disabled { opacity: 0.6; cursor: not-allowed; }
    #progress { display: none; margin-top: 28px; }
    .progress-header {
      font-size: 0.85rem; font-weight: 600; color: #4a5568;
      margin-bottom: 8px; display: flex; align-items: center; gap: 8px;
    }
    .spinner {
      width: 14px; height: 14px; border: 2px solid #e2e8f0;
      border-top-color: #7B2FBE; border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    #log {
      background: #1a1a2e; border-radius: 8px;
      padding: 16px; max-height: 300px; overflow-y: auto;
      font-family: "SFMono-Regular", Consolas, monospace; font-size: 0.78rem;
      color: #a0aec0; line-height: 1.6;
    }
    #log .line::before { content: '› '; color: #7B2FBE; }
    #error-msg {
      display: none; background: #fff5f5; border: 1px solid #fc8181;
      border-radius: 8px; padding: 12px 16px; color: #c53030;
      font-size: 0.85rem; margin-top: 16px;
    }
    .features { display: flex; gap: 16px; margin-bottom: 28px; flex-wrap: wrap; }
    .feature { flex: 1; min-width: 120px; background: #f7fafc; border-radius: 8px; padding: 12px; font-size: 0.8rem; color: #4a5568; }
    .feature strong { display: block; color: #1a202c; margin-bottom: 2px; font-size: 0.82rem; }
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">
      <div class="logo-icon">Σ</div>
      <h1>Sigma CI</h1>
    </div>
    <p class="subtitle">Validate your Sigma data models — check downstream blast radius, detect schema drift, and fix broken column references.</p>

    <div class="features">
      <div class="feature"><strong>📊 Blast Radius</strong>Which workbooks break when you change a model</div>
      <div class="feature"><strong>🔍 Schema Drift</strong>Columns missing from the warehouse</div>
      <div class="feature"><strong>🔧 Auto-Fix</strong>Remove missing columns from models</div>
    </div>

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
        <label for="baseUrl">API Base URL <span class="optional">(optional)</span></label>
        <input id="baseUrl" type="text" placeholder="https://aws-api.sigmacomputing.com" />
      </div>
      <button type="submit" id="runBtn">Run Validation</button>
    </form>

    <div id="progress">
      <div class="progress-header">
        <div class="spinner"></div>
        <span>Validation in progress…</span>
      </div>
      <div id="log"></div>
    </div>
    <div id="error-msg"></div>
  </div>

  <script>
    const form = document.getElementById('form');
    const runBtn = document.getElementById('runBtn');
    const progressEl = document.getElementById('progress');
    const logEl = document.getElementById('log');
    const errorEl = document.getElementById('error-msg');

    function addLog(text) {
      const line = document.createElement('div');
      line.className = 'line';
      line.textContent = text;
      logEl.appendChild(line);
      logEl.scrollTop = logEl.scrollHeight;
    }

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      runBtn.disabled = true;
      runBtn.textContent = 'Validating…';
      progressEl.style.display = 'block';
      errorEl.style.display = 'none';
      logEl.innerHTML = '';

      const body = {
        clientId: document.getElementById('clientId').value,
        clientSecret: document.getElementById('clientSecret').value,
        baseUrl: document.getElementById('baseUrl').value || undefined,
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
            const dataLine = block.match(/^data: (.+)/m);
            if (!eventLine || !dataLine) continue;
            const eventType = eventLine[1].trim();
            const data = JSON.parse(dataLine[1]);

            if (eventType === 'progress') {
              addLog(data);
            } else if (eventType === 'done') {
              document.open();
              document.write(data);
              document.close();
              return;
            } else if (eventType === 'error') {
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
    send("done", html);
  } catch (err) {
    send("error", (err as Error).message);
  }

  res.end();
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
  console.log(`Sigma CI server running on http://localhost:${PORT}`);
});
