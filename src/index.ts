#!/usr/bin/env node

import { Command } from "commander";
import { writeFileSync, mkdtempSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { exec } from "child_process";
import { SigmaClient, DataModel } from "./sigma-client.js";
import { runContentValidation } from "./validators/content.js";
import { runSchemaDriftValidation } from "./validators/schema-drift.js";
import { runFormulaCheck } from "./validators/formula-check.js";
import { runWorkbookDirectSourceCheck } from "./validators/workbook-direct-source.js";
import {
  toJsonReport,
  toHtmlReport,
  toTextReport,
} from "./report.js";

type OutputFormat = "text" | "json" | "html";

function getClient(): SigmaClient {
  const clientId = process.env["SIGMA_CLIENT_ID"];
  const clientSecret = process.env["SIGMA_CLIENT_SECRET"];
  const baseUrl =
    process.env["SIGMA_BASE_URL"] ?? "https://aws-api.sigmacomputing.com";

  if (!clientId || !clientSecret) {
    console.error(
      "Error: SIGMA_CLIENT_ID and SIGMA_CLIENT_SECRET environment variables are required."
    );
    process.exit(1);
  }

  return new SigmaClient({ clientId, clientSecret, baseUrl });
}

async function resolveModels(
  client: SigmaClient,
  ids: string[],
  all: boolean
): Promise<{ models: DataModel[]; modelUrlMap: Map<string, string> }> {
  const allModels = await client.listDataModels();
  const modelUrlMap = new Map(allModels.map((m) => [m.dataModelId, m.url ?? ""]));

  if (all) {
    console.error("Fetching all data models...");
    if (allModels.length === 0) {
      console.error("No data models found in org.");
      return { models: [], modelUrlMap };
    }
    console.error(`Found ${allModels.length} data model(s).`);
    return { models: allModels, modelUrlMap };
  }
  const filtered = allModels.filter((m) => ids.includes(m.dataModelId));
  return { models: filtered, modelUrlMap };
}

function openInBrowser(html: string): void {
  const dir = mkdtempSync(join(tmpdir(), "sigma-ci-"));
  const file = join(dir, "report.html");
  writeFileSync(file, html, "utf8");
  const cmd =
    process.platform === "win32"
      ? `start "" "${file}"`
      : process.platform === "darwin"
      ? `open "${file}"`
      : `xdg-open "${file}"`;
  exec(cmd);
  console.error(`Report opened in browser: ${file}`);
}

function outputResult(
  format: OutputFormat,
  open: boolean,
  html: string,
  json: string,
  text: string
): void {
  if (open || format === "html") {
    const content = html;
    if (open) {
      openInBrowser(content);
    } else {
      process.stdout.write(content + "\n");
    }
  } else if (format === "json") {
    process.stdout.write(json + "\n");
  } else {
    process.stdout.write(text + "\n");
  }
}

const program = new Command();

program
  .name("sigma-ci")
  .description("CI/CD content validation for Sigma Computing data models")
  .version("0.1.0");

// ─── validate command ────────────────────────────────────────────────────────
program
  .command("validate")
  .description(
    "Validate data models: check downstream blast radius and schema drift"
  )
  .option(
    "-m, --model <id>",
    "Model ID to validate (can be repeated)",
    (val: string, prev: string[]) => [...prev, val],
    [] as string[]
  )
  .option("-a, --all", "Validate all models in the org")
  .option(
    "-f, --format <format>",
    "Output format: text | json | html",
    "text"
  )
  .option("--open", "Generate HTML report and open in browser")
  .option("--content-only", "Run only content validation")
  .option("--drift-only", "Run only schema drift validation")
  .option("--skip-sync", "Skip pre-syncing table schemas before drift check")
  .action(
    async (opts: {
      model: string[];
      all: boolean;
      format: string;
      open: boolean;
      contentOnly: boolean;
      driftOnly: boolean;
      skipSync: boolean;
    }) => {
      const format = opts.format as OutputFormat;

      if (opts.model.length === 0 && !opts.all) {
        console.error("Error: specify --model <id> or --all");
        process.exit(1);
      }

      const client = getClient();

      try {
        await client.authenticate();
      } catch (err) {
        console.error(`Authentication failed: ${(err as Error).message}`);
        process.exit(1);
      }

      const { models, modelUrlMap } = await resolveModels(client, opts.model, opts.all);
      if (models.length === 0) process.exit(0);
      const modelIds = models.map((m) => m.dataModelId);

      console.error(`Validating ${models.length} model(s)...`);

      const runContent = !opts.driftOnly;
      const runDrift = !opts.contentOnly;

      // Run sequentially to avoid concurrent API storms against the same rate-limit quota.
      const contentReport = runContent
        ? await runContentValidation(client, models, modelUrlMap)
        : { models: [], modelDependencies: {}, generatedAt: new Date().toISOString() };
      const driftReport = runDrift
        ? await runSchemaDriftValidation(client, modelIds, modelUrlMap, undefined, { skipSync: opts.skipSync })
        : { models: [], generatedAt: new Date().toISOString() };

      const formulaReport = await runFormulaCheck(client, models, modelUrlMap);
      const directSourceReport = await runWorkbookDirectSourceCheck(client, modelUrlMap);

      outputResult(
        format,
        opts.open,
        toHtmlReport(contentReport, driftReport, { formulaReport, directSourceReport }),
        toJsonReport(contentReport, driftReport, formulaReport, directSourceReport),
        toTextReport(contentReport, driftReport)
      );
    }
  );

// ─── report command ───────────────────────────────────────────────────────────
program
  .command("report")
  .description("Generate a full report for models in the org")
  .option(
    "-m, --model <id>",
    "Model ID (can be repeated)",
    (val: string, prev: string[]) => [...prev, val],
    [] as string[]
  )
  .option("-a, --all", "Report on all models in the org")
  .option(
    "-f, --format <format>",
    "Output format: text | json | html",
    "html"
  )
  .option("--open", "Open HTML report in browser (default for report command)")
  .option("--skip-sync", "Skip pre-syncing table schemas before drift check")
  .action(
    async (opts: { model: string[]; all: boolean; format: string; open: boolean; skipSync: boolean }) => {
      const format = opts.format as OutputFormat;

      if (opts.model.length === 0 && !opts.all) {
        console.error("Error: specify --model <id> or --all");
        process.exit(1);
      }

      const client = getClient();

      try {
        await client.authenticate();
      } catch (err) {
        console.error(`Authentication failed: ${(err as Error).message}`);
        process.exit(1);
      }

      const { models, modelUrlMap } = await resolveModels(client, opts.model, opts.all);
      if (models.length === 0) process.exit(0);
      const modelIds = models.map((m) => m.dataModelId);

      console.error(`Generating report for ${models.length} model(s)...`);

      // Run sequentially to avoid concurrent API storms against the same rate-limit quota.
      const contentReport = await runContentValidation(client, models, modelUrlMap);
      const driftReport = await runSchemaDriftValidation(client, modelIds, modelUrlMap, undefined, { skipSync: opts.skipSync });

      const formulaReport = await runFormulaCheck(client, models, modelUrlMap);
      const directSourceReport = await runWorkbookDirectSourceCheck(client, modelUrlMap);

      // report command defaults to open=true when format is html
      const shouldOpen = opts.open || format === "html";

      outputResult(
        format,
        shouldOpen,
        toHtmlReport(contentReport, driftReport, { formulaReport, directSourceReport }),
        toJsonReport(contentReport, driftReport, formulaReport, directSourceReport),
        toTextReport(contentReport, driftReport)
      );
    }
  );

program.parse(process.argv);
