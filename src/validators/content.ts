import { SigmaClient, DataModel, Workbook, WorkbookSource } from "../sigma-client.js";

async function runConcurrent(tasks: Array<() => Promise<void>>, concurrency: number): Promise<void> {
  const queue = [...tasks];
  const workers = Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
    while (queue.length > 0) {
      const task = queue.shift()!;
      await task();
    }
  });
  await Promise.all(workers);
}

export interface DownstreamWorkbook {
  workbookId: string;
  name: string;
  folder: string;
  url?: string;
}

export interface ContentModelResult {
  modelId: string;
  modelName: string;
  modelUrl?: string;
  path?: string;
  ownerId?: string;
  downstreamWorkbooks: DownstreamWorkbook[];
  // Transitive workbooks: workbooks that reach this model through a downstream model
  transitiveWorkbooks: DownstreamWorkbook[];
}

export interface ContentReport {
  models: ContentModelResult[];
  // model dependency graph: modelId → set of upstream modelIds it depends on
  modelDependencies: Record<string, string[]>;
  generatedAt: string;
}

function getFolder(workbook: Workbook): string {
  return workbook.path ?? "—";
}

/**
 * Run content validation.
 *
 * Approach:
 *  1. Build a map of dataModelUrlId → dataModelId.
 *  2. Fetch all workbook lineages. For each lineage entry whose dataSourceIds
 *     contain a "{dataModelUrlId}/{elementId}" reference, record the connection.
 *  3. Also scan MODEL lineages to detect model→model dependencies
 *     (same "{urlId}/{elementId}" pattern, but pointing to another model).
 *  4. Compute transitive blast radius using the dependency graph.
 */
export async function runContentValidation(
  client: SigmaClient,
  models: DataModel[],
  modelUrlMap?: Map<string, string>,   // dataModelId → sigma URL (for links)
  opts?: { skipModelDeps?: boolean }
): Promise<ContentReport> {
  // Build lookup maps
  const urlIdToModelId = new Map<string, string>();
  const modelIdToName = new Map<string, string>();
  const modelIdToUrl = new Map<string, string>();

  for (const m of models) {
    modelIdToName.set(m.dataModelId, m.name);
    if (m.dataModelUrlId) urlIdToModelId.set(m.dataModelUrlId, m.dataModelId);
    if (m.url) modelIdToUrl.set(m.dataModelId, m.url);
    // Also pick up URLs from the passed-in map
    if (modelUrlMap) {
      const u = modelUrlMap.get(m.dataModelId);
      if (u) modelIdToUrl.set(m.dataModelId, u);
    }
  }

  // ── Step 1: model→model dependency graph from model lineages ──────────────
  // Skipped by default on large orgs — each DM lineage call is rate-limited
  // (~5 req/s) so 1,000+ models takes several minutes. Enable with skipModelDeps:false.
  const modelDeps = new Map<string, Set<string>>();
  const reverseDeps = new Map<string, Set<string>>();

  if (!opts?.skipModelDeps) {
    console.error("  [content] Building model dependency graph…");

    await runConcurrent(models.map((m) => async () => {
      try {
        const lineage = await client.getDataModelLineage(m.dataModelId);
        const upstream = new Set<string>();
        for (const entry of lineage.entries) {
          for (const dsId of entry.dataSourceIds ?? []) {
            const urlId = dsId.split("/")[0];
            const upstreamId = urlIdToModelId.get(urlId);
            if (upstreamId && upstreamId !== m.dataModelId) {
              upstream.add(upstreamId);
            }
          }
        }
        if (upstream.size > 0) modelDeps.set(m.dataModelId, upstream);
      } catch { /* skip */ }
    }), 3);

    for (const [dep, upstreams] of modelDeps) {
      for (const up of upstreams) {
        if (!reverseDeps.has(up)) reverseDeps.set(up, new Set());
        reverseDeps.get(up)!.add(dep);
      }
    }
  }

  // ── Step 2: workbook→model connections via /sources (faster than /lineage) ─
  console.error("  [content] Fetching workbook list…");
  const workbooks = await client.listWorkbooks();
  console.error(`  [content] Scanning ${workbooks.length} workbooks via /sources…`);

  // workbookId → { wb, models } — only stores workbooks that actually match a model
  // (avoids keeping all 12k workbook objects in a separate wbById map)
  const wbToModels = new Map<string, { wb: Workbook; models: Set<string> }>();

  // Build a fast lookup set for direct modelId matching (type: "data-model" sources)
  const modelIdSet = new Set(models.map((m) => m.dataModelId));

  let scanned = 0;
  await runConcurrent(workbooks.map((wb) => async () => {
    try {
      const sources: WorkbookSource[] = await client.getWorkbookSources(wb.workbookId);
      const matched = new Set<string>();
      for (const src of sources) {
        if (src.type === "data-model" && src.dataModelId && modelIdSet.has(src.dataModelId)) {
          matched.add(src.dataModelId);
        }
      }
      if (matched.size > 0) wbToModels.set(wb.workbookId, { wb, models: matched });
    } catch { /* skip */ }
    scanned++;
    if (scanned % 500 === 0) console.error(`  [content] Scanned ${scanned}/${workbooks.length} workbooks…`);
  }), 50);

  // ── Step 3: compute transitive downstream workbooks ───────────────────────
  // For a model M, transitively downstream workbooks = workbooks that use any
  // model that (directly or transitively) depends on M.

  function getTransitiveDownstreamModels(modelId: string, visited = new Set<string>()): Set<string> {
    if (visited.has(modelId)) return visited;
    visited.add(modelId);
    for (const dep of reverseDeps.get(modelId) ?? []) {
      getTransitiveDownstreamModels(dep, visited);
    }
    return visited;
  }

  function makeDownstreamWb(wb: Workbook): DownstreamWorkbook {
    return {
      workbookId: wb.workbookId,
      name: wb.name,
      folder: getFolder(wb),
      url: wb.url,
    };
  }

  // ── Step 4: build per-model results ──────────────────────────────────────
  const results: ContentModelResult[] = [];

  for (const m of models) {
    const directWbs: DownstreamWorkbook[] = [];
    const transitiveWbs: DownstreamWorkbook[] = [];
    const seenDirect = new Set<string>();
    const seenTransitive = new Set<string>();

    // Direct workbooks — use this model directly
    for (const [wbId, { wb, models: modelSet }] of wbToModels) {
      if (modelSet.has(m.dataModelId)) {
        seenDirect.add(wbId);
        directWbs.push(makeDownstreamWb(wb));
      }
    }

    // Transitive workbooks — use a model that depends (transitively) on this model
    const downstreamModels = getTransitiveDownstreamModels(m.dataModelId);
    downstreamModels.delete(m.dataModelId); // exclude self

    for (const downstreamModelId of downstreamModels) {
      for (const [wbId, { wb, models: modelSet }] of wbToModels) {
        if (modelSet.has(downstreamModelId) && !seenDirect.has(wbId) && !seenTransitive.has(wbId)) {
          seenTransitive.add(wbId);
          transitiveWbs.push(makeDownstreamWb(wb));
        }
      }
    }

    results.push({
      modelId: m.dataModelId,
      modelName: modelIdToName.get(m.dataModelId) ?? m.dataModelId,
      modelUrl: modelIdToUrl.get(m.dataModelId),
      path: m.path,
      ownerId: m.ownerId,
      downstreamWorkbooks: directWbs,
      transitiveWorkbooks: transitiveWbs,
    });
  }

  // Serialise dependency graph for the report
  const modelDependenciesRecord: Record<string, string[]> = {};
  for (const [dep, upstreams] of modelDeps) {
    modelDependenciesRecord[dep] = [...upstreams];
  }

  return {
    models: results,
    modelDependencies: modelDependenciesRecord,
    generatedAt: new Date().toISOString(),
  };
}
