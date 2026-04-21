import { SigmaClient, DataModel, Workbook } from "../sigma-client.js";

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
  modelUrlMap?: Map<string, string>   // dataModelId → sigma URL (for links)
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
  console.error("  [content] Building model dependency graph…");
  // modelDeps: dependentModelId → Set<upstreamModelId>
  const modelDeps = new Map<string, Set<string>>();

  const MDEP_BATCH = 3;
  for (let i = 0; i < models.length; i += MDEP_BATCH) {
    const batch = models.slice(i, i + MDEP_BATCH);
    await Promise.all(batch.map(async (m) => {
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
    }));
  }

  // Build reverse graph: upstreamModelId → Set<dependentModelId>
  const reverseDeps = new Map<string, Set<string>>();
  for (const [dep, upstreams] of modelDeps) {
    for (const up of upstreams) {
      if (!reverseDeps.has(up)) reverseDeps.set(up, new Set());
      reverseDeps.get(up)!.add(dep);
    }
  }

  // ── Step 2: workbook→model connections via workbook lineages ─────────────
  console.error("  [content] Fetching workbook list…");
  const workbooks = await client.listWorkbooks();
  console.error(`  [content] Scanning ${workbooks.length} workbooks…`);

  // workbookId → Set<modelId> (direct connections)
  const wbToModels = new Map<string, Set<string>>();
  const wbById = new Map<string, Workbook>(workbooks.map((w) => [w.workbookId, w]));

  const WB_BATCH = 20;
  for (let i = 0; i < workbooks.length; i += WB_BATCH) {
    const batch = workbooks.slice(i, i + WB_BATCH);
    await Promise.all(batch.map(async (wb) => {
      try {
        const lineage = await client.getWorkbookLineage(wb.workbookId);
        const matched = new Set<string>();
        for (const entry of lineage.entries) {
          for (const dsId of entry.dataSourceIds ?? []) {
            const urlId = dsId.split("/")[0];
            const modelId = urlIdToModelId.get(urlId);
            if (modelId) matched.add(modelId);
          }
        }
        if (matched.size > 0) wbToModels.set(wb.workbookId, matched);
      } catch { /* skip */ }
    }));
  }

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
    for (const [wbId, modelSet] of wbToModels) {
      if (modelSet.has(m.dataModelId)) {
        seenDirect.add(wbId);
        const wb = wbById.get(wbId);
        if (wb) directWbs.push(makeDownstreamWb(wb));
      }
    }

    // Transitive workbooks — use a model that depends (transitively) on this model
    const downstreamModels = getTransitiveDownstreamModels(m.dataModelId);
    downstreamModels.delete(m.dataModelId); // exclude self

    for (const downstreamModelId of downstreamModels) {
      for (const [wbId, modelSet] of wbToModels) {
        if (modelSet.has(downstreamModelId) && !seenDirect.has(wbId) && !seenTransitive.has(wbId)) {
          seenTransitive.add(wbId);
          const wb = wbById.get(wbId);
          if (wb) transitiveWbs.push(makeDownstreamWb(wb));
        }
      }
    }

    results.push({
      modelId: m.dataModelId,
      modelName: modelIdToName.get(m.dataModelId) ?? m.dataModelId,
      modelUrl: modelIdToUrl.get(m.dataModelId),
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
