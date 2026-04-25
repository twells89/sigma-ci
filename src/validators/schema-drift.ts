import { SigmaClient, SpecColumn, DataModelSpec, LineageResponse } from "../sigma-client.js";

export interface DriftTable {
  tableName: string;
  tableInodeId: string;
  referencedColumns: string[];    // column names the model references
  actualColumns: string[];        // column names that actually exist in the warehouse
  missingColumns: string[];       // referenced but not in warehouse
}

export interface DriftModelResult {
  modelId: string;
  modelName: string;
  modelUrl?: string;
  tables: DriftTable[];
  hasDrift: boolean;
}

export interface DriftReport {
  models: DriftModelResult[];
  generatedAt: string;
}

/**
 * Extract the warehouse column name from a column spec entry.
 *
 * Two formats exist in the wild:
 *   1. Inode format (older):  id = "inode-{22char}/{WAREHOUSE_COLUMN}"
 *      → extract WAREHOUSE_COLUMN directly from the id.
 *   2. Short-id format (newer): id = "HGCZXED0kJ", formula = "[TABLE/Display Name]"
 *      → extract Display Name from the formula and normalise to DISPLAY_NAME
 *        (Sigma auto-generates display names by title-casing the warehouse column
 *         name, so reversing that gives back the warehouse column name in the
 *         common case).  If the user has renamed the column the comparison will
 *         miss it, but that is acceptable for a best-effort drift check.
 */
function extractColumnName(col: SpecColumn): string | null {
  // 1. Inode format
  const inodeMatch = col.id.match(/^inode-[A-Za-z0-9]{22}\/(.+)$/);
  if (inodeMatch) return inodeMatch[1].toUpperCase().replace(/[ /]/g, "_");

  // 2. Formula fallback: "[TABLE_NAME/Column Display Name]"
  //    Use [^\]]+ to prevent matching arithmetic expressions like "[A] / [B]"
  const formulaMatch = col.formula?.match(/^\[[^\]]+\/([^\]]+)\]$/);
  if (formulaMatch) {
    return formulaMatch[1].toUpperCase().replace(/[ /]/g, "_");
  }

  return null;
}

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

export async function runSchemaDriftValidation(
  client: SigmaClient,
  modelIds: string[],
  modelUrlMap?: Map<string, string>,
  onProgress?: (msg: string) => void,
  opts?: { skipSync?: boolean }
): Promise<DriftReport> {
  const results: DriftModelResult[] = [];

  // Phase 1: fetch all specs + lineages upfront so we can collect sync targets
  // before any column checks run.
  onProgress?.(`Fetching specs for ${modelIds.length} model(s)...`);
  const specCache = new Map<string, DataModelSpec>();
  const lineageCache = new Map<string, LineageResponse>();

  for (const modelId of modelIds) {
    try {
      const [spec, lineage] = await Promise.all([
        client.getDataModelSpec(modelId),
        client.getDataModelLineage(modelId),
      ]);
      specCache.set(modelId, spec);
      lineageCache.set(modelId, lineage);
    } catch (e) {
      console.error(`  [drift] Could not fetch spec/lineage for ${modelId}: ${(e as Error).message}`);
    }
  }

  // Phase 2: sync all warehouse-table paths with Sigma so the column cache is
  // fresh before we query it. Fires up to 20 syncs concurrently; errors are
  // non-fatal (logged as warnings) so a 403 on an OAuth-only connection never
  // blocks the drift check.
  if (!opts?.skipSync) {
    const seen = new Set<string>();
    const syncTasks: Array<() => Promise<void>> = [];

    for (const spec of specCache.values()) {
      for (const page of spec.pages ?? []) {
        for (const element of page.elements ?? []) {
          if (element.source?.kind !== "warehouse-table") continue;
          const connId = element.source.connectionId;
          const path = element.source.path ?? [];
          // Sync requires an exact 3-part path [db, schema, table]
          if (!connId || path.length !== 3) continue;
          const key = `${connId}::${path.join("::")}`;
          if (!seen.has(key)) {
            seen.add(key);
            syncTasks.push(async () => {
              try {
                await client.syncConnectionPath(connId, path as string[]);
              } catch (e) {
                console.error(`  [sync] ${path.join(".")} — ${(e as Error).message}`);
              }
            });
          }
        }
      }
    }

    if (syncTasks.length > 0) {
      onProgress?.(`Syncing ${syncTasks.length} table(s) with warehouse...`);
      await runConcurrent(syncTasks, 20);
    }
  }

  // Phase 3: drift check — compare model column references against Sigma's
  // (now-refreshed) cached column lists.
  const tableColumnsCache = new Map<string, string[]>();

  for (let i = 0; i < modelIds.length; i++) {
    const modelId = modelIds[i];
    const spec = specCache.get(modelId);
    const lineage = lineageCache.get(modelId);
    let modelName = modelId;
    const tables: DriftTable[] = [];

    onProgress?.(`Checking schema drift: model ${i + 1}/${modelIds.length}...`);

    if (!spec || !lineage) {
      results.push({ modelId, modelName, modelUrl: modelUrlMap?.get(modelId), tables, hasDrift: false });
      continue;
    }

    try {
      modelName = spec.name ?? modelId;

      // Build a map: table name (upper-case) → lineage inodeId
      const tableNameToInodeId = new Map<string, string>();
      for (const entry of lineage.entries) {
        if (entry.type === "table" && entry.inodeId && entry.name) {
          tableNameToInodeId.set(entry.name.toUpperCase(), entry.inodeId);
        }
      }

      for (const page of spec.pages ?? []) {
        for (const element of page.elements ?? []) {
          if (element.source?.kind !== "warehouse-table") continue;

          const path = element.source.path ?? [];
          if (path.length === 0) continue;
          const tableName = path[path.length - 1];
          const inodeId = tableNameToInodeId.get(tableName.toUpperCase());

          // Collect referenced warehouse columns from this element's columns
          const referencedCols = new Set<string>();
          for (const col of element.columns ?? []) {
            const colName = extractColumnName(col);
            if (colName) referencedCols.add(colName);
          }

          if (referencedCols.size === 0) continue;

          // Fetch actual columns from the warehouse via Sigma's connection API.
          // Use the shared cache so the same physical table is only fetched once
          // across all models — avoids redundant requests and rate-limit storms on large orgs.
          let actualCols: string[] = [];
          if (inodeId) {
            if (tableColumnsCache.has(inodeId)) {
              actualCols = tableColumnsCache.get(inodeId)!;
            } else {
              try {
                const warehouseCols = await client.getTableColumns(inodeId);
                actualCols = warehouseCols.map((c) => c.name.toUpperCase().replace(/[ /]/g, "_"));
                tableColumnsCache.set(inodeId, actualCols);
              } catch (e) {
                console.error(
                  `  [drift] Could not fetch columns for table ${tableName}: ${(e as Error).message}`
                );
              }
            }
          }

          // Safety: if warehouse returned no columns, skip — can't distinguish
          // a real empty table from a failed/inaccessible lookup.
          if (actualCols.length === 0) continue;

          const actualSet = new Set(actualCols);
          const missing = [...referencedCols].filter((c) => !actualSet.has(c));

          tables.push({
            tableName,
            tableInodeId: inodeId ?? "unknown",
            referencedColumns: [...referencedCols],
            actualColumns: actualCols,
            missingColumns: missing,
          });
        }
      }
    } catch (e) {
      console.error(
        `  [drift] Error during drift check for ${modelId}: ${(e as Error).message}`
      );
    }

    results.push({
      modelId,
      modelName,
      modelUrl: modelUrlMap?.get(modelId),
      tables,
      hasDrift: tables.some((t) => t.missingColumns.length > 0),
    });
  }

  return { models: results, generatedAt: new Date().toISOString() };
}
