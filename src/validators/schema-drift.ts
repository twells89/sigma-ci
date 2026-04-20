import { SigmaClient, SpecColumn } from "../sigma-client.js";

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

export async function runSchemaDriftValidation(
  client: SigmaClient,
  modelIds: string[],
  modelUrlMap?: Map<string, string>,
  onProgress?: (msg: string) => void
): Promise<DriftReport> {
  const results: DriftModelResult[] = [];

  // Cache table columns by inodeId across all models.
  // Large orgs share the same physical tables (e.g. CUSTOMER_DIM) across many data
  // models — without a cache every element in every model fires a separate API call,
  // which immediately exhausts Sigma/Cloudflare's rate limit.
  const tableColumnsCache = new Map<string, string[]>();

  for (let i = 0; i < modelIds.length; i++) {
    const modelId = modelIds[i];
    let modelName = modelId;
    const tables: DriftTable[] = [];

    onProgress?.(`Checking schema drift: model ${i + 1}/${modelIds.length}...`);

    try {
      const [spec, lineage] = await Promise.all([
        client.getDataModelSpec(modelId),
        client.getDataModelLineage(modelId),
      ]);

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
                // Don't cache failures — let a later model retry once rate limit clears
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
