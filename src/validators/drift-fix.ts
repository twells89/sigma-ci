import { SigmaClient, SpecColumn, DataModelSpec } from "../sigma-client.js";

export interface ColumnRemoval {
  elementId: string;
  tableName: string;
  columnsRemoved: string[];
}

export interface DriftFixPreview {
  modelId: string;
  modelName: string;
  removals: ColumnRemoval[];
  totalColumnsRemoved: number;
}

export interface DriftFixResult extends DriftFixPreview {
  success: boolean;
  error?: string;
}

function extractColumnName(col: SpecColumn): string | null {
  const inodeMatch = col.id.match(/^inode-[A-Za-z0-9]{22}\/(.+)$/);
  if (inodeMatch) return inodeMatch[1].toUpperCase().replace(/[ /]/g, "_");
  const formulaMatch = col.formula?.match(/^\[[^\]]+\/([^\]]+)\]$/);
  if (formulaMatch) return formulaMatch[1].toUpperCase().replace(/[ /]/g, "_");
  return null;
}

/**
 * Compute which columns would be removed to fix drift, without applying changes.
 */
export async function previewDriftFix(
  client: SigmaClient,
  modelId: string
): Promise<DriftFixPreview> {
  const [spec, lineage] = await Promise.all([
    client.getDataModelSpec(modelId),
    client.getDataModelLineage(modelId),
  ]);

  const modelName = spec.name ?? modelId;
  const tableNameToInodeId = new Map<string, string>();
  for (const entry of lineage.entries) {
    if (entry.type === "table" && entry.inodeId && entry.name) {
      tableNameToInodeId.set(entry.name.toUpperCase(), entry.inodeId);
    }
  }

  const removals: ColumnRemoval[] = [];

  for (const page of spec.pages ?? []) {
    for (const element of page.elements ?? []) {
      if (element.source?.kind !== "warehouse-table") continue;
      const path = element.source.path ?? [];
      if (path.length === 0) continue;
      const tableName = path[path.length - 1];
      const inodeId = tableNameToInodeId.get(tableName.toUpperCase());
      if (!inodeId) continue;

      let actualCols: string[] = [];
      try {
        const warehouseCols = await client.getTableColumns(inodeId);
        actualCols = warehouseCols.map((c) => c.name.toUpperCase().replace(/[ /]/g, "_"));
      } catch {
        continue;
      }

      // Safety: if the warehouse returned no columns, we can't verify anything — skip
      if (actualCols.length === 0) continue;

      const actualSet = new Set(actualCols);
      const elementCols = element.columns ?? [];
      const toRemove = elementCols
        .map((col) => ({ col, name: extractColumnName(col) }))
        .filter(({ name }) => name !== null && !actualSet.has(name!))
        .map(({ name }) => name!);

      // Safety: never remove all columns from an element — something is wrong
      if (toRemove.length > 0 && toRemove.length < elementCols.length) {
        removals.push({ elementId: element.id, tableName, columnsRemoved: toRemove });
      }
    }
  }

  return {
    modelId,
    modelName,
    removals,
    totalColumnsRemoved: removals.reduce((s, r) => s + r.columnsRemoved.length, 0),
  };
}

/**
 * Apply the drift fix: remove missing columns from spec elements and save via API.
 */
export async function applyDriftFix(
  client: SigmaClient,
  modelId: string
): Promise<DriftFixResult> {
  const preview = await previewDriftFix(client, modelId);

  if (preview.totalColumnsRemoved === 0) {
    return { ...preview, success: true };
  }

  // Re-fetch spec to get a fresh copy to modify
  const spec = await client.getDataModelSpec(modelId);

  // Build a lookup: elementId → set of column names to remove
  const removalsByElement = new Map<string, Set<string>>();
  for (const r of preview.removals) {
    removalsByElement.set(r.elementId, new Set(r.columnsRemoved));
  }

  // Apply removals to spec
  const updatedSpec: DataModelSpec = {
    ...spec,
    pages: (spec.pages ?? []).map((page) => ({
      ...page,
      elements: (page.elements ?? []).map((element) => {
        const toRemove = removalsByElement.get(element.id);
        if (!toRemove) return element;

        const keepIds = new Set<string>();
        const updatedColumns = (element.columns ?? []).filter((col) => {
          const name = extractColumnName(col);
          if (name && toRemove.has(name)) return false;
          keepIds.add(col.id);
          return true;
        });

        const updatedOrder = (element.order as string[] | undefined)?.filter(
          (id) => keepIds.has(id)
        );

        return { ...element, columns: updatedColumns, order: updatedOrder };
      }),
    })),
  };

  try {
    await client.updateDataModelSpec(modelId, updatedSpec);
    return { ...preview, success: true };
  } catch (e) {
    return { ...preview, success: false, error: (e as Error).message };
  }
}
