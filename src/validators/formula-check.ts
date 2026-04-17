import { SigmaClient } from "../sigma-client.js";
import type { DataModel, SpecColumn, SpecMetric } from "../sigma-client.js";

// ── Types ─────────────────────────────────────────────────────────────────────

export interface BrokenRef {
  ref: string;            // the broken reference text, e.g. "First order date"
  suggestion: string | null;  // closest matching column name
  similarity: number;     // 0–1
}

export interface BrokenFormulaColumn {
  columnId: string;
  columnName: string | null;
  formula: string;
  brokenRefs: BrokenRef[];
  isMetric: boolean;
}

export interface FormulaElementResult {
  elementId: string;
  elementName: string | null;
  brokenColumns: BrokenFormulaColumn[];
}

export interface FormulaModelResult {
  modelId: string;
  modelName: string;
  modelUrl?: string;
  elements: FormulaElementResult[];
  totalBroken: number;
}

export interface FormulaCheckReport {
  models: FormulaModelResult[];
  generatedAt: string;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Extract single-level column references from a Sigma formula.
 * Matches [Ref Name] but NOT [Table/Col] (cross-element refs).
 */
function extractSimpleRefs(formula: string): string[] {
  const matches = [...formula.matchAll(/\[([^/\]\\]+)\]/g)];
  const seen = new Set<string>();
  const result: string[] = [];
  for (const m of matches) {
    const ref = m[1].trim();
    if (ref && !seen.has(ref)) {
      seen.add(ref);
      result.push(ref);
    }
  }
  return result;
}

/** Levenshtein distance (space-optimised). */
function editDistance(a: string, b: string): number {
  const m = a.length, n = b.length;
  const dp: number[] = Array.from({ length: n + 1 }, (_, i) => i);
  for (let i = 1; i <= m; i++) {
    let prev = dp[0];
    dp[0] = i;
    for (let j = 1; j <= n; j++) {
      const temp = dp[j];
      dp[j] = a[i - 1] === b[j - 1] ? prev : 1 + Math.min(prev, dp[j], dp[j - 1]);
      prev = temp;
    }
  }
  return dp[n];
}

function findSuggestion(
  broken: string,
  candidates: string[]
): { name: string; similarity: number } | null {
  if (candidates.length === 0) return null;
  const brokenUp = broken.toUpperCase();
  let bestName = "";
  let bestDist = Infinity;
  for (const c of candidates) {
    const dist = editDistance(brokenUp, c.toUpperCase());
    if (dist < bestDist) { bestDist = dist; bestName = c; }
  }
  if (!bestName) return null;
  const maxLen = Math.max(broken.length, bestName.length);
  const similarity = maxLen === 0 ? 1 : 1 - bestDist / maxLen;
  return similarity >= 0.4 ? { name: bestName, similarity } : null;
}

function getColumnDisplayName(col: SpecColumn | SpecMetric): string | null {
  if (col.name) return col.name;
  return null;
}

// ── Main validator ─────────────────────────────────────────────────────────────

export async function runFormulaCheck(
  client: SigmaClient,
  models: DataModel[],
  modelUrlMap?: Map<string, string>,
  onProgress?: (msg: string) => void
): Promise<FormulaCheckReport> {
  const results: FormulaModelResult[] = [];

  for (let i = 0; i < models.length; i++) {
    const model = models[i];
    onProgress?.(`Checking formula references: model ${i + 1}/${models.length}…`);

    try {
      const spec = await client.getDataModelSpec(model.dataModelId);
      const modelElements: FormulaElementResult[] = [];

      for (const page of spec.pages ?? []) {
        for (const element of page.elements ?? []) {
          // Build set of available column names in this element (case-insensitive lookup)
          const availableNames: string[] = [];
          const availableLower = new Set<string>();

          for (const col of element.columns ?? []) {
            const name = getColumnDisplayName(col);
            if (name) {
              availableNames.push(name);
              availableLower.add(name.toLowerCase());
            }
          }
          for (const metric of element.metrics ?? []) {
            const name = getColumnDisplayName(metric);
            if (name) {
              availableNames.push(name);
              availableLower.add(name.toLowerCase());
            }
          }

          const brokenCols: BrokenFormulaColumn[] = [];

          function checkFormulas(
            items: Array<SpecColumn | SpecMetric>,
            isMetric: boolean
          ) {
            for (const item of items) {
              const formula = item.formula;
              if (!formula) continue;

              const refs = extractSimpleRefs(formula);
              if (refs.length === 0) continue;

              const broken: BrokenRef[] = [];
              for (const ref of refs) {
                if (!availableLower.has(ref.toLowerCase())) {
                  const suggestion = findSuggestion(ref, availableNames);
                  broken.push({
                    ref,
                    suggestion: suggestion?.name ?? null,
                    similarity: suggestion?.similarity ?? 0,
                  });
                }
              }

              if (broken.length > 0) {
                brokenCols.push({
                  columnId: item.id,
                  columnName: getColumnDisplayName(item),
                  formula,
                  brokenRefs: broken,
                  isMetric,
                });
              }
            }
          }

          checkFormulas(element.columns ?? [], false);
          checkFormulas(element.metrics ?? [], true);

          if (brokenCols.length > 0) {
            const elementName =
              element.name ??
              element.source?.path?.slice(-1)[0] ??
              element.id;
            modelElements.push({
              elementId: element.id,
              elementName: elementName as string | null,
              brokenColumns: brokenCols,
            });
          }
        }
      }

      const totalBroken = modelElements.reduce(
        (sum, e) => sum + e.brokenColumns.length,
        0
      );
      results.push({
        modelId: model.dataModelId,
        modelName: model.name,
        modelUrl: modelUrlMap?.get(model.dataModelId),
        elements: modelElements,
        totalBroken,
      });
    } catch (e) {
      console.error(
        `  [formula] Error checking ${model.dataModelId}: ${(e as Error).message}`
      );
      results.push({
        modelId: model.dataModelId,
        modelName: model.name,
        modelUrl: modelUrlMap?.get(model.dataModelId),
        elements: [],
        totalBroken: 0,
      });
    }
  }

  return { models: results, generatedAt: new Date().toISOString() };
}
