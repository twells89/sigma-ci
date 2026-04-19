import { SigmaClient } from "../sigma-client.js";
import type { DataModel, DataModelColumn, SpecColumn, SpecMetric } from "../sigma-client.js";

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
 * Extract column aliases from a SQL SELECT statement.
 * Handles: expr AS alias, expr AS "alias", expr AS `alias`
 * Bare column names without AS are omitted — those appear in element.columns.
 */
function extractSqlAliases(sql: string): string[] {
  const aliases: string[] = [];
  for (const m of sql.matchAll(/\bAS\s+["'`]?(\w[\w ]*)["'`]?/gi)) {
    const alias = m[1].trim();
    if (alias) aliases.push(alias);
  }
  return aliases;
}

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

/**
 * Normalize a column reference for existence checking.
 *
 * Sigma auto-generates display names from warehouse column names by
 * title-casing and replacing underscores with spaces:
 *   TOTAL_NET_REVENUE  →  "Total Net Revenue"
 *
 * Passthrough column formulas in the spec keep the warehouse name
 * (e.g. [TABLE/TOTAL_NET_REVENUE]), so getColumnDisplayName returns
 * "TOTAL_NET_REVENUE".  Meanwhile, calculated formulas written in the
 * Sigma UI use the display name: [Total Net Revenue].
 *
 * Stripping spaces, underscores, and lowercasing both sides lets these
 * match without conflating genuinely different columns.
 */
function normalizeRef(s: string): string {
  return s.toLowerCase().replace(/[\s_]+/g, "");
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
  return similarity >= 0.55 ? { name: bestName, similarity } : null;
}

function getColumnDisplayName(col: SpecColumn | SpecMetric): string | null {
  if (col.name) return col.name;
  // Derive display name from passthrough formula: [TABLE/Col Name] → "Col Name", or [Col Name] → "Col Name"
  if (col.formula) {
    const m = col.formula.match(/^\[(?:[^\]/]+\/)?([^\]/]+)\]$/);
    if (m) return m[1].trim();
  }
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
      // Fetch spec (for element structure/SQL sources) and the authoritative
      // columns list in parallel.  The /v2/dataModels/{id}/columns endpoint
      // returns every column with its exact Sigma display name, grouped by
      // elementId — far more accurate than deriving names from spec formulas.
      const [spec, apiCols] = await Promise.all([
        client.getDataModelSpec(model.dataModelId),
        client.getDataModelColumns(model.dataModelId).catch(() => [] as DataModelColumn[]),
      ]);

      // Build elementId → column name set from the API result.
      const apiColsByElement = new Map<string, string[]>();
      for (const col of apiCols) {
        const list = apiColsByElement.get(col.elementId) ?? [];
        list.push(col.name);
        apiColsByElement.set(col.elementId, list);
      }

      const modelElements: FormulaElementResult[] = [];

      for (const page of spec.pages ?? []) {
        for (const element of page.elements ?? []) {
          // Seed available names from the columns API (authoritative).
          const availableNames: string[] = [...(apiColsByElement.get(element.id) ?? [])];
          const availableNormalized = new Set(availableNames.map(normalizeRef));

          // Supplement with metric names from the spec — the columns API only
          // returns columns, not aggregated metrics.
          for (const metric of element.metrics ?? []) {
            const name = getColumnDisplayName(metric);
            if (name && !availableNormalized.has(normalizeRef(name))) {
              availableNames.push(name);
              availableNormalized.add(normalizeRef(name));
            }
          }

          // For custom SQL sources the columns API may not include SQL-derived
          // columns (e.g. AS-aliased aggregates).  Extract them from the SQL.
          if (element.source?.statement) {
            for (const alias of extractSqlAliases(element.source.statement as string)) {
              if (!availableNormalized.has(normalizeRef(alias))) {
                availableNames.push(alias);
                availableNormalized.add(normalizeRef(alias));
              }
            }
          }

          // Fallback: if the columns API returned nothing for this element
          // (e.g., API error or empty element), derive names from the spec.
          if (availableNames.length === 0) {
            for (const col of element.columns ?? []) {
              const name = getColumnDisplayName(col);
              if (name) { availableNames.push(name); availableNormalized.add(normalizeRef(name)); }
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

              // Exclude columns already used as valid refs in this formula from
              // the suggestion pool — prevents e.g. suggesting "Start Date" as a
              // replacement for the broken [End Date] in DateDiff("day",[Start Date],[End Date]).
              // Also exclude the column's own name — suggesting it would create a circular ref.
              const ownNorm = normalizeRef(getColumnDisplayName(item) ?? "");
              const validRefNorms = new Set(
                refs.filter((r) => availableNormalized.has(normalizeRef(r))).map(normalizeRef)
              );
              const suggestionCandidates = availableNames.filter((n) => {
                const nn = normalizeRef(n);
                return !validRefNorms.has(nn) && nn !== ownNorm;
              });

              const broken: BrokenRef[] = [];
              for (const ref of refs) {
                if (!availableNormalized.has(normalizeRef(ref))) {
                  const suggestion = findSuggestion(ref, suggestionCandidates);
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
