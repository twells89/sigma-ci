import type {
  SigmaClient,
  Workbook,
  LineageEntry,
  WorkbookElementColumn,
} from "../sigma-client.js";

// ── Types ─────────────────────────────────────────────────────────────────────

export interface DirectSourceElement {
  elementId: string;
  elementName: string | null;
  elementType: string;
  sourceKind: "warehouse-table" | "custom-sql";
  // warehouse-table only
  tableInodeId: string;
  tableName: string;
  connectionId: string | null;
  referencedColumns: string[];
  actualColumns: string[];
  missingColumns: string[];
  // custom-sql only
  sqlDefinition?: string;
}

export interface DirectSourceWorkbook {
  workbookId: string;
  workbookName: string;
  workbookUrl?: string;
  elements: DirectSourceElement[];
  hasDrift: boolean;
  hasCustomSql: boolean;
}

export interface DirectSourceReport {
  workbooks: DirectSourceWorkbook[];
  totalWorkbooksScanned: number;
  totalDirectElements: number;
  totalCustomSqlElements: number;
  totalMissingColumns: number;
  generatedAt: string;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
// Sigma uses "inode-{base62}" sourceIds for direct-warehouse elements
const INODE_RE = /^inode-[A-Za-z0-9]+$/;

/** Pull all column refs from any formula (handles multi-ref formulas like calculated columns). */
function extractAllColRefs(formula: string): string[] {
  const refs: string[] = [];
  const re = /(?<!\])\[([^\]]+)\]/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(formula)) !== null) {
    const inner = m[1];
    const slash = inner.indexOf("/");
    const display = slash >= 0 ? inner.slice(slash + 1) : inner;
    refs.push(display.toUpperCase().replace(/[\s/]+/g, "_"));
  }
  return [...new Set(refs)];
}

// ── Main validator ────────────────────────────────────────────────────────────

export async function runWorkbookDirectSourceCheck(
  client: SigmaClient,
  workbookUrlMap?: Map<string, string>,
  onProgress?: (msg: string) => void,
  opts?: { skipSync?: boolean }
): Promise<DirectSourceReport> {
  const log = (msg: string) => { if (onProgress) onProgress(msg); else console.error(msg); };

  // ── Phase 1: Fetch all workbook lineages in parallel ──────────────────────
  log("  [direct-source] Fetching workbooks…");
  const workbooks = await client.listWorkbooks();
  log(`  [direct-source] Fetched ${workbooks.length} workbooks. Scanning lineages…`);

  const lineages = await Promise.all(
    workbooks.map((wb) =>
      client.getWorkbookLineage(wb.workbookId)
        .then((l) => ({ wb, entries: l.entries }))
        .catch(() => ({ wb, entries: [] as LineageEntry[] }))
    )
  );

  // ── Phase 2: Identify candidates ─────────────────────────────────────────
  // Warehouse candidates: element whose sourceId is a UUID matching a type:"table"
  //   entry's inodeId, or an "inode-{base62}" string.
  // Custom SQL hits: element whose sourceId matches a type:"customSQL" entry's name.
  interface WarehouseCandidate {
    wb: Workbook;
    elementId: string;
    tablesByName: Map<string, { inodeId: string; connectionId: string | null }>;
  }

  interface CustomSqlHit {
    wb: Workbook;
    elementId: string;
    connectionId: string | null;
    sqlDefinition: string;
  }

  const warehouseCandidates: WarehouseCandidate[] = [];
  const customSqlHits: CustomSqlHit[] = [];

  for (const { wb, entries } of lineages) {
    const tablesByInodeId = new Map<string, { name: string; connectionId: string | null }>();
    const tablesByName = new Map<string, { inodeId: string; connectionId: string | null }>();
    const customSqlById = new Map<string, { connectionId: string | null; definition: string }>();

    for (const e of entries) {
      if (e.type === "table" && e.inodeId) {
        tablesByInodeId.set(e.inodeId, {
          name: e.name ?? e.inodeId,
          connectionId: e.connectionId ?? null,
        });
        if (e.name) {
          tablesByName.set(e.name.toUpperCase(), {
            inodeId: e.inodeId,
            connectionId: e.connectionId ?? null,
          });
        }
      } else if (e.type === "customSQL" && e.name) {
        customSqlById.set(e.name, {
          connectionId: e.connectionId ?? null,
          definition: e.definition ?? "",
        });
      }
    }

    for (const e of entries) {
      if (e.type !== "element" || !e.elementId) continue;

      // Check warehouse-table direct source
      const hasDirectWarehouse = (e.sourceIds ?? []).some(
        (sid) =>
          (UUID_RE.test(sid) && tablesByInodeId.has(sid)) ||
          INODE_RE.test(sid)
      );
      if (hasDirectWarehouse) {
        warehouseCandidates.push({ wb, elementId: e.elementId, tablesByName });
        continue;
      }

      // Check custom SQL source
      const customSqlId = (e.sourceIds ?? []).find((sid) => customSqlById.has(sid));
      if (customSqlId) {
        const sql = customSqlById.get(customSqlId)!;
        customSqlHits.push({
          wb,
          elementId: e.elementId,
          connectionId: sql.connectionId,
          sqlDefinition: sql.definition,
        });
      }
    }
  }

  log(`  [direct-source] Found ${warehouseCandidates.length} warehouse candidate(s), ${customSqlHits.length} custom SQL element(s).`);

  if (warehouseCandidates.length === 0 && customSqlHits.length === 0) {
    return {
      workbooks: [],
      totalWorkbooksScanned: workbooks.length,
      totalDirectElements: 0,
      totalCustomSqlElements: 0,
      totalMissingColumns: 0,
      generatedAt: new Date().toISOString(),
    };
  }

  // ── Phase 3: Fetch element columns for warehouse candidates ───────────────
  const colsByCandidate = await Promise.all(
    warehouseCandidates.map((c) =>
      client
        .getWorkbookElementColumns(c.wb.workbookId, c.elementId)
        .catch((): WorkbookElementColumn[] => [])
    )
  );

  // ── Phase 4: Confirm via column formula [TABLE_NAME/col] and resolve table ─
  interface WarehouseHit {
    wb: Workbook;
    elementId: string;
    tableInodeId: string;
    tableName: string;
    connectionId: string | null;
    cols: WorkbookElementColumn[];
  }

  const warehouseHits: WarehouseHit[] = [];

  for (let i = 0; i < warehouseCandidates.length; i++) {
    const { wb, elementId, tablesByName } = warehouseCandidates[i];
    const cols = colsByCandidate[i];

    let resolvedTableName: string | null = null;
    let resolvedInodeId: string | null = null;
    let resolvedConnectionId: string | null = null;

    for (const col of cols) {
      if (!col.formula) continue;
      const m = col.formula.match(/^\[([^\]\/]+)\//);
      if (!m) continue;
      const nameUpper = m[1].toUpperCase();
      const info = tablesByName.get(nameUpper);
      if (info) {
        resolvedTableName = m[1];
        resolvedInodeId = info.inodeId;
        resolvedConnectionId = info.connectionId;
        break;
      }
    }

    if (!resolvedTableName || !resolvedInodeId) continue;

    warehouseHits.push({
      wb,
      elementId,
      tableInodeId: resolvedInodeId,
      tableName: resolvedTableName,
      connectionId: resolvedConnectionId,
      cols,
    });
  }

  log(`  [direct-source] Confirmed ${warehouseHits.length} direct-warehouse element(s).`);

  // ── Phase 5: Sync warehouse table schemas (unique inodes only) ────────────
  const uniqueInodes = [...new Set(warehouseHits.map((h) => h.tableInodeId))];

  if (uniqueInodes.length > 0 && !opts?.skipSync) {
    log(`  [direct-source] Syncing ${uniqueInodes.length} warehouse table schema(s)…`);
    await Promise.all(
      uniqueInodes.map(async (inodeId) => {
        try {
          const info = await client.getInodeConnectionPath(inodeId);
          if (info) await client.syncConnectionPath(info.connectionId, info.path);
        } catch (e) {
          log(`  [direct-source] sync warning for ${inodeId}: ${(e as Error).message}`);
        }
      })
    );
  }

  // ── Phase 6: Fetch warehouse table columns ────────────────────────────────
  const warehouseColsByInode = new Map<string, string[]>();
  if (uniqueInodes.length > 0) {
    log(`  [direct-source] Fetching ${uniqueInodes.length} warehouse table schema(s)…`);
    await Promise.all(
      uniqueInodes.map(async (inodeId) => {
        try {
          const cols = await client.getTableColumns(inodeId);
          warehouseColsByInode.set(
            inodeId,
            cols.map((c) => c.name.toUpperCase().replace(/[\s/]+/g, "_"))
          );
        } catch {
          warehouseColsByInode.set(inodeId, []);
        }
      })
    );
  }

  // ── Phase 7: Build results ────────────────────────────────────────────────
  const workbookResults = new Map<string, DirectSourceWorkbook>();

  const ensureWorkbook = (wb: Workbook) => {
    if (!workbookResults.has(wb.workbookId)) {
      workbookResults.set(wb.workbookId, {
        workbookId: wb.workbookId,
        workbookName: wb.name,
        workbookUrl: workbookUrlMap?.get(wb.workbookId) ?? wb.url,
        elements: [],
        hasDrift: false,
        hasCustomSql: false,
      });
    }
    return workbookResults.get(wb.workbookId)!;
  };

  for (const h of warehouseHits) {
    const warehouseCols = warehouseColsByInode.get(h.tableInodeId) ?? [];

    const referencedSet = new Set<string>();
    let elementName: string | null = null;

    for (const col of h.cols) {
      if (!elementName && col.formula) {
        const m = col.formula.match(/^\[([^\]\/]+)\//);
        if (m) elementName = m[1];
      }
      // Only warehouse pass-through columns have inode-prefixed IDs and table-qualified
      // [TABLE/col] formulas. Calculated columns have bare [col] refs that aren't warehouse
      // columns — including them produces false drift positives.
      if (col.columnId.startsWith("inode-") && col.formula) {
        for (const ref of extractAllColRefs(col.formula)) referencedSet.add(ref);
      }
    }

    const referencedColumns = [...referencedSet].sort();
    const actualSet = new Set(warehouseCols);
    const missingColumns =
      warehouseCols.length === 0
        ? []
        : referencedColumns.filter((c) => !actualSet.has(c));

    const wbResult = ensureWorkbook(h.wb);
    wbResult.elements.push({
      elementId: h.elementId,
      elementName,
      elementType: "element",
      sourceKind: "warehouse-table",
      tableInodeId: h.tableInodeId,
      tableName: h.tableName,
      connectionId: h.connectionId,
      referencedColumns,
      actualColumns: warehouseCols,
      missingColumns,
    });
    if (missingColumns.length > 0) wbResult.hasDrift = true;
  }

  for (const h of customSqlHits) {
    const wbResult = ensureWorkbook(h.wb);
    wbResult.elements.push({
      elementId: h.elementId,
      elementName: null,
      elementType: "element",
      sourceKind: "custom-sql",
      tableInodeId: "",
      tableName: "",
      connectionId: h.connectionId,
      referencedColumns: [],
      actualColumns: [],
      missingColumns: [],
      sqlDefinition: h.sqlDefinition,
    });
    wbResult.hasCustomSql = true;
  }

  const workbookList = [...workbookResults.values()];
  const totalDirectElements = workbookList.reduce(
    (s, w) => s + w.elements.filter((e) => e.sourceKind === "warehouse-table").length,
    0
  );
  const totalCustomSqlElements = workbookList.reduce(
    (s, w) => s + w.elements.filter((e) => e.sourceKind === "custom-sql").length,
    0
  );
  const totalMissingColumns = workbookList.reduce(
    (s, w) => s + w.elements.reduce((es, e) => es + e.missingColumns.length, 0),
    0
  );

  log(
    `  [direct-source] Done. ${workbookList.length} workbook(s): ${totalDirectElements} direct-warehouse element(s), ${totalCustomSqlElements} custom SQL element(s); ${totalMissingColumns} missing column(s).`
  );

  return {
    workbooks: workbookList,
    totalWorkbooksScanned: workbooks.length,
    totalDirectElements,
    totalCustomSqlElements,
    totalMissingColumns,
    generatedAt: new Date().toISOString(),
  };
}
