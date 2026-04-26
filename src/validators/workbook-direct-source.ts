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
  tableInodeId: string;
  tableName: string;
  connectionId: string | null;
  referencedColumns: string[];
  actualColumns: string[];
  missingColumns: string[];
}

export interface DirectSourceWorkbook {
  workbookId: string;
  workbookName: string;
  workbookUrl?: string;
  elements: DirectSourceElement[];
  hasDrift: boolean;
}

export interface DirectSourceReport {
  workbooks: DirectSourceWorkbook[];
  totalDirectElements: number;
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

  // ── Phase 2: Identify candidate direct-warehouse elements ─────────────────
  // An element is a candidate if any sourceId is:
  //   (a) a UUID matching a type:"table" lineage entry's inodeId, OR
  //   (b) an "inode-{base62}" string (Sigma's compact inode format)
  // We confirm and resolve the actual table in Phase 4 via column formulas.
  interface Candidate {
    wb: Workbook;
    elementId: string;
    tablesByName: Map<string, { inodeId: string; connectionId: string | null }>;
  }

  const candidates: Candidate[] = [];

  for (const { wb, entries } of lineages) {
    const tablesByInodeId = new Map<string, { name: string; connectionId: string | null }>();
    const tablesByName = new Map<string, { inodeId: string; connectionId: string | null }>();

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
      }
    }

    if (tablesByInodeId.size === 0) continue;

    for (const e of entries) {
      if (e.type !== "element" || !e.elementId) continue;

      const hasDirectSource = (e.sourceIds ?? []).some(
        (sid) =>
          (UUID_RE.test(sid) && tablesByInodeId.has(sid)) ||
          INODE_RE.test(sid)
      );

      if (hasDirectSource) {
        candidates.push({ wb, elementId: e.elementId, tablesByName });
      }
    }
  }

  log(`  [direct-source] Found ${candidates.length} candidate element(s). Fetching columns to confirm…`);

  if (candidates.length === 0) {
    return {
      workbooks: [],
      totalDirectElements: 0,
      totalMissingColumns: 0,
      generatedAt: new Date().toISOString(),
    };
  }

  // ── Phase 3: Fetch element columns for all candidates in parallel ──────────
  const colsByCandidate = await Promise.all(
    candidates.map((c) =>
      client
        .getWorkbookElementColumns(c.wb.workbookId, c.elementId)
        .catch((): WorkbookElementColumn[] => [])
    )
  );

  // ── Phase 4: Confirm via column formula [TABLE_NAME/col] and resolve table ─
  interface DirectHit {
    wb: Workbook;
    elementId: string;
    tableInodeId: string;
    tableName: string;
    connectionId: string | null;
    cols: WorkbookElementColumn[];
  }

  const hits: DirectHit[] = [];

  for (let i = 0; i < candidates.length; i++) {
    const { wb, elementId, tablesByName } = candidates[i];
    const cols = colsByCandidate[i];

    // Identify the warehouse table from "[TABLE_NAME/col]" formula prefix
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

    hits.push({
      wb,
      elementId,
      tableInodeId: resolvedInodeId,
      tableName: resolvedTableName,
      connectionId: resolvedConnectionId,
      cols,
    });
  }

  log(`  [direct-source] Confirmed ${hits.length} direct-warehouse element(s) across ${new Set(hits.map((h) => h.wb.workbookId)).size} workbook(s).`);

  if (hits.length === 0) {
    return {
      workbooks: [],
      totalDirectElements: 0,
      totalMissingColumns: 0,
      generatedAt: new Date().toISOString(),
    };
  }

  // ── Phase 5: Sync warehouse table schemas (unique inodes only) ───────────
  const uniqueInodes = [...new Set(hits.map((h) => h.tableInodeId))];

  if (!opts?.skipSync) {
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
  log(`  [direct-source] Fetching ${uniqueInodes.length} warehouse table schema(s)…`);

  const warehouseColsByInode = new Map<string, string[]>();
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

  // ── Phase 7: Build results ────────────────────────────────────────────────
  const workbookResults = new Map<string, DirectSourceWorkbook>();

  for (const h of hits) {
    const warehouseCols = warehouseColsByInode.get(h.tableInodeId) ?? [];

    const referencedSet = new Set<string>();
    let elementName: string | null = null;

    for (const col of h.cols) {
      if (!elementName && col.formula) {
        const m = col.formula.match(/^\[([^\]\/]+)\//);
        if (m) elementName = m[1];
      }
      if (col.formula) {
        for (const ref of extractAllColRefs(col.formula)) {
          referencedSet.add(ref);
        }
      }
    }

    const referencedColumns = [...referencedSet].sort();
    const actualSet = new Set(warehouseCols);
    const missingColumns =
      warehouseCols.length === 0
        ? []
        : referencedColumns.filter((c) => !actualSet.has(c));

    const elem: DirectSourceElement = {
      elementId: h.elementId,
      elementName,
      elementType: "element",
      tableInodeId: h.tableInodeId,
      tableName: h.tableName,
      connectionId: h.connectionId,
      referencedColumns,
      actualColumns: warehouseCols,
      missingColumns,
    };

    if (!workbookResults.has(h.wb.workbookId)) {
      workbookResults.set(h.wb.workbookId, {
        workbookId: h.wb.workbookId,
        workbookName: h.wb.name,
        workbookUrl: workbookUrlMap?.get(h.wb.workbookId) ?? h.wb.url,
        elements: [],
        hasDrift: false,
      });
    }

    const wbResult = workbookResults.get(h.wb.workbookId)!;
    wbResult.elements.push(elem);
    if (missingColumns.length > 0) wbResult.hasDrift = true;
  }

  const workbookList = [...workbookResults.values()];
  const totalDirectElements = workbookList.reduce((s, w) => s + w.elements.length, 0);
  const totalMissingColumns = workbookList.reduce(
    (s, w) => s + w.elements.reduce((es, e) => es + e.missingColumns.length, 0),
    0
  );

  log(
    `  [direct-source] Done. ${workbookList.length} workbook(s) with direct warehouse elements; ${totalMissingColumns} missing column(s).`
  );

  return {
    workbooks: workbookList,
    totalDirectElements,
    totalMissingColumns,
    generatedAt: new Date().toISOString(),
  };
}
