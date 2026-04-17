import { describe, it, expect, vi } from "vitest";
import { runSchemaDriftValidation } from "./schema-drift.js";
import type { SigmaClient } from "../sigma-client.js";

// ---------------------------------------------------------------------------
// Helpers to build stub Sigma API responses
// ---------------------------------------------------------------------------

function makeSpec(columns: Array<{ id: string; formula?: string }>) {
  return {
    name: "Test Model",
    pages: [
      {
        id: "p1",
        name: "Page 1",
        elements: [
          {
            id: "el1",
            source: {
              kind: "warehouse-table",
              path: ["DB", "SCHEMA", "ORDER_FACT"],
            },
            columns,
          },
        ],
      },
    ],
  };
}

function makeLineage(inodeId = "test-inode-001") {
  return {
    entries: [
      { type: "table", name: "ORDER_FACT", inodeId },
    ],
  };
}

function makeWarehouseCols(names: string[]) {
  return names.map((name) => ({ name }));
}

function makeClient(overrides: Partial<SigmaClient>): SigmaClient {
  return {
    getDataModelSpec: vi.fn(),
    getDataModelLineage: vi.fn(),
    getTableColumns: vi.fn(),
    ...overrides,
  } as unknown as SigmaClient;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("runSchemaDriftValidation", () => {
  it("reports no drift when all referenced columns exist", async () => {
    const client = makeClient({
      getDataModelSpec: vi.fn().mockResolvedValue(
        makeSpec([
          { id: "col1", formula: "[ORDER_FACT/Order Id]" },
          { id: "col2", formula: "[ORDER_FACT/Customer Key]" },
        ])
      ),
      getDataModelLineage: vi.fn().mockResolvedValue(makeLineage()),
      getTableColumns: vi
        .fn()
        .mockResolvedValue(makeWarehouseCols(["ORDER_ID", "CUSTOMER_KEY"])),
    });

    const report = await runSchemaDriftValidation(client, ["model-1"]);

    expect(report.models[0].hasDrift).toBe(false);
    expect(report.models[0].tables[0].missingColumns).toHaveLength(0);
  });

  it("detects a missing column (formula format)", async () => {
    const client = makeClient({
      getDataModelSpec: vi.fn().mockResolvedValue(
        makeSpec([
          { id: "col1", formula: "[ORDER_FACT/Order Id]" },
          // This column no longer exists in the warehouse
          { id: "col2", formula: "[ORDER_FACT/Dropped Column]" },
        ])
      ),
      getDataModelLineage: vi.fn().mockResolvedValue(makeLineage()),
      getTableColumns: vi
        .fn()
        // Warehouse only has ORDER_ID — DROPPED_COLUMN is gone
        .mockResolvedValue(makeWarehouseCols(["ORDER_ID"])),
    });

    const report = await runSchemaDriftValidation(client, ["model-1"]);

    expect(report.models[0].hasDrift).toBe(true);
    expect(report.models[0].tables[0].missingColumns).toContain("DROPPED_COLUMN");
  });

  it("detects a missing column (inode format)", async () => {
    const client = makeClient({
      getDataModelSpec: vi.fn().mockResolvedValue(
        makeSpec([
          { id: "inode-AAAAAAAAAAAAAAAAAAAAAA/ORDER_ID" },
          // Inode pointing to a column that's been dropped
          { id: "inode-AAAAAAAAAAAAAAAAAAAAAA/DROPPED_COLUMN" },
        ])
      ),
      getDataModelLineage: vi.fn().mockResolvedValue(makeLineage()),
      getTableColumns: vi
        .fn()
        .mockResolvedValue(makeWarehouseCols(["ORDER_ID"])),
    });

    const report = await runSchemaDriftValidation(client, ["model-1"]);

    expect(report.models[0].hasDrift).toBe(true);
    expect(report.models[0].tables[0].missingColumns).toContain("DROPPED_COLUMN");
  });

  it("normalises spaces and slashes in warehouse column names", async () => {
    // Warehouse has 'PRODUCT_KEY/NAME' — Sigma friendly names converts to 'PRODUCT_KEY_NAME'
    const client = makeClient({
      getDataModelSpec: vi.fn().mockResolvedValue(
        makeSpec([{ id: "col1", formula: "[ORDER_FACT/Product Key Name]" }])
      ),
      getDataModelLineage: vi.fn().mockResolvedValue(makeLineage()),
      getTableColumns: vi
        .fn()
        .mockResolvedValue(makeWarehouseCols(["PRODUCT_KEY/NAME"])),
    });

    const report = await runSchemaDriftValidation(client, ["model-1"]);

    // PRODUCT_KEY/NAME → PRODUCT_KEY_NAME, spec → PRODUCT_KEY_NAME — should match
    expect(report.models[0].hasDrift).toBe(false);
    expect(report.models[0].tables[0].missingColumns).toHaveLength(0);
  });

  it("does NOT flag arithmetic expressions as warehouse column references", async () => {
    // This was the root cause of the false-positive bug: [A] / [B] being misread
    // as a warehouse reference to table [A]/column [B].
    const client = makeClient({
      getDataModelSpec: vi.fn().mockResolvedValue(
        makeSpec([
          // Calculated column — arithmetic, NOT a warehouse reference
          { id: "calc1", formula: "[Net Revenue] / [Gross Revenue]" },
          // Real warehouse column
          { id: "col1", formula: "[ORDER_FACT/Order Id]" },
        ])
      ),
      getDataModelLineage: vi.fn().mockResolvedValue(makeLineage()),
      getTableColumns: vi
        .fn()
        .mockResolvedValue(makeWarehouseCols(["ORDER_ID"])),
    });

    const report = await runSchemaDriftValidation(client, ["model-1"]);

    // The arithmetic expression must not produce a missing-column report
    expect(report.models[0].hasDrift).toBe(false);
    expect(report.models[0].tables[0].missingColumns).toHaveLength(0);
  });

  it("skips elements with no columns", async () => {
    const client = makeClient({
      getDataModelSpec: vi.fn().mockResolvedValue(makeSpec([])),
      getDataModelLineage: vi.fn().mockResolvedValue(makeLineage()),
      getTableColumns: vi.fn().mockResolvedValue(makeWarehouseCols(["ORDER_ID"])),
    });

    const report = await runSchemaDriftValidation(client, ["model-1"]);

    expect(report.models[0].hasDrift).toBe(false);
    expect(report.models[0].tables).toHaveLength(0);
  });

  it("skips elements when warehouse returns 0 columns (inaccessible table)", async () => {
    // Safety guard: if warehouse lookup fails/returns empty, don't flag everything as missing
    const client = makeClient({
      getDataModelSpec: vi.fn().mockResolvedValue(
        makeSpec([{ id: "col1", formula: "[ORDER_FACT/Order Id]" }])
      ),
      getDataModelLineage: vi.fn().mockResolvedValue(makeLineage()),
      // Warehouse returns nothing — table is inaccessible
      getTableColumns: vi.fn().mockResolvedValue([]),
    });

    const report = await runSchemaDriftValidation(client, ["model-1"]);

    expect(report.models[0].hasDrift).toBe(false);
    expect(report.models[0].tables).toHaveLength(0);
  });

  it("skips elements whose table has no lineage inodeId", async () => {
    const client = makeClient({
      getDataModelSpec: vi.fn().mockResolvedValue(
        makeSpec([{ id: "col1", formula: "[ORDER_FACT/Order Id]" }])
      ),
      // Lineage has no table entry for ORDER_FACT
      getDataModelLineage: vi.fn().mockResolvedValue({ entries: [] }),
      getTableColumns: vi.fn().mockResolvedValue(makeWarehouseCols(["ORDER_ID"])),
    });

    const report = await runSchemaDriftValidation(client, ["model-1"]);

    // No inodeId → can't fetch warehouse cols → skip
    expect(report.models[0].hasDrift).toBe(false);
    expect(report.models[0].tables).toHaveLength(0);
  });

  it("handles multiple models and only flags the drifted one", async () => {
    const cleanSpec = makeSpec([{ id: "col1", formula: "[ORDER_FACT/Order Id]" }]);
    const driftSpec = makeSpec([
      { id: "col1", formula: "[ORDER_FACT/Order Id]" },
      { id: "col2", formula: "[ORDER_FACT/Dropped Column]" },
    ]);

    const client = makeClient({
      getDataModelSpec: vi
        .fn()
        .mockResolvedValueOnce({ ...cleanSpec, name: "Clean Model" })
        .mockResolvedValueOnce({ ...driftSpec, name: "Drifted Model" }),
      getDataModelLineage: vi.fn().mockResolvedValue(makeLineage()),
      getTableColumns: vi.fn().mockResolvedValue(makeWarehouseCols(["ORDER_ID"])),
    });

    const report = await runSchemaDriftValidation(client, ["clean", "drifted"]);

    expect(report.models[0].hasDrift).toBe(false);
    expect(report.models[1].hasDrift).toBe(true);
    expect(report.models[1].tables[0].missingColumns).toContain("DROPPED_COLUMN");
  });
});
