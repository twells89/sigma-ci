import fetch from "node-fetch";

export interface SigmaClientConfig {
  clientId: string;
  clientSecret: string;
  baseUrl?: string;
}

export interface DataModel {
  dataModelId: string;
  dataModelUrlId?: string;
  name: string;
  path?: string;
  ownerId?: string;
  latestVersion?: number;
  createdAt?: string;
  updatedAt?: string;
  url?: string;
}

export interface DataModelSpec {
  dataModelId?: string;
  name?: string;
  pages?: SpecPage[];
  [key: string]: unknown;
}

export interface SpecPage {
  id: string;
  name?: string;
  elements?: SpecElement[];
}

export interface SpecElement {
  id: string;
  kind?: string;
  name?: string;
  source?: SpecSource;
  columns?: SpecColumn[];
  metrics?: SpecMetric[];
  [key: string]: unknown;
}

export interface SpecSource {
  kind: string;
  connectionId?: string;
  path?: string[];
  elementId?: string;
  dataModelId?: string;
  statement?: string;
  [key: string]: unknown;
}

export interface SpecColumn {
  id: string;
  formula?: string;
  name?: string;
  [key: string]: unknown;
}

export interface SpecMetric {
  id: string;
  formula?: string;
  name?: string;
}

/** Entry from GET /v2/dataModels/{id}/lineage or /v2/workbooks/{id}/lineage */
export interface LineageEntry {
  connectionId?: string;
  name?: string;
  type: string;            // "table" | "element"
  inodeId?: string;        // for type:"table" — use as tableId for columns API
  elementId?: string;      // for type:"element"
  sourceIds?: string[];
  dataSourceIds?: string[];// inode prefixes — used to match workbooks to data models
}

export interface LineageResponse {
  entries: LineageEntry[];
  nextPage?: string;
}

export interface Workbook {
  workbookId: string;
  workbookUrlId?: string;
  name: string;
  path?: string;
  url?: string;
  ownerId?: string;
  latestVersion?: number;
  createdAt?: string;
  updatedAt?: string;
}

export interface TableColumn {
  name: string;
  type?: { type?: string } | string;
  visibility?: string;
  [key: string]: unknown;
}

/** Entry from GET /v2/dataModels/{id}/columns */
export interface DataModelColumn {
  elementId: string;
  columnId: string;
  name: string;
  label?: string;
  formula?: string;
}

export interface PaginatedResponse<T> {
  entries: T[];
  nextPage?: string;
  total?: number;
}

export class SigmaClient {
  private clientId: string;
  private clientSecret: string;
  public baseUrl: string;
  private accessToken: string | null = null;
  // In-process caches — both content and drift validators fetch the same lineage
  // and spec data; caching here eliminates the duplicate API calls entirely.
  private _lineageCache = new Map<string, LineageResponse>();
  private _specCache = new Map<string, DataModelSpec>();

  constructor(config: SigmaClientConfig) {
    this.clientId = config.clientId;
    this.clientSecret = config.clientSecret;
    this.baseUrl =
      config.baseUrl?.replace(/\/$/, "") ??
      "https://aws-api.sigmacomputing.com";
  }

  async authenticate(): Promise<void> {
    const body = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: this.clientId,
      client_secret: this.clientSecret,
    });

    const response = await fetch(`${this.baseUrl}/v2/auth/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: body.toString(),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Authentication failed (${response.status}): ${text}`);
    }

    const data = (await response.json()) as { access_token: string };
    this.accessToken = data.access_token;
    console.error("Authenticated successfully.");
  }

  private getHeaders(): Record<string, string> {
    if (!this.accessToken)
      throw new Error("Not authenticated. Call authenticate() first.");
    return {
      Authorization: `Bearer ${this.accessToken}`,
      "Content-Type": "application/json",
    };
  }

  private async get<T>(path: string, retries = 4): Promise<T> {
    let lastError: Error = new Error("unreachable");
    for (let attempt = 0; attempt <= retries; attempt++) {
      const response = await fetch(`${this.baseUrl}${path}`, {
        method: "GET",
        headers: this.getHeaders(),
      });

      // Retry on rate-limit (429) and transient server errors (502/503/504)
      if (response.status === 429 || response.status === 502 || response.status === 503 || response.status === 504) {
        const retryAfterSec = parseInt(response.headers.get("Retry-After") ?? "0", 10);
        const delayMs = retryAfterSec > 0
          ? retryAfterSec * 1000
          : Math.min(1000 * Math.pow(2, attempt), 30_000); // 1s, 2s, 4s … up to 30s
        const label = response.status === 429 ? "rate-limit" : "transient";
        console.error(`  [${label}] ${response.status} on ${path} — waiting ${delayMs}ms (attempt ${attempt + 1}/${retries + 1})`);
        if (attempt < retries) {
          await new Promise((r) => setTimeout(r, delayMs));
          continue;
        }
        const text = await response.text();
        lastError = new Error(`GET ${path} failed (${response.status} after ${retries + 1} attempts): ${text.slice(0, 200)}`);
        break;
      }

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`GET ${path} failed (${response.status}): ${text}`);
      }
      return response.json() as Promise<T>;
    }
    throw lastError;
  }

  private async getAllPages<T>(firstPath: string): Promise<T[]> {
    const all: T[] = [];
    let nextPage: string | undefined;
    let path = firstPath;

    do {
      const data = await this.get<PaginatedResponse<T>>(path);
      all.push(...(data.entries ?? []));
      nextPage = data.nextPage;
      if (nextPage) {
        const sep = firstPath.includes("?") ? "&" : "?";
        path = `${firstPath}${sep}page=${encodeURIComponent(nextPage)}`;
      }
    } while (nextPage);

    return all;
  }

  async listDataModels(): Promise<DataModel[]> {
    return this.getAllPages<DataModel>("/v2/dataModels?skipPermissionCheck=true");
  }

  async listWorkbooks(): Promise<Workbook[]> {
    return this.getAllPages<Workbook>("/v2/workbooks?skipPermissionCheck=true");
  }

  async getDataModelSpec(id: string): Promise<DataModelSpec> {
    if (this._specCache.has(id)) return this._specCache.get(id)!;
    const result = await this.get<DataModelSpec>(`/v3alpha/datamodels/${id}/spec`);
    this._specCache.set(id, result);
    return result;
  }

  async getDataModelColumns(id: string): Promise<DataModelColumn[]> {
    return this.getAllPages<DataModelColumn>(`/v2/dataModels/${id}/columns`);
  }

  async getDataModelLineage(id: string): Promise<LineageResponse> {
    if (this._lineageCache.has(id)) return this._lineageCache.get(id)!;
    const entries = await this.getAllPages<LineageEntry>(`/v2/dataModels/${id}/lineage`);
    const result: LineageResponse = { entries };
    this._lineageCache.set(id, result);
    return result;
  }

  async getWorkbookLineage(id: string): Promise<LineageResponse> {
    return this.getAllPages<LineageEntry>(`/v2/workbooks/${id}/lineage`).then(
      (entries) => ({ entries })
    );
  }

  async getTableColumns(tableInodeId: string): Promise<TableColumn[]> {
    return this.getAllPages<TableColumn>(
      `/v2/connections/tables/${tableInodeId}/columns`
    );
  }

  async syncConnectionPath(connectionId: string, path: string[]): Promise<void> {
    const response = await fetch(
      `${this.baseUrl}/v2/connections/${connectionId}/sync`,
      {
        method: "POST",
        headers: this.getHeaders(),
        body: JSON.stringify({ path }),
      }
    );
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`sync failed (${response.status}): ${text}`);
    }
  }

  async updateDataModelSpec(id: string, spec: DataModelSpec): Promise<void> {
    // Try v3alpha first (matches the read endpoint so the spec round-trips cleanly).
    // Fall back to v2 if v3alpha returns 404 or 405 (method not allowed).
    const v3alphaUrl = `${this.baseUrl}/v3alpha/datamodels/${id}/spec`;
    const v2Url      = `${this.baseUrl}/v2/dataModels/${id}/spec`;

    const attemptPut = async (url: string) =>
      fetch(url, { method: "PUT", headers: this.getHeaders(), body: JSON.stringify(spec) });

    let response = await attemptPut(v3alphaUrl);
    if (response.status === 404 || response.status === 405) {
      response = await attemptPut(v2Url);
    }

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`updateDataModelSpec failed (${response.status}) for model ${id}: ${text}`);
    }
  }
}
