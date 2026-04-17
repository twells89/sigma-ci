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

  private async get<T>(path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "GET",
      headers: this.getHeaders(),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`GET ${path} failed (${response.status}): ${text}`);
    }
    return response.json() as Promise<T>;
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
    return this.getAllPages<DataModel>("/v2/dataModels");
  }

  async listWorkbooks(): Promise<Workbook[]> {
    return this.getAllPages<Workbook>("/v2/workbooks");
  }

  async getDataModelSpec(id: string): Promise<DataModelSpec> {
    return this.get<DataModelSpec>(`/v3alpha/datamodels/${id}/spec`);
  }

  async getDataModelLineage(id: string): Promise<LineageResponse> {
    return this.getAllPages<LineageEntry>(`/v2/dataModels/${id}/lineage`).then(
      (entries) => ({ entries })
    );
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

  async updateDataModelSpec(id: string, spec: DataModelSpec): Promise<void> {
    const response = await fetch(`${this.baseUrl}/v2/dataModels/${id}/spec`, {
      method: "PUT",
      headers: this.getHeaders(),
      body: JSON.stringify(spec),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`PUT /v2/dataModels/${id}/spec failed (${response.status}): ${text}`);
    }
  }
}
