export interface TableInfo {
  name: string;
  location: string;
  bucket: string;
  path: string;
  projectId?: string;
}

export interface TableMetadata {
  tableName: string;
  location: string;
  formatVersion: number;
  schema: SchemaField[];
  partitionSpec: PartitionSpec[];
  sortOrder: SortOrder[];
  properties: Record<string, string>;
  currentSnapshotId: string | number;  // String for large integers, number for -1
  snapshots: Snapshot[];
  dataFiles: DataFile[];
  partitionStats: PartitionStats[];
  statistics?: {
    totalFiles: number;
    totalRecords: number;
    totalSize: number;
    totalPartitions: number;
  };
}

export interface SchemaField {
  id: number;
  name: string;
  type: string;
  required: boolean;
  doc?: string;
}

export interface PartitionSpec {
  fieldId: number;
  sourceId: number;
  name: string;
  transform: string;
}

export interface SortOrder {
  orderId: number;
  direction: string;
  nullOrder: string;
  sortFieldId: number;
}

export interface Snapshot {
  snapshotId: string;  // String to preserve precision for large integers
  sequenceNumber?: number;
  timestamp: string;
  summary: Record<string, string>;
  manifestList: string;
  parentSnapshotId?: string;  // String to preserve precision
  statistics?: {
    fileCount: number;
    recordCount: number;
    totalSize: number;
    delta: {
      addedFiles: number;
      addedRecords: number;
      addedSize: number;
    };
  };
}

export interface DataFile {
  filePath: string;
  fileFormat: string;
  partition: Record<string, unknown>;
  recordCount: number;
  fileSizeInBytes: number;
  columnSizes?: Record<string, number>;
  valueCounts?: Record<string, number>;
  nullValueCounts?: Record<string, number>;
}

export interface PartitionStats {
  partition: Record<string, unknown>;
  fileCount: number;
  recordCount: number;
  totalSize: number;
}

export interface GCSBucket {
  name: string;
}

export interface GCSObject {
  name: string;
  size: number;
  contentType: string;
  timeCreated: string;
}

export interface SampleData {
  rows: Record<string, unknown>[];
  columns: string[];
  totalRows: number;
  filesRead: number;
  message?: string;
}

export interface SnapshotComparison {
  snapshot1: Snapshot;
  snapshot2: Snapshot;
  addedFiles: DataFile[];
  removedFiles: DataFile[];
  modifiedFiles: Array<{
    filePath: string;
    before: DataFile;
    after: DataFile;
    changes: {
      sizeDelta: number;
      recordDelta: number;
    };
  }>;
  statistics: {
    snapshot1: {
      fileCount: number;
      recordCount: number;
      totalSize: number;
    };
    snapshot2: {
      fileCount: number;
      recordCount: number;
      totalSize: number;
    };
    delta: {
      files: number;
      records: number;
      size: number;
    };
  };
  summary: {
    addedCount: number;
    removedCount: number;
    modifiedCount: number;
  };
}


export interface BigQueryDataset {
  dataset_id: string;
  project: string;
  full_dataset_id: string;
  labels: Record<string, string>;
}

export interface BigQueryTable {
  table_id: string;
  table_type: string;
  full_table_id: string;
  created: string | null;
  expires: string | null;
}
