'use client';

import { useState, useEffect } from 'react';
import { Loader2 } from 'lucide-react';
import { TableInfo } from '@/types';
import axios from 'axios';
import IcebergTree from './IcebergTree';

interface IcebergGraphViewProps {
  tableInfo: TableInfo;
}

interface GraphData {
  tableName: string;
  location: string;
  currentSnapshotId?: string;
  catalog?: {
    name: string;
    type: string;
    description?: string;
  };
  metadataFiles?: {
    file: string;
    version: number;
    currentSnapshotId: string | null;
    previousMetadataFile: string | null;
    timestamp: number;
  }[];
  snapshots: {
    snapshotId: string;
    timestamp: string | null;
    manifestList: string;
    manifests: {
      path: string;
      length: number;
      partitionSpecId: number;
      addedSnapshotId: number;
      added_data_files_count?: number;
      dataFiles?: {
        path: string;
        format: string;
        recordCount: number;
        fileSizeInBytes: number;
      }[];
    }[];
    summary: Record<string, string>;
    parentId?: string | null;
  }[];
}

export default function IcebergGraphView({ tableInfo }: IcebergGraphViewProps) {
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadGraphData();
  }, [tableInfo]);

  const loadGraphData = async () => {
    setLoading(true);
    setError(null);
    try {
      // Use the correct API endpoint
      const response = await axios.get(`http://localhost:8000/analyze`, {
        params: {
          bucket: tableInfo.bucket,
          path: tableInfo.path,
          project_id: tableInfo.projectId
        }
      });
      setGraphData(response.data);
    } catch (err: any) {
      console.error("Error loading graph data:", err);
      setError(err.response?.data?.detail || err.message || "Failed to load graph data");
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full text-slate-500">
        <Loader2 className="w-6 h-6 animate-spin mr-2" />
        Analyzing table structure...
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-red-500 p-4">
        <p className="font-semibold mb-2">Error loading table data</p>
        <p className="text-sm text-center max-w-lg bg-red-50 p-4 rounded border border-red-100 font-mono">
          {error}
        </p>
        <button
          onClick={loadGraphData}
          className="mt-4 px-4 py-2 bg-white border border-slate-200 rounded hover:bg-slate-50 text-slate-600 text-sm"
        >
          Retry
        </button>
      </div>
    );
  }

  if (!graphData) {
    return (
      <div className="flex items-center justify-center h-full text-slate-400">
        No data available
      </div>
    );
  }

  return (
    <div className="w-full h-full bg-slate-50 relative">
      <IcebergTree data={graphData} />
    </div>
  );
}
