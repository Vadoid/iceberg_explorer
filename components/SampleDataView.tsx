'use client';

import { useState, useEffect } from 'react';
import { Loader2, RefreshCw } from 'lucide-react';
import { TableInfo, TableMetadata, SampleData } from '@/types';
import axios from 'axios';

interface SampleDataViewProps {
  tableInfo: TableInfo;
  metadata: TableMetadata | null;
}

export default function SampleDataView({ tableInfo, metadata }: SampleDataViewProps) {
  const [sampleData, setSampleData] = useState<SampleData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedSnapshotId, setSelectedSnapshotId] = useState<string | null>(null);

  useEffect(() => {
    if (tableInfo && metadata) {
      loadSampleData();
    }
  }, [tableInfo, metadata, selectedSnapshotId]);

  const loadSampleData = async () => {
    try {
      setLoading(true);
      setError(null);
      const params: { bucket: string; path: string; limit: number; project_id?: string; snapshot_id?: string } = {
        bucket: tableInfo.bucket,
        path: tableInfo.path,
        limit: 100,
      };
      if (tableInfo.projectId) {
        params.project_id = tableInfo.projectId;
      }
      if (selectedSnapshotId) {
        params.snapshot_id = selectedSnapshotId;
      }
      const response = await axios.get('/api/backend/sample', { params });
      setSampleData(response.data);
    } catch (err) {
      let errorMessage = 'Failed to load sample data.';
      if (err && typeof err === 'object' && 'response' in err) {
        const axiosError = err as { response?: { data?: { detail?: string } } };
        if (axiosError.response?.data?.detail) {
          errorMessage = axiosError.response.data.detail;
        }
      }
      setError(errorMessage);
      console.error('Error loading sample data:', err);
    } finally {
      setLoading(false);
    }
  };

  if (loading && !sampleData) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="animate-spin h-8 w-8 text-blue-500" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4 text-red-700 dark:text-red-400">
        <div className="font-semibold mb-2">Error Loading Sample Data</div>
        <div className="text-sm">{error}</div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold">Sample Data</h3>
          {sampleData && (
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Showing {sampleData.totalRows} rows from {sampleData.filesRead} file(s)
            </p>
          )}
        </div>
        <div className="flex items-center gap-4">
          {metadata && metadata.snapshots && metadata.snapshots.length > 0 && (
            <select
              value={selectedSnapshotId || ''}
              onChange={(e) => setSelectedSnapshotId(e.target.value || null)}
              className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-sm"
            >
              <option value="">Current Snapshot</option>
              {metadata.snapshots.map((snapshot) => (
                <option key={snapshot.snapshotId} value={snapshot.snapshotId.toString()}>
                  Snapshot {snapshot.snapshotId} ({new Date(snapshot.timestamp).toLocaleString()})
                </option>
              ))}
            </select>
          )}
          <button
            onClick={loadSampleData}
            className="px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 flex items-center gap-2"
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </button>
        </div>
      </div>

      {sampleData && sampleData.rows.length > 0 ? (
        <div className="overflow-x-auto border border-gray-200 dark:border-gray-700 rounded-lg">
          <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
            <thead className="bg-gray-50 dark:bg-gray-800">
              <tr>
                {sampleData.columns.map((col) => (
                  <th
                    key={col}
                    className="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider"
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
              {sampleData.rows.map((row, idx) => (
                <tr key={idx} className="hover:bg-gray-50 dark:hover:bg-gray-800">
                  {sampleData.columns.map((col) => (
                    <td
                      key={col}
                      className="px-4 py-3 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100"
                    >
                      {row[col] !== null && row[col] !== undefined ? String(row[col]) : <span className="text-gray-400">null</span>}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="text-center py-8 text-gray-500 dark:text-gray-400">
          {sampleData?.message || 'No sample data available'}
        </div>
      )}
    </div>
  );
}

