'use client';

import { useState, useEffect } from 'react';
import { Loader2, GitBranch, Plus, Minus, Edit, ArrowRight } from 'lucide-react';
import { TableInfo, TableMetadata, SnapshotComparison } from '@/types';
import api from '@/lib/api';

interface SnapshotComparisonViewProps {
  tableInfo: TableInfo;
  metadata: TableMetadata | null;
}

export default function SnapshotComparisonView({ tableInfo, metadata }: SnapshotComparisonViewProps) {
  const [comparison, setComparison] = useState<SnapshotComparison | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [snapshot1Id, setSnapshot1Id] = useState<string | null>(null);
  const [snapshot2Id, setSnapshot2Id] = useState<string | null>(null);

  useEffect(() => {
    if (metadata && metadata.snapshots && metadata.snapshots.length > 0) {
    // Default to comparing first two snapshots, or just the first one if only 1 exists
      const sortedSnapshots = [...metadata.snapshots].sort((a, b) => 
        new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
      );

      if (sortedSnapshots.length >= 2) {
        setSnapshot1Id(sortedSnapshots[1]?.snapshotId?.toString() || null);
        setSnapshot2Id(sortedSnapshots[0]?.snapshotId?.toString() || null);
      } else if (sortedSnapshots.length === 1) {
        setSnapshot1Id(null); // Compare against nothing (empty state)
        setSnapshot2Id(sortedSnapshots[0]?.snapshotId?.toString() || null);
      }
    }
  }, [metadata]);

  useEffect(() => {
    // Allow loading if snapshot2Id is selected (snapshot1Id can be null)
    if (snapshot2Id && snapshot1Id !== snapshot2Id) {
      loadComparison();
    }
  }, [snapshot1Id, snapshot2Id]);

  const loadComparison = async () => {
    if (!snapshot2Id) return;
    
    try {
      setLoading(true);
      setError(null);
      const params: { 
        bucket: string; 
        path: string; 
        snapshot_id_1: string; 
        snapshot_id_2: string; 
        project_id?: string;
      } = {
        bucket: tableInfo.bucket,
        path: tableInfo.path,
        snapshot_id_1: snapshot1Id || '',
        snapshot_id_2: snapshot2Id,
      };
      if (tableInfo.projectId) {
        params.project_id = tableInfo.projectId;
      }
      const response = await api.get('/snapshot/compare', { params });
      setComparison(response.data);
    } catch (err) {
      let errorMessage = 'Failed to load snapshot comparison.';
      if (err && typeof err === 'object' && 'response' in err) {
        const axiosError = err as { response?: { data?: { detail?: string } } };
        if (axiosError.response?.data?.detail) {
          errorMessage = axiosError.response.data.detail;
        }
      }
      setError(errorMessage);
      console.error('Error loading comparison:', err);
    } finally {
      setLoading(false);
    }
  };

  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
  };

  const formatNumber = (num: number) => {
    return new Intl.NumberFormat().format(num);
  };

  if (loading && !comparison) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="animate-spin h-8 w-8 text-blue-500" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4 text-red-700 dark:text-red-400">
        <div className="font-semibold mb-2">Error Loading Comparison</div>
        <div className="text-sm">{error}</div>
      </div>
    );
  }

  if (!metadata || !metadata.snapshots || metadata.snapshots.length === 0) {
    return (
      <div className="text-center py-8 text-gray-500 dark:text-gray-400">
        No snapshots available
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <div className="flex-1">
          <label className="block text-sm font-medium mb-2">Snapshot 1 (Base)</label>
          <select
            value={snapshot1Id || ''}
            onChange={(e) => setSnapshot1Id(e.target.value || null)}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
          >
            <option value="">(None - Start of History)</option>
            {metadata.snapshots.map((snapshot) => (
              <option key={snapshot.snapshotId} value={snapshot.snapshotId.toString()}>
                {snapshot.snapshotId} - {new Date(snapshot.timestamp).toLocaleString()}
              </option>
            ))}
          </select>
        </div>
        <ArrowRight className="mt-6 h-6 w-6 text-gray-400" />
        <div className="flex-1">
          <label className="block text-sm font-medium mb-2">Snapshot 2 (Compare)</label>
          <select
            value={snapshot2Id || ''}
            onChange={(e) => setSnapshot2Id(e.target.value || null)}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100"
          >
            {metadata.snapshots.map((snapshot) => (
              <option key={snapshot.snapshotId} value={snapshot.snapshotId.toString()}>
                {snapshot.snapshotId} - {new Date(snapshot.timestamp).toLocaleString()}
              </option>
            ))}
          </select>
        </div>
      </div>

      {comparison && (
        <>
          {/* Summary Statistics */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
              <div className="text-sm text-blue-600 dark:text-blue-400 font-medium">Files Added</div>
              <div className="text-2xl font-bold text-blue-700 dark:text-blue-300">
                <Plus className="inline h-5 w-5 mr-1" />
                {comparison.summary.addedCount}
              </div>
            </div>
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
              <div className="text-sm text-red-600 dark:text-red-400 font-medium">Files Removed</div>
              <div className="text-2xl font-bold text-red-700 dark:text-red-300">
                <Minus className="inline h-5 w-5 mr-1" />
                {comparison.summary.removedCount}
              </div>
            </div>
            <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4">
              <div className="text-sm text-yellow-600 dark:text-yellow-400 font-medium">Files Modified</div>
              <div className="text-2xl font-bold text-yellow-700 dark:text-yellow-300">
                <Edit className="inline h-5 w-5 mr-1" />
                {comparison.summary.modifiedCount}
              </div>
            </div>
            <div className="bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
              <div className="text-sm text-gray-600 dark:text-gray-400 font-medium">Records Delta</div>
              <div className={`text-2xl font-bold ${
                comparison.statistics.delta.records >= 0 
                  ? 'text-green-700 dark:text-green-300' 
                  : 'text-red-700 dark:text-red-300'
              }`}>
                {comparison.statistics.delta.records >= 0 ? '+' : ''}
                {formatNumber(comparison.statistics.delta.records)}
              </div>
            </div>
          </div>

          {/* Detailed Statistics */}
          <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-6 text-gray-900 dark:text-gray-100">
            <h3 className="text-lg font-semibold mb-4">Statistics Comparison</h3>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <div className="text-sm text-gray-500 dark:text-gray-400">Files</div>
                <div className="text-lg font-semibold">
                  {comparison.statistics.snapshot1.fileCount} → {comparison.statistics.snapshot2.fileCount}
                  {comparison.statistics.delta.files !== 0 && (
                    <span className={`ml-2 text-sm ${comparison.statistics.delta.files >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                      ({comparison.statistics.delta.files >= 0 ? '+' : ''}{comparison.statistics.delta.files})
                    </span>
                  )}
                </div>
              </div>
              <div>
                <div className="text-sm text-gray-500 dark:text-gray-400">Records</div>
                <div className="text-lg font-semibold">
                  {formatNumber(comparison.statistics.snapshot1.recordCount)} → {formatNumber(comparison.statistics.snapshot2.recordCount)}
                  {comparison.statistics.delta.records !== 0 && (
                    <span className={`ml-2 text-sm ${comparison.statistics.delta.records >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                      ({comparison.statistics.delta.records >= 0 ? '+' : ''}{formatNumber(comparison.statistics.delta.records)})
                    </span>
                  )}
                </div>
              </div>
              <div>
                <div className="text-sm text-gray-500 dark:text-gray-400">Total Size</div>
                <div className="text-lg font-semibold">
                  {formatBytes(comparison.statistics.snapshot1.totalSize)} → {formatBytes(comparison.statistics.snapshot2.totalSize)}
                  {comparison.statistics.delta.size !== 0 && (
                    <span className={`ml-2 text-sm ${comparison.statistics.delta.size >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                      ({comparison.statistics.delta.size >= 0 ? '+' : ''}{formatBytes(comparison.statistics.delta.size)})
                    </span>
                  )}
                </div>
              </div>
            </div>
          </div>

          {/* Added Files */}
          {comparison.addedFiles.length > 0 && (
            <div>
              <h3 className="text-lg font-semibold mb-3 flex items-center gap-2">
                <Plus className="h-5 w-5 text-green-600" />
                Added Files ({comparison.addedFiles.length})
              </h3>
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg p-4 max-h-64 overflow-y-auto">
                <div className="space-y-2">
                  {comparison.addedFiles.map((file, idx) => (
                    <div key={idx} className="text-sm font-mono bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 p-2 rounded border border-gray-200 dark:border-gray-700">
                      {file.filePath} ({formatBytes(file.fileSizeInBytes)}, {formatNumber(file.recordCount)} records)
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Removed Files */}
          {comparison.removedFiles.length > 0 && (
            <div>
              <h3 className="text-lg font-semibold mb-3 flex items-center gap-2">
                <Minus className="h-5 w-5 text-red-600" />
                Removed Files ({comparison.removedFiles.length})
              </h3>
              <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4 max-h-64 overflow-y-auto">
                <div className="space-y-2">
                  {comparison.removedFiles.map((file, idx) => (
                    <div key={idx} className="text-sm font-mono bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 p-2 rounded border border-gray-200 dark:border-gray-700">
                      {file.filePath} ({formatBytes(file.fileSizeInBytes)}, {formatNumber(file.recordCount)} records)
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Modified Files */}
          {comparison.modifiedFiles.length > 0 && (
            <div>
              <h3 className="text-lg font-semibold mb-3 flex items-center gap-2">
                <Edit className="h-5 w-5 text-yellow-600" />
                Modified Files ({comparison.modifiedFiles.length})
              </h3>
              <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4 max-h-64 overflow-y-auto">
                <div className="space-y-3">
                  {comparison.modifiedFiles.map((file, idx) => (
                    <div key={idx} className="bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 p-3 rounded border border-gray-200 dark:border-gray-700">
                      <div className="text-sm font-mono mb-2">{file.filePath}</div>
                      <div className="grid grid-cols-2 gap-4 text-sm">
                        <div>
                          <div className="text-gray-500 dark:text-gray-400">Size:</div>
                          <div>
                            {formatBytes(file.before.fileSizeInBytes)} → {formatBytes(file.after.fileSizeInBytes)}
                            {file.changes.sizeDelta !== 0 && (
                              <span className={`ml-2 ${file.changes.sizeDelta >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
                                ({file.changes.sizeDelta >= 0 ? '+' : ''}{formatBytes(file.changes.sizeDelta)})
                              </span>
                            )}
                          </div>
                        </div>
                        <div>
                          <div className="text-gray-500 dark:text-gray-400">Records:</div>
                          <div>
                            {formatNumber(file.before.recordCount)} → {formatNumber(file.after.recordCount)}
                            {file.changes.recordDelta !== 0 && (
                              <span className={`ml-2 ${file.changes.recordDelta >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>
                                ({file.changes.recordDelta >= 0 ? '+' : ''}{formatNumber(file.changes.recordDelta)})
                              </span>
                            )}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {comparison.addedFiles.length === 0 && 
           comparison.removedFiles.length === 0 && 
           comparison.modifiedFiles.length === 0 && (
            <div className="text-center py-8 text-gray-500 dark:text-gray-400">
              No changes between these snapshots
            </div>
          )}
        </>
      )}
    </div>
  );
}

