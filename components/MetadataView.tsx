'use client';

import { TableMetadata } from '@/types';
import { Info, Calendar, Hash, File } from 'lucide-react';

interface MetadataViewProps {
  metadata: TableMetadata;
}

export default function MetadataView({ metadata }: MetadataViewProps) {
  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
  };

  const totalSize = metadata.dataFiles.reduce(
    (sum, file) => sum + file.fileSizeInBytes,
    0
  );
  const totalRecords = metadata.dataFiles.reduce(
    (sum, file) => sum + file.recordCount,
    0
  );

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Info className="h-5 w-5 text-blue-500" />
            <h3 className="font-semibold text-gray-800 dark:text-white">Table Name</h3>
          </div>
          <p className="text-gray-600 dark:text-gray-300">{metadata.tableName}</p>
        </div>

        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Hash className="h-5 w-5 text-green-500" />
            <h3 className="font-semibold text-gray-800 dark:text-white">Format Version</h3>
          </div>
          <p className="text-gray-600 dark:text-gray-300">v{metadata.formatVersion}</p>
        </div>

        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <File className="h-5 w-5 text-purple-500" />
            <h3 className="font-semibold text-gray-800 dark:text-white">Data Files</h3>
          </div>
          <p className="text-gray-600 dark:text-gray-300">{metadata.dataFiles.length}</p>
        </div>

        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Hash className="h-5 w-5 text-orange-500" />
            <h3 className="font-semibold text-gray-800 dark:text-white">Total Records</h3>
          </div>
          <p className="text-gray-600 dark:text-gray-300">
            {totalRecords.toLocaleString()}
          </p>
        </div>

        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <File className="h-5 w-5 text-red-500" />
            <h3 className="font-semibold text-gray-800 dark:text-white">Total Size</h3>
          </div>
          <p className="text-gray-600 dark:text-gray-300">{formatBytes(totalSize)}</p>
        </div>

        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <Calendar className="h-5 w-5 text-indigo-500" />
            <h3 className="font-semibold text-gray-800 dark:text-white">Snapshots</h3>
          </div>
          <p className="text-gray-600 dark:text-gray-300">{metadata.snapshots.length}</p>
        </div>
      </div>

      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
        <h3 className="font-semibold text-gray-800 dark:text-white mb-3">Location</h3>
        <p className="text-sm text-gray-600 dark:text-gray-300 break-all">{metadata.location}</p>
      </div>

      {Object.keys(metadata.properties).length > 0 && (
        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <h3 className="font-semibold text-gray-800 dark:text-white mb-3">Properties</h3>
          <div className="space-y-2">
            {Object.entries(metadata.properties).map(([key, value]) => (
              <div key={key} className="flex justify-between text-sm">
                <span className="text-gray-600 dark:text-gray-400 font-medium">{key}:</span>
                <span className="text-gray-800 dark:text-gray-200">{value}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {metadata.snapshots.length > 0 && (
        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <h3 className="font-semibold text-gray-800 dark:text-white mb-3">Recent Snapshots</h3>
          <div className="space-y-2 max-h-48 overflow-y-auto">
            {metadata.snapshots.slice(-5).reverse().map((snapshot) => (
              <div
                key={snapshot.snapshotId}
                className="flex justify-between items-center text-sm p-2 bg-white dark:bg-gray-800 rounded"
              >
                <span className="text-gray-600 dark:text-gray-400">
                  Snapshot {snapshot.snapshotId}
                </span>
                <span className="text-gray-500 dark:text-gray-500 text-xs">
                  {new Date(snapshot.timestamp).toLocaleString()}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

