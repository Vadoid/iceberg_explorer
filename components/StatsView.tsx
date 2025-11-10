'use client';

import { TableMetadata } from '@/types';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

interface StatsViewProps {
  metadata: TableMetadata;
}

export default function StatsView({ metadata }: StatsViewProps) {
  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
  };

  // Aggregate statistics
  const totalSize = metadata.dataFiles.reduce((sum, file) => sum + file.fileSizeInBytes, 0);
  const totalRecords = metadata.dataFiles.reduce((sum, file) => sum + file.recordCount, 0);
  const avgFileSize = totalSize / metadata.dataFiles.length;
  const avgRecordsPerFile = totalRecords / metadata.dataFiles.length;

  // File size distribution
  const sizeRanges = [
    { range: '0-10MB', min: 0, max: 10 * 1024 * 1024, count: 0 },
    { range: '10-100MB', min: 10 * 1024 * 1024, max: 100 * 1024 * 1024, count: 0 },
    { range: '100MB-1GB', min: 100 * 1024 * 1024, max: 1024 * 1024 * 1024, count: 0 },
    { range: '1GB+', min: 1024 * 1024 * 1024, max: Infinity, count: 0 },
  ];

  metadata.dataFiles.forEach((file) => {
    for (const range of sizeRanges) {
      if (file.fileSizeInBytes >= range.min && file.fileSizeInBytes < range.max) {
        range.count++;
        break;
      }
    }
  });

  // Snapshot timeline
  const snapshotData = metadata.snapshots.map((snapshot) => ({
    id: snapshot.snapshotId,
    timestamp: new Date(snapshot.timestamp).toLocaleDateString(),
    records: parseInt(snapshot.summary['total-records'] || '0', 10),
    files: parseInt(snapshot.summary['total-data-files'] || '0', 10),
  }));

  // Top files by size
  const topFilesBySize = [...metadata.dataFiles]
    .sort((a, b) => b.fileSizeInBytes - a.fileSizeInBytes)
    .slice(0, 10)
    .map((file, idx) => ({
      name: `File ${idx + 1}`,
      size: file.fileSizeInBytes / (1024 * 1024), // MB
      records: file.recordCount,
      path: file.filePath.split('/').pop() || file.filePath,
    }));

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <div className="text-sm text-gray-600 dark:text-gray-400">Total Files</div>
          <div className="text-2xl font-bold text-gray-800 dark:text-white">
            {metadata.dataFiles.length}
          </div>
        </div>
        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <div className="text-sm text-gray-600 dark:text-gray-400">Total Records</div>
          <div className="text-2xl font-bold text-gray-800 dark:text-white">
            {totalRecords.toLocaleString()}
          </div>
        </div>
        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <div className="text-sm text-gray-600 dark:text-gray-400">Total Size</div>
          <div className="text-2xl font-bold text-gray-800 dark:text-white">
            {formatBytes(totalSize)}
          </div>
        </div>
        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <div className="text-sm text-gray-600 dark:text-gray-400">Avg File Size</div>
          <div className="text-2xl font-bold text-gray-800 dark:text-white">
            {formatBytes(avgFileSize)}
          </div>
        </div>
      </div>

      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
        <h3 className="font-semibold text-gray-800 dark:text-white mb-4">
          File Size Distribution
        </h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={sizeRanges}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="range" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="count" fill="#8884d8" name="File Count" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {snapshotData.length > 0 && (
        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <h3 className="font-semibold text-gray-800 dark:text-white mb-4">
            Snapshot Timeline
          </h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={snapshotData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="timestamp" />
              <YAxis yAxisId="left" />
              <YAxis yAxisId="right" orientation="right" />
              <Tooltip />
              <Legend />
              <Line
                yAxisId="left"
                type="monotone"
                dataKey="records"
                stroke="#8884d8"
                name="Records"
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="files"
                stroke="#82ca9d"
                name="Files"
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
        <h3 className="font-semibold text-gray-800 dark:text-white mb-4">
          Top 10 Files by Size
        </h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={topFilesBySize} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" />
            <YAxis dataKey="name" type="category" width={150} />
            <Tooltip />
            <Legend />
            <Bar dataKey="size" fill="#8884d8" name="Size (MB)" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
        <h3 className="font-semibold text-gray-800 dark:text-white mb-4">
          File Statistics Summary
        </h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="bg-white dark:bg-gray-800 rounded-lg p-3">
            <div className="text-xs text-gray-600 dark:text-gray-400">Avg Records/File</div>
            <div className="text-lg font-bold text-gray-800 dark:text-white">
              {Math.round(avgRecordsPerFile).toLocaleString()}
            </div>
          </div>
          <div className="bg-white dark:bg-gray-800 rounded-lg p-3">
            <div className="text-xs text-gray-600 dark:text-gray-400">Min File Size</div>
            <div className="text-lg font-bold text-gray-800 dark:text-white">
              {formatBytes(Math.min(...metadata.dataFiles.map((f) => f.fileSizeInBytes)))}
            </div>
          </div>
          <div className="bg-white dark:bg-gray-800 rounded-lg p-3">
            <div className="text-xs text-gray-600 dark:text-gray-400">Max File Size</div>
            <div className="text-lg font-bold text-gray-800 dark:text-white">
              {formatBytes(Math.max(...metadata.dataFiles.map((f) => f.fileSizeInBytes)))}
            </div>
          </div>
          <div className="bg-white dark:bg-gray-800 rounded-lg p-3">
            <div className="text-xs text-gray-600 dark:text-gray-400">File Formats</div>
            <div className="text-lg font-bold text-gray-800 dark:text-white">
              {new Set(metadata.dataFiles.map((f) => f.fileFormat)).size}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

