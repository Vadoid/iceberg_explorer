'use client';

import { TableMetadata } from '@/types';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

interface PartitionViewProps {
  metadata: TableMetadata;
}

export default function PartitionView({ metadata }: PartitionViewProps) {
  const partitionKeys = metadata.partitionSpec.map((spec) => spec.name);

  // Group data files by partition
  const partitionMap = new Map<string, { partition: Record<string, unknown>; fileCount: number; recordCount: number; totalSize: number }>();

  metadata.dataFiles.forEach((file) => {
    const partitionKey = JSON.stringify(file.partition);
    if (partitionMap.has(partitionKey)) {
      const existing = partitionMap.get(partitionKey)!;
      existing.fileCount += 1;
      existing.recordCount += file.recordCount;
      existing.totalSize += file.fileSizeInBytes;
    } else {
      partitionMap.set(partitionKey, {
        partition: file.partition,
        fileCount: 1,
        recordCount: file.recordCount,
        totalSize: file.fileSizeInBytes,
      });
    }
  });

  const partitionData = Array.from(partitionMap.values()).map((p, idx) => ({
    id: idx,
    ...p,
    partitionLabel: Object.entries(p.partition)
      .map(([key, value]) => `${key}=${value}`)
      .join(', '),
    sizeGB: p.totalSize / (1024 * 1024 * 1024),
  }));

  const chartData = partitionData.slice(0, 20).map((p) => ({
    name: p.partitionLabel.length > 30 ? p.partitionLabel.substring(0, 30) + '...' : p.partitionLabel,
    files: p.fileCount,
    records: p.recordCount,
    sizeGB: Number(p.sizeGB.toFixed(2)),
  }));

  const pieData = partitionData.slice(0, 10).map((p) => ({
    name: p.partitionLabel.length > 20 ? p.partitionLabel.substring(0, 20) + '...' : p.partitionLabel,
    value: p.fileCount,
  }));

  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8', '#82ca9d', '#ffc658', '#ff7300', '#8dd1e1', '#d084d0'];

  return (
    <div className="space-y-6">
      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
        <h3 className="font-semibold text-gray-800 dark:text-white mb-4">
          Partition Overview
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
          <div className="bg-white dark:bg-gray-800 rounded-lg p-3">
            <div className="text-sm text-gray-600 dark:text-gray-400">Total Partitions</div>
            <div className="text-2xl font-bold text-gray-800 dark:text-white">
              {partitionData.length}
            </div>
          </div>
          <div className="bg-white dark:bg-gray-800 rounded-lg p-3">
            <div className="text-sm text-gray-600 dark:text-gray-400">Partition Keys</div>
            <div className="text-sm font-semibold text-gray-800 dark:text-white">
              {partitionKeys.join(', ') || 'None'}
            </div>
          </div>
          <div className="bg-white dark:bg-gray-800 rounded-lg p-3">
            <div className="text-sm text-gray-600 dark:text-gray-400">Avg Files/Partition</div>
            <div className="text-2xl font-bold text-gray-800 dark:text-white">
              {partitionData.length > 0
                ? Math.round(metadata.dataFiles.length / partitionData.length)
                : 0}
            </div>
          </div>
        </div>
      </div>

      {chartData.length > 0 && (
        <>
          <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
            <h3 className="font-semibold text-gray-800 dark:text-white mb-4">
              Files per Partition (Top 20)
            </h3>
            <ResponsiveContainer width="100%" height={400}>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="name"
                  angle={-45}
                  textAnchor="end"
                  height={100}
                  tick={{ fontSize: 10 }}
                />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="files" fill="#8884d8" name="File Count" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
            <h3 className="font-semibold text-gray-800 dark:text-white mb-4">
              Records per Partition (Top 20)
            </h3>
            <ResponsiveContainer width="100%" height={400}>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="name"
                  angle={-45}
                  textAnchor="end"
                  height={100}
                  tick={{ fontSize: 10 }}
                />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="records" fill="#82ca9d" name="Record Count" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
            <h3 className="font-semibold text-gray-800 dark:text-white mb-4">
              Size Distribution (Top 10 Partitions)
            </h3>
            <ResponsiveContainer width="100%" height={400}>
              <PieChart>
                <Pie
                  data={pieData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                  outerRadius={120}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {pieData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </>
      )}

      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
        <h3 className="font-semibold text-gray-800 dark:text-white mb-4">
          Partition Details
        </h3>
        <div className="space-y-2 max-h-96 overflow-y-auto">
          {partitionData.map((p) => (
            <div
              key={p.id}
              className="bg-white dark:bg-gray-800 rounded-lg p-4 border border-gray-200 dark:border-gray-700"
            >
              <div className="font-semibold text-gray-800 dark:text-white mb-2">
                {p.partitionLabel || 'Default Partition'}
              </div>
              <div className="grid grid-cols-3 gap-4 text-sm">
                <div>
                  <span className="text-gray-600 dark:text-gray-400">Files: </span>
                  <span className="font-semibold text-gray-800 dark:text-white">{p.fileCount}</span>
                </div>
                <div>
                  <span className="text-gray-600 dark:text-gray-400">Records: </span>
                  <span className="font-semibold text-gray-800 dark:text-white">
                    {p.recordCount.toLocaleString()}
                  </span>
                </div>
                <div>
                  <span className="text-gray-600 dark:text-gray-400">Size: </span>
                  <span className="font-semibold text-gray-800 dark:text-white">
                    {(p.totalSize / (1024 * 1024 * 1024)).toFixed(2)} GB
                  </span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

