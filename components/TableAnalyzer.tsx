'use client';

import { useState, useEffect } from 'react';
import { Loader2, Database, FileText, BarChart3, Layers, Table2, GitCompare, RefreshCw, Network } from 'lucide-react';
import { TableInfo, TableMetadata } from '@/types';
import api from '@/lib/api';
import MetadataView from './MetadataView';
import SchemaView from './SchemaView';
import PartitionView from './PartitionView';
import StatsView from './StatsView';
import SampleDataView from './SampleDataView';
import SnapshotComparisonView from './SnapshotComparisonView';
import IcebergGraphView from './IcebergGraphView';

interface TableAnalyzerProps {
  tableInfo: TableInfo;
}

export default function TableAnalyzer({ tableInfo }: TableAnalyzerProps) {
  const [metadata, setMetadata] = useState<TableMetadata | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'overview' | 'schema' | 'partitions' | 'stats' | 'sample' | 'snapshots' | 'graph'>('overview');

  useEffect(() => {
    if (tableInfo) {
      loadTableMetadata();
    }
  }, [tableInfo]);

  const loadTableMetadata = async () => {
    try {
      setLoading(true);
      setMetadata(null); // Clear previous data to show loading state
      setError(null);
      const params: { bucket: string; path: string; project_id?: string } = {
        bucket: tableInfo.bucket,
        path: tableInfo.path,
      };
      if (tableInfo.projectId) {
        params.project_id = tableInfo.projectId;
      }
      const response = await api.get('/analyze', { params });
      setMetadata(response.data);
    } catch (err) {
      // Extract detailed error message if available
      let errorMessage = 'Failed to load table metadata. Make sure this is a valid Iceberg table.';
      if (err && typeof err === 'object' && 'response' in err) {
        const axiosError = err as { response?: { data?: { detail?: string } } };
        if (axiosError.response?.data?.detail) {
          errorMessage = axiosError.response.data.detail;
        }
      }
      setError(errorMessage);
      console.error('Error loading metadata:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="h-full flex flex-col">
      <div className="flex-shrink-0 border-b border-gray-200 dark:border-gray-700 mb-4">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between mb-2 gap-4">
          <nav className="flex space-x-4 overflow-x-auto pb-2 sm:pb-0 no-scrollbar">
          <button
            onClick={() => setActiveTab('overview')}
              className={`px-4 py-2 font-medium text-sm transition-colors whitespace-nowrap ${
              activeTab === 'overview'
                ? 'border-b-2 border-blue-500 text-blue-600 dark:text-blue-400'
                : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
          >
            <Database className="inline h-4 w-4 mr-1" />
            Overview
          </button>
          <button
            onClick={() => setActiveTab('schema')}
              className={`px-4 py-2 font-medium text-sm transition-colors whitespace-nowrap ${
              activeTab === 'schema'
                ? 'border-b-2 border-blue-500 text-blue-600 dark:text-blue-400'
                : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
          >
            <FileText className="inline h-4 w-4 mr-1" />
            Schema
          </button>
          <button
            onClick={() => setActiveTab('partitions')}
              className={`px-4 py-2 font-medium text-sm transition-colors whitespace-nowrap ${
              activeTab === 'partitions'
                ? 'border-b-2 border-blue-500 text-blue-600 dark:text-blue-400'
                : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
          >
            <Layers className="inline h-4 w-4 mr-1" />
            Partitions
          </button>
          <button
            onClick={() => setActiveTab('stats')}
              className={`px-4 py-2 font-medium text-sm transition-colors whitespace-nowrap ${
              activeTab === 'stats'
                ? 'border-b-2 border-blue-500 text-blue-600 dark:text-blue-400'
                : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
          >
            <BarChart3 className="inline h-4 w-4 mr-1" />
            Statistics
          </button>
          <button
            onClick={() => setActiveTab('sample')}
              className={`px-4 py-2 font-medium text-sm transition-colors whitespace-nowrap ${
              activeTab === 'sample'
                ? 'border-b-2 border-blue-500 text-blue-600 dark:text-blue-400'
                : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
          >
            <Table2 className="inline h-4 w-4 mr-1" />
            Sample Data
          </button>
          <button
            onClick={() => setActiveTab('snapshots')}
              className={`px-4 py-2 font-medium text-sm transition-colors whitespace-nowrap ${
              activeTab === 'snapshots'
                ? 'border-b-2 border-blue-500 text-blue-600 dark:text-blue-400'
                : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
          >
            <GitCompare className="inline h-4 w-4 mr-1" />
            Snapshots
          </button>
            <button
              onClick={() => setActiveTab('graph')}
              className={`px-4 py-2 font-medium text-sm transition-colors whitespace-nowrap ${activeTab === 'graph'
                ? 'border-b-2 border-blue-500 text-blue-600 dark:text-blue-400'
                : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
                }`}
            >
              <Network className="inline h-4 w-4 mr-1" />
              Graph
            </button>
          </nav>
          <div className="flex-shrink-0">
            <button
              onClick={loadTableMetadata}
              disabled={loading}
              className="px-3 py-1.5 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 rounded-lg transition-colors flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed w-full sm:w-auto justify-center"
              title="Refresh metadata"
            >
              <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
          </div>
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-auto">
        {loading && !metadata ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="animate-spin h-8 w-8 text-blue-500" />
          </div>
        ) : error ? (
          <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4 text-red-700 dark:text-red-400">
            <div className="font-semibold mb-2">Error Loading Table Metadata</div>
            <div className="text-sm whitespace-pre-wrap font-mono">{error}</div>
          </div>
        ) : !metadata ? (
          <div className="text-center py-8 text-gray-500 dark:text-gray-400">
            No metadata available
          </div>
        ) : (
                <div className="h-full flex flex-col">
            {activeTab === 'overview' && <MetadataView metadata={metadata} />}
            {activeTab === 'schema' && <SchemaView metadata={metadata} />}
            {activeTab === 'partitions' && <PartitionView metadata={metadata} />}
            {activeTab === 'stats' && <StatsView metadata={metadata} />}
            {activeTab === 'sample' && <SampleDataView tableInfo={tableInfo} metadata={metadata} />}
            {activeTab === 'snapshots' && <SnapshotComparisonView tableInfo={tableInfo} metadata={metadata} />}
                  {activeTab === 'graph' && <div className="h-full"><IcebergGraphView tableInfo={tableInfo} /></div>}
                </div>
        )}
      </div>
    </div>
  );
}

