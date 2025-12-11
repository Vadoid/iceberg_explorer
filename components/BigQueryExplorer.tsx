'use client';

import { useState, useEffect } from 'react';
import { Database, Table, Loader2, ChevronRight, ChevronDown } from 'lucide-react';
import { TableInfo, BigQueryDataset, BigQueryTable } from '@/types';
import api from '@/lib/api';

interface BigQueryExplorerProps {
  projectId: string;
  onTableSelect: (table: TableInfo) => void;
}

export default function BigQueryExplorer({ projectId, onTableSelect }: BigQueryExplorerProps) {
  const [datasets, setDatasets] = useState<BigQueryDataset[]>([]);
  const [expandedDatasets, setExpandedDatasets] = useState<Set<string>>(new Set());
  const [datasetTables, setDatasetTables] = useState<Record<string, BigQueryTable[]>>({});
  const [loading, setLoading] = useState(false);
  const [loadingTables, setLoadingTables] = useState<Record<string, boolean>>({});
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (projectId) {
      loadDatasets();
    } else {
      setDatasets([]);
    }
  }, [projectId]);

  const loadDatasets = async () => {
    if (!projectId) return;

    try {
      setLoading(true);
      setError(null);
      const response = await api.get('/bigquery/datasets', {
        params: { project_id: projectId },
      });
      setDatasets(response.data.datasets || []);
    } catch (err) {
      console.error('Error loading datasets:', err);
      setError('Failed to load datasets. Ensure BigQuery API is enabled.');
    } finally {
      setLoading(false);
    }
  };

  const toggleDataset = async (datasetId: string) => {
    const newExpanded = new Set(expandedDatasets);
    if (newExpanded.has(datasetId)) {
      newExpanded.delete(datasetId);
      setExpandedDatasets(newExpanded);
    } else {
      newExpanded.add(datasetId);
      setExpandedDatasets(newExpanded);

      // Load tables if not already loaded
      if (!datasetTables[datasetId]) {
        await loadTables(datasetId);
      }
    }
  };

  const loadTables = async (datasetId: string) => {
    try {
      setLoadingTables(prev => ({ ...prev, [datasetId]: true }));
      const response = await api.get('/bigquery/tables', {
        params: { project_id: projectId, dataset_id: datasetId },
      });
      setDatasetTables(prev => ({ ...prev, [datasetId]: response.data.tables || [] }));
    } catch (err) {
      console.error(`Error loading tables for ${datasetId}:`, err);
    } finally {
      setLoadingTables(prev => ({ ...prev, [datasetId]: false }));
    }
  };

  const [searching, setSearching] = useState(false);
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [showSearchResults, setShowSearchResults] = useState(false);

  const searchIcebergTables = async () => {
    if (!projectId) return;
    try {
      setSearching(true);
      setShowSearchResults(true);
      setError(null);
      const response = await api.get('/bigquery/search-iceberg', {
        params: { project_id: projectId },
      });
      setSearchResults(response.data.tables || []);
    } catch (err) {
      console.error('Error searching Iceberg tables:', err);
      setError('Failed to search for Iceberg tables.');
    } finally {
      setSearching(false);
    }
  };

  const openInWorkspace = (location: string) => {
    // Extract bucket and path from gs://bucket/path/to/table
    if (!location.startsWith('gs://')) {
      console.error('Invalid GCS location:', location);
      return;
    }
    const parts = location.slice(5).split('/');
    const bucket = parts[0];
    const path = parts.slice(1).join('/');

    onTableSelect({
      name: path.split('/').pop() || 'table',
      location: location,
      bucket: bucket,
      path: path,
      projectId: projectId
    });
  };

  if (!projectId) {
    return (
      <div className="text-center py-8 text-gray-500 dark:text-gray-400">
        Please select a project to browse BigQuery.
      </div>
    );
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="animate-spin h-6 w-6 text-blue-500" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4 text-red-700 dark:text-red-400 text-sm">
        <p>{error}</p>
        <button
          onClick={() => setError(null)}
          className="mt-2 text-xs font-medium underline"
        >
          Dismiss
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Search Button */}
      <div className="flex justify-end px-2">
        <button
          onClick={searchIcebergTables}
          disabled={searching}
          className="text-xs flex items-center gap-1.5 px-3 py-1.5 bg-blue-50 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 rounded-md hover:bg-blue-100 dark:hover:bg-blue-900/50 transition-colors disabled:opacity-50"
        >
          {searching ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : (
            <Database className="h-3.5 w-3.5" />
          )}
          {searching ? 'Searching...' : 'Search Iceberg Tables'}
        </button>
      </div>

      {showSearchResults && (
        <div className="border-b border-gray-200 dark:border-gray-700 pb-4 mb-4">
          <div className="flex items-center justify-between px-2 mb-2">
            <h3 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider">
              Search Results ({searchResults.length})
            </h3>
            <button
              onClick={() => setShowSearchResults(false)}
              className="text-xs text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
            >
              Close
            </button>
          </div>

          {searchResults.length === 0 && !searching ? (
            <p className="text-sm text-gray-500 dark:text-gray-400 px-2 italic">
              No Iceberg tables found in this project.
            </p>
          ) : (
            <div className="space-y-1 max-h-60 overflow-y-auto">
              {searchResults.map((table) => (
                <div key={table.full_table_id} className="px-2 py-2 hover:bg-gray-50 dark:hover:bg-gray-800 rounded-md group">
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-sm font-medium text-gray-700 dark:text-gray-300 truncate" title={table.full_table_id}>
                      {table.dataset_id}.{table.table_id}
                    </span>
                    <span className="text-[10px] px-1.5 py-0.5 bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 rounded-full">
                      Iceberg
                    </span>
                  </div>
                  <div className="flex items-center gap-2 mt-1.5">
                    <button
                      onClick={() => openInWorkspace(table.location)}
                      className="text-xs text-blue-600 dark:text-blue-400 hover:underline flex items-center gap-1"
                      title={table.location}
                    >
                      <ChevronRight className="h-3 w-3" />
                      Open in Workspace
                    </button>
                    {/* Placeholder for Show Metadata if needed later */}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      <div className="space-y-1">
        {datasets.length === 0 ? (
          <p className="text-sm text-gray-500 dark:text-gray-400 text-center py-4">
            No datasets found.
          </p>
        ) : (
          datasets.map((dataset) => (
            <div key={dataset.dataset_id} className="space-y-1">
              <button
                onClick={() => toggleDataset(dataset.dataset_id)}
                className="w-full text-left px-2 py-1.5 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors flex items-center gap-2 group"
              >
                {expandedDatasets.has(dataset.dataset_id) ? (
                  <ChevronDown className="h-4 w-4 text-gray-400" />
                ) : (
                  <ChevronRight className="h-4 w-4 text-gray-400" />
                )}
                <Database className="h-4 w-4 text-blue-600 dark:text-blue-400" />
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300 truncate">
                  {dataset.dataset_id}
                </span>
              </button>

              {expandedDatasets.has(dataset.dataset_id) && (
                <div className="pl-6 space-y-0.5">
                  {loadingTables[dataset.dataset_id] ? (
                    <div className="py-2 pl-2">
                      <Loader2 className="animate-spin h-3 w-3 text-gray-400" />
                    </div>
                  ) : datasetTables[dataset.dataset_id]?.length === 0 ? (
                    <p className="text-xs text-gray-500 pl-2 py-1">No tables</p>
                  ) : (
                    datasetTables[dataset.dataset_id]?.map((table) => (
                      <button
                        key={table.table_id}
                        onClick={() => {
                          // TODO: Handle BigQuery table selection
                          // For now, we might just show metadata or support Iceberg tables in BQ
                          console.log('Selected BQ table:', table);
                        }}
                        className="w-full text-left px-2 py-1.5 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors flex items-center gap-2"
                      >
                        <Table className="h-3.5 w-3.5 text-gray-500" />
                        <span className="text-sm text-gray-600 dark:text-gray-400 truncate">
                          {table.table_id}
                        </span>
                      </button>
                    ))
                  )}
                </div>
              )}
            </div>
          ))
        )}
      </div>
    </div>
  );
}
