'use client';

import { useState, useEffect } from 'react';
import { Folder, Loader2, Search, Database, Star } from 'lucide-react';
import { TableInfo } from '@/types';
import api from '@/lib/api';

interface BucketBrowserProps {
  projectId: string;
  onTableSelect: (table: TableInfo) => void;
  onToggleFavorite: (table: TableInfo) => void;
  favorites: TableInfo[];
}

export default function BucketBrowser({
  projectId,
  onTableSelect,
  onToggleFavorite,
  favorites,
}: BucketBrowserProps) {
  const [buckets, setBuckets] = useState<string[]>([]);
  const [selectedBucket, setSelectedBucket] = useState<string>('');
  const [currentPath, setCurrentPath] = useState<string>('');
  const [folders, setFolders] = useState<string[]>([]);
  const [items, setItems] = useState<Array<{name: string; type: string; path: string; table?: TableInfo}>>([]);
  const [discoveredTables, setDiscoveredTables] = useState<TableInfo[]>([]);
  const [showDiscovered, setShowDiscovered] = useState(false);
  const [discovering, setDiscovering] = useState(false);
  const [loading, setLoading] = useState(false);
  const [loadingBuckets, setLoadingBuckets] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');

  useEffect(() => {
    if (projectId) {
      loadBuckets();
    } else {
      setBuckets([]);
      setSelectedBucket('');
    }
  }, [projectId]);

  const loadBuckets = async () => {
    if (!projectId) return;
    
    try {
      setLoadingBuckets(true);
      setError(null);
      const response = await api.get('/buckets', {
        params: { project_id: projectId },
      });
      setBuckets(response.data.buckets || []);
      setSelectedBucket('');
      setCurrentPath('');
    } catch (err) {
      let errorMessage = 'Failed to load buckets.';
      if (err && typeof err === 'object' && 'response' in err) {
        const axiosError = err as { response?: { status?: number; data?: { detail?: string } } };
        if (axiosError.response?.status === 401) {
          errorMessage = 'Authentication failed. Please sign in again.';
        } else if (axiosError.response?.status === 403) {
          errorMessage = axiosError.response.data?.detail || 'Access denied. You do not have permission to view buckets in this project.';
          errorMessage += ' Ensure you have "storage.buckets.list" permission.';
        } else if (axiosError.response?.data?.detail) {
          errorMessage = axiosError.response.data.detail;
        }
      }
      setError(errorMessage);
      console.error('Error loading buckets:', err);
    } finally {
      setLoadingBuckets(false);
    }
  };

  const loadFolderContents = async (bucket: string, path: string = '') => {
    if (!projectId) return;
    
    try {
      setLoading(true);
      setError(null);
      const response = await api.get('/browse', {
        params: { bucket, path, project_id: projectId },
      });
      setFolders(response.data.folders || []);
      setItems(response.data.items || []);
      setCurrentPath(path);
    } catch (err) {
      setError('Failed to load folder contents.');
      console.error('Error loading folder:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleBucketSelect = (bucket: string) => {
    setSelectedBucket(bucket);
    loadFolderContents(bucket, '');
  };

  const handleFolderClick = (folder: string) => {
    const newPath = currentPath ? `${currentPath}/${folder}` : folder;
    loadFolderContents(selectedBucket, newPath);
  };

  const handleTableClick = (table: TableInfo) => {
    onTableSelect(table);
  };

  const handleItemClick = (item: { name: string; type: string; path: string; table?: TableInfo }, event?: React.MouseEvent) => {
    if (item.type === 'iceberg_table' && item.table) {
      const tableWithProject = {
        ...item.table,
        projectId: item.table.projectId || projectId,
      };
      handleTableClick(tableWithProject);
    } else if (item.type === 'folder') {
      handleFolderClick(item.name);
    }
  };

  const handleDiscoverTables = async () => {
    if (!selectedBucket || !projectId) return;
    
    try {
      setDiscovering(true);
      setError(null);
      const response = await api.get('/discover', {
        params: { bucket: selectedBucket, project_id: projectId },
      });
      const tables = response.data.tables || [];
      setDiscoveredTables(tables);
      setShowDiscovered(true);
    } catch (err) {
      setError('Failed to discover Iceberg tables.');
      console.error('Error discovering tables:', err);
    } finally {
      setDiscovering(false);
    }
  };

  const navigateUp = () => {
    if (currentPath) {
      const parentPath = currentPath.split('/').slice(0, -1).join('/');
      loadFolderContents(selectedBucket, parentPath);
    }
  };

  const filteredItems = items.filter((item) =>
    item.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const isFavorite = (table: TableInfo) => {
    return favorites.some(f => f.location === table.location);
  };

  if (!projectId) {
    return (
      <div className="text-center py-8 text-gray-500 dark:text-gray-400">
        Please select a project to browse buckets.
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {!selectedBucket ? (
        <div>
          <h3 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-3">
            Buckets
          </h3>
          {loadingBuckets ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="animate-spin h-6 w-6 text-blue-500" />
            </div>
          ) : error ? (
              <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4 text-red-700 dark:text-red-400 text-sm">
                <p>{error}</p>
            </div>
          ) : (
                <div className="space-y-1">
                  {buckets.length === 0 ? (
                    <p className="text-sm text-gray-500 dark:text-gray-400 text-center py-4">
                      No buckets found.
                </p>
              ) : (
                buckets.map((bucket) => (
                  <button
                    key={bucket}
                    onClick={() => handleBucketSelect(bucket)}
                    className="w-full text-left px-3 py-2 bg-gray-50 dark:bg-gray-700/50 hover:bg-gray-100 dark:hover:bg-gray-600 rounded-lg transition-colors flex items-center gap-2 group"
                  >
                    <Folder className="h-4 w-4 text-blue-500 group-hover:text-blue-600" />
                    <span className="text-sm font-medium text-gray-700 dark:text-gray-300 truncate">
                      {bucket}
                    </span>
                  </button>
                ))
              )}
            </div>
          )}
        </div>
      ) : (
        <div>
            <div className="flex flex-col gap-3 mb-4">
              <div className="flex items-center gap-2">
                <button
                  onClick={() => {
                    if (currentPath) {
                      navigateUp();
                    } else {
                      setSelectedBucket('');
                    }
                  }}
                  className="px-2 py-1 hover:bg-gray-100 dark:hover:bg-gray-700 rounded text-xs text-gray-500 dark:text-gray-400 flex-shrink-0"
                >
                  ← Back
                </button>
                <div className="flex-1 text-sm text-gray-600 dark:text-gray-400 min-w-0 break-all">
                  <span className="font-medium">{selectedBucket}</span>
                  {currentPath && <span> / {currentPath}</span>}
                </div>
            </div>

            <button
              onClick={handleDiscoverTables}
                disabled={discovering}
                className="w-full px-3 py-2 bg-indigo-50 dark:bg-indigo-900/20 hover:bg-indigo-100 dark:hover:bg-indigo-900/40 text-indigo-700 dark:text-indigo-300 rounded-lg text-xs font-medium flex items-center justify-center gap-2 transition-colors"
            >
              {discovering ? (
                <>
                    <Loader2 className="animate-spin h-3 w-3" />
                  Discovering...
                </>
              ) : (
                <>
                      <Search className="h-3 w-3" />
                      Scan for Tables
                </>
              )}
            </button>
          </div>

          <div className="relative mb-4">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-3.5 w-3.5 text-gray-400" />
            <input
              type="text"
                placeholder="Filter..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full pl-9 pr-4 py-1.5 border border-gray-200 dark:border-gray-700 rounded-lg bg-gray-50 dark:bg-gray-800 text-sm text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          {loading ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="animate-spin h-6 w-6 text-blue-500" />
            </div>
          ) : error ? (
                <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4 text-red-700 dark:text-red-400 text-sm">
                  <p>{error}</p>
            </div>
          ) : (
                  <div className="space-y-4">
              {showDiscovered && discoveredTables.length > 0 && (
                      <div className="mb-4 p-3 bg-indigo-50 dark:bg-indigo-900/20 rounded-lg border border-indigo-100 dark:border-indigo-800">
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="text-xs font-semibold text-indigo-900 dark:text-indigo-200 uppercase tracking-wider">
                            Discovered ({discoveredTables.length})
                    </h4>
                    <button
                      onClick={() => setShowDiscovered(false)}
                            className="text-xs text-indigo-600 dark:text-indigo-400 hover:text-indigo-800 dark:hover:text-indigo-200"
                    >
                      Hide
                    </button>
                  </div>
                        <div className="space-y-1 max-h-48 overflow-y-auto">
                    {discoveredTables.map((table) => (
                      <div
                        key={table.location}
                        className="flex items-center gap-2 group"
                      >
                        <button
                          onClick={() => {
                            const tableWithProject = {
                              ...table,
                              projectId: table.projectId || projectId,
                            };
                            handleTableClick(tableWithProject);
                          }}
                          className="flex-1 text-left px-2 py-1.5 rounded hover:bg-white dark:hover:bg-gray-800 transition-colors flex items-center gap-2 min-w-0"
                        >
                          <Database className="h-3.5 w-3.5 text-indigo-500 flex-shrink-0" />
                          <div className="min-w-0 flex-1">
                            <div className="text-sm font-medium text-gray-700 dark:text-gray-300 truncate">
                              {table.name}
                            </div>
                            <div className="text-xs text-gray-500 dark:text-gray-400 truncate" title={table.location}>
                              {table.location}
                            </div>
                          </div>
                        </button>
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            onToggleFavorite({ ...table, projectId: projectId });
                          }}
                          className="p-1 hover:bg-gray-200 dark:hover:bg-gray-700 rounded opacity-0 group-hover:opacity-100 transition-opacity"
                        >
                          <Star
                            className={`h-3.5 w-3.5 ${isFavorite(table) ? 'text-yellow-400 fill-yellow-400' : 'text-gray-400'}`}
                          />
                        </button>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              
              {filteredItems.length > 0 ? (
                      <div className="space-y-0.5">
                  {filteredItems.map((item, idx) => (
                    <div
                      key={`${item.path}-${idx}`}
                      className="flex items-center gap-1 group"
                    >
                      <button
                        onClick={() => handleItemClick(item)}
                        className={`flex-1 text-left px-2 py-1.5 rounded transition-colors flex items-center gap-2 min-w-0 ${item.type === 'iceberg_table'
                          ? 'hover:bg-indigo-50 dark:hover:bg-indigo-900/20'
                          : 'hover:bg-gray-100 dark:hover:bg-gray-700'
                          }`}
                      >
                        {item.type === 'iceberg_table' ? (
                          <Database className="h-3.5 w-3.5 text-indigo-500 flex-shrink-0" />
                        ) : (
                          <Folder className="h-3.5 w-3.5 text-blue-500 flex-shrink-0" />
                        )}
                        <span className="text-sm text-gray-700 dark:text-gray-300 truncate">
                          {item.name}
                        </span>
                        {item.type === 'iceberg_table' && (
                          <span className="ml-auto text-[10px] bg-indigo-100 dark:bg-indigo-900 text-indigo-700 dark:text-indigo-300 px-1.5 py-0.5 rounded">
                            Iceberg
                          </span>
                        )}
                      </button>

                      {item.type === 'iceberg_table' && item.table && (
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            if (item.table) {
                              onToggleFavorite({ ...item.table, projectId: projectId });
                            }
                          }}
                          className="p-1 hover:bg-gray-200 dark:hover:bg-gray-700 rounded opacity-0 group-hover:opacity-100 transition-opacity"
                        >
                          <Star
                            className={`h-3.5 w-3.5 ${isFavorite(item.table) ? 'text-yellow-400 fill-yellow-400' : 'text-gray-400'}`}
                          />
                        </button>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                        <p className="text-sm text-gray-500 dark:text-gray-400 text-center py-4">
                          {searchTerm ? 'No results found' : 'Empty folder'}
                </p>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

