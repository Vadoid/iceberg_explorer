'use client';

import { useState, useEffect } from 'react';
import { Folder, File, Loader2, Search, Database } from 'lucide-react';
import { TableInfo } from '@/types';
import axios from 'axios';

interface BucketBrowserProps {
  onTableSelect: (table: TableInfo) => void;
  onBucketChange: (bucket: string) => void;
  onFolderChange: (folder: string) => void;
}

interface Project {
  id: string;
  name: string;
}

export default function BucketBrowser({
  onTableSelect,
  onBucketChange,
  onFolderChange,
}: BucketBrowserProps) {
  const [projects, setProjects] = useState<Project[]>([]);
  const [selectedProject, setSelectedProject] = useState<string>('');
  const [manualProjectId, setManualProjectId] = useState<string>('');
  const [useManualProject, setUseManualProject] = useState(false);
  const [buckets, setBuckets] = useState<string[]>([]);
  const [selectedBucket, setSelectedBucket] = useState<string>('');
  const [currentPath, setCurrentPath] = useState<string>('');
  const [folders, setFolders] = useState<string[]>([]);
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [items, setItems] = useState<Array<{name: string; type: string; path: string; table?: TableInfo}>>([]);
  const [discoveredTables, setDiscoveredTables] = useState<TableInfo[]>([]);
  const [showDiscovered, setShowDiscovered] = useState(false);
  const [discovering, setDiscovering] = useState(false);
  const [loading, setLoading] = useState(false);
  const [loadingBuckets, setLoadingBuckets] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');

  useEffect(() => {
    loadProjects();
  }, []);

  useEffect(() => {
    const projectToUse = useManualProject ? manualProjectId : selectedProject;
    if (projectToUse) {
      loadBuckets();
    } else {
      setBuckets([]);
      setSelectedBucket('');
    }
  }, [selectedProject, manualProjectId, useManualProject]);

  const loadProjects = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await axios.get('/api/backend/projects');
      const projectsList = response.data.projects || [];
      setProjects(projectsList);
      
      // Log additional info if available
      if (response.data.total_found !== undefined) {
        console.log(`Found ${response.data.total_found} total projects, ${response.data.active_count || projectsList.length} active`);
      }
      if (response.data.errors) {
        console.warn('Project loading errors:', response.data.errors);
      }
      
      // Auto-select first project if available
      if (projectsList.length > 0 && !selectedProject) {
        setSelectedProject(projectsList[0].id);
      }
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to load projects';
      setError(`Failed to load projects: ${errorMsg}. Make sure GCS credentials are configured and Resource Manager API is enabled.`);
      console.error('Error loading projects:', err);
    } finally {
      setLoading(false);
    }
  };

  const loadBuckets = async () => {
    const projectToUse = useManualProject ? manualProjectId : selectedProject;
    if (!projectToUse) {
      setBuckets([]);
      setLoadingBuckets(false);
      return;
    }
    
    try {
      setLoadingBuckets(true);
      setError(null);
      const response = await axios.get('/api/backend/buckets', {
        params: { project_id: projectToUse },
      });
      setBuckets(response.data.buckets || []);
      // Clear selected bucket when project changes
      setSelectedBucket('');
      setCurrentPath('');
    } catch (err) {
      setError('Failed to load buckets. Make sure GCS credentials are configured.');
      console.error('Error loading buckets:', err);
    } finally {
      setLoadingBuckets(false);
    }
  };

  const loadFolderContents = async (bucket: string, path: string = '') => {
    const projectToUse = useManualProject ? manualProjectId : selectedProject;
    if (!projectToUse) return;
    
    try {
      setLoading(true);
      setError(null);
      const response = await axios.get('/api/backend/browse', {
        params: { bucket, path, project_id: projectToUse },
      });
      setFolders(response.data.folders || []);
      setTables(response.data.tables || []);
      setItems(response.data.items || []);
      setCurrentPath(path);
      onBucketChange(bucket);
      onFolderChange(path);
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

  const handleItemClick = (item: {name: string; type: string; path: string; table?: TableInfo}, event?: React.MouseEvent) => {
    // If it's an Iceberg table, always select it for exploration
    if (item.type === 'iceberg_table' && item.table) {
      // Clicking an Iceberg table moves it to explore
      // Ensure project ID is set
      const tableWithProject = {
        ...item.table,
        projectId: item.table.projectId || currentProject,
      };
      handleTableClick(tableWithProject);
    } else if (item.type === 'folder') {
      // Check if this folder might be an Iceberg table (has metadata subfolder)
      // For now, allow navigation - user can discover tables to find them
      handleFolderClick(item.name);
    }
  };

  const handleDiscoverTables = async () => {
    if (!selectedBucket || !currentProject) return;
    
    try {
      setDiscovering(true);
      setError(null);
      const response = await axios.get('/api/backend/discover', {
        params: { bucket: selectedBucket, project_id: currentProject },
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

  const currentProject = useManualProject ? manualProjectId : selectedProject;

  return (
    <div className="space-y-4">
      <div>
        <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
          GCP Project
        </label>
        <div className="space-y-2">
          <div className="flex items-center gap-2 mb-2">
            <input
              type="checkbox"
              id="useManualProject"
              checked={useManualProject}
              onChange={(e) => {
                setUseManualProject(e.target.checked);
                if (!e.target.checked) {
                  setManualProjectId('');
                }
              }}
              className="w-4 h-4"
            />
            <label htmlFor="useManualProject" className="text-sm text-gray-600 dark:text-gray-400">
              Enter project ID manually
            </label>
          </div>
          
          {useManualProject ? (
            <input
              type="text"
              value={manualProjectId}
              onChange={(e) => setManualProjectId(e.target.value)}
              placeholder="Enter GCP project ID..."
              className="w-full px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
              disabled={loading}
            />
          ) : (
            <select
              value={selectedProject}
              onChange={(e) => setSelectedProject(e.target.value)}
              className="w-full px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
              disabled={loading}
            >
              <option value="">Select a project...</option>
              {projects.map((project) => (
                <option key={project.id} value={project.id}>
                  {project.name} ({project.id})
                </option>
              ))}
            </select>
          )}
        </div>
        {projects.length > 0 && !useManualProject && (
          <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
            <p>Found {projects.length} project{projects.length !== 1 ? 's' : ''}.</p>
            {projects.length === 1 && (
              <p className="mt-1">If you have access to more projects, check Resource Manager API permissions or enter project ID manually.</p>
            )}
          </div>
        )}
      </div>

      {!selectedBucket ? (
        <div>
          <h3 className="text-lg font-medium mb-3 text-gray-700 dark:text-gray-300">
            Select a GCS Bucket
          </h3>
          {loadingBuckets ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="animate-spin h-6 w-6 text-blue-500" />
              <span className="ml-2 text-gray-600 dark:text-gray-400">Searching for buckets...</span>
            </div>
          ) : error ? (
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4 text-red-700 dark:text-red-400">
              {error}
            </div>
          ) : (
            <div className="space-y-2 max-h-96 overflow-y-auto">
              {!currentProject ? (
                <p className="text-gray-500 dark:text-gray-400 text-center py-4">
                  Please select a GCP project first.
                </p>
              ) : buckets.length === 0 ? (
                <p className="text-gray-500 dark:text-gray-400 text-center py-4">
                  No buckets found in this project. Check your GCS credentials.
                </p>
              ) : (
                buckets.map((bucket) => (
                  <button
                    key={bucket}
                    onClick={() => handleBucketSelect(bucket)}
                    className="w-full text-left px-4 py-3 bg-gray-50 dark:bg-gray-700 hover:bg-gray-100 dark:hover:bg-gray-600 rounded-lg transition-colors flex items-center gap-2"
                  >
                    <Folder className="h-5 w-5 text-blue-500" />
                    <span className="font-medium text-gray-700 dark:text-gray-300">
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
          <div className="flex items-center gap-2 mb-4">
            {currentPath && (
              <button
                onClick={navigateUp}
                className="px-3 py-1 bg-gray-200 dark:bg-gray-700 hover:bg-gray-300 dark:hover:bg-gray-600 rounded text-sm"
              >
                ← Up
              </button>
            )}
            <div className="flex-1 text-sm text-gray-600 dark:text-gray-400">
              <span className="font-medium">{selectedBucket}</span>
              {currentPath && <span> / {currentPath}</span>}
            </div>
            <button
              onClick={handleDiscoverTables}
              disabled={discovering || !selectedBucket}
              className="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:bg-gray-400 text-white rounded-lg text-sm font-medium flex items-center gap-2"
            >
              {discovering ? (
                <>
                  <Loader2 className="animate-spin h-4 w-4" />
                  Discovering...
                </>
              ) : (
                <>
                  <Search className="h-4 w-4" />
                  Discover Iceberg Tables
                </>
              )}
            </button>
          </div>

          <div className="relative mb-4">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search folders and tables..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
            />
          </div>

          {loading ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="animate-spin h-6 w-6 text-blue-500" />
            </div>
          ) : error ? (
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4 text-red-700 dark:text-red-400">
              {error}
            </div>
          ) : (
            <div className="space-y-4 max-h-96 overflow-y-auto">
              {showDiscovered && discoveredTables.length > 0 && (
                <div className="mb-4 p-4 bg-indigo-50 dark:bg-indigo-900/20 rounded-lg border border-indigo-200 dark:border-indigo-800">
                  <div className="flex items-center justify-between mb-3">
                    <h4 className="font-semibold text-indigo-900 dark:text-indigo-200">
                      Discovered {discoveredTables.length} Iceberg Table{discoveredTables.length !== 1 ? 's' : ''}
                    </h4>
                    <button
                      onClick={() => setShowDiscovered(false)}
                      className="text-sm text-indigo-600 dark:text-indigo-400 hover:text-indigo-800 dark:hover:text-indigo-200"
                    >
                      Hide
                    </button>
                  </div>
                  <div className="space-y-2 max-h-64 overflow-y-auto">
                    {discoveredTables.map((table) => (
                      <button
                        key={table.location}
                        onClick={() => {
                          const tableWithProject = {
                            ...table,
                            projectId: table.projectId || currentProject,
                          };
                          handleTableClick(tableWithProject);
                        }}
                        className="w-full text-left px-4 py-2 bg-white dark:bg-gray-800 hover:bg-indigo-50 dark:hover:bg-indigo-900/30 rounded-lg transition-colors flex items-center gap-2 border border-indigo-200 dark:border-indigo-700"
                      >
                        <Database className="h-4 w-4 text-indigo-500" />
                        <div className="flex-1 min-w-0">
                          <div className="font-medium text-gray-700 dark:text-gray-300 truncate">
                            {table.name}
                          </div>
                          <div className="text-xs text-gray-500 dark:text-gray-400 truncate">
                            {table.path}
                          </div>
                        </div>
                      </button>
                    ))}
                  </div>
                </div>
              )}
              
              {filteredItems.length > 0 ? (
                <div className="space-y-1">
                  {filteredItems.map((item, idx) => (
                    <button
                      key={`${item.path}-${idx}`}
                      onClick={() => handleItemClick(item)}
                      className={`w-full text-left px-4 py-2 rounded-lg transition-colors flex items-center gap-2 ${
                        item.type === 'iceberg_table'
                          ? 'bg-indigo-50 dark:bg-indigo-900/20 hover:bg-indigo-100 dark:hover:bg-indigo-900/30 border border-indigo-200 dark:border-indigo-800'
                          : 'bg-gray-50 dark:bg-gray-700 hover:bg-gray-100 dark:hover:bg-gray-600'
                      }`}
                    >
                      {item.type === 'iceberg_table' ? (
                        <>
                          <Database className="h-4 w-4 text-indigo-500" />
                          <span className="text-gray-700 dark:text-gray-300 font-medium">
                            {item.name}
                          </span>
                          <span className="ml-auto text-xs bg-indigo-200 dark:bg-indigo-800 text-indigo-800 dark:text-indigo-200 px-2 py-1 rounded">
                            Iceberg
                          </span>
                        </>
                      ) : (
                        <>
                          <Folder className="h-4 w-4 text-blue-500" />
                          <span className="text-gray-700 dark:text-gray-300">
                            {item.name}
                          </span>
                        </>
                      )}
                    </button>
                  ))}
                </div>
              ) : (
                <p className="text-gray-500 dark:text-gray-400 text-center py-4">
                  {searchTerm
                    ? 'No results found'
                    : 'No folders or tables in this location'}
                </p>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

