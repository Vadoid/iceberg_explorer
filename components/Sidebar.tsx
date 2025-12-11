'use client';

import { useState, useEffect } from 'react';
import {
  LayoutGrid,
  Database,
  Star,
  ChevronDown,
  ChevronRight,
  Settings,
  Plus,
  Search,
  Loader2
} from 'lucide-react';
import BucketBrowser from './BucketBrowser';
// import BigQueryExplorer from '@/components/BigQueryExplorer';
import { TableInfo } from '@/types';
import api from '@/lib/api';

interface Project {
  id: string;
  name: string;
}

interface SidebarProps {
  width: number;
  onTableSelect: (table: TableInfo) => void;
}

export default function Sidebar({ width, onTableSelect }: SidebarProps) {
  // const [activeTab, setActiveTab] = useState<'storage' | 'bigquery'>('storage');
  const [activeTab, setActiveTab] = useState<'storage'>('storage');
  const [projects, setProjects] = useState<Project[]>([]);
  const [selectedProject, setSelectedProject] = useState<string>('');
  const [manualProjectId, setManualProjectId] = useState<string>('');
  const [useManualProject, setUseManualProject] = useState(false);
  const [loadingProjects, setLoadingProjects] = useState(false);
  const [showProjectInput, setShowProjectInput] = useState(false);
  const [favorites, setFavorites] = useState<TableInfo[]>([]);

  const [isStorageLoaded, setIsStorageLoaded] = useState(false);

  // Load state from localStorage on mount
  useEffect(() => {
    try {
      const storedFavorites = localStorage.getItem('iceberg_favorites');
      if (storedFavorites) {
        setFavorites(JSON.parse(storedFavorites));
      }

      const storedProject = localStorage.getItem('iceberg_selected_project');
      if (storedProject) {
        setSelectedProject(storedProject);
      }
    } catch (e) {
      console.error('Failed to load from localStorage', e);
    } finally {
      setIsStorageLoaded(true);
    }
  }, []);

  // Save favorites to localStorage
  useEffect(() => {
    if (isStorageLoaded) {
      localStorage.setItem('iceberg_favorites', JSON.stringify(favorites));
    }
  }, [favorites, isStorageLoaded]);

  // Load projects
  useEffect(() => {
    loadProjects();
  }, []);

  // Save selected project to localStorage
  useEffect(() => {
    if (isStorageLoaded && selectedProject) {
      localStorage.setItem('iceberg_selected_project', selectedProject);
    }
  }, [selectedProject, isStorageLoaded]);

  const loadProjects = async () => {
    try {
      setLoadingProjects(true);
      const response = await api.get('/projects');
      const projectsList = response.data.projects || [];
      setProjects(projectsList);
    } catch (err) {
      console.error('Failed to load projects', err);
    } finally {
      setLoadingProjects(false);
    }
  };

  // Auto-select removed to require explicit user selection
  // useEffect(() => {
  //   if (isStorageLoaded && projects.length > 0 && !selectedProject) {
  //     setSelectedProject(projects[0].id);
  //   }
  // }, [isStorageLoaded, projects, selectedProject]);

  const toggleFavorite = (table: TableInfo) => {
    if (favorites.some(f => f.location === table.location)) {
      setFavorites(favorites.filter(f => f.location !== table.location));
    } else {
      setFavorites([...favorites, table]);
    }
  };

  const currentProject = useManualProject ? manualProjectId : selectedProject;

  return (
    <aside
      className="flex-shrink-0 border-r border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 flex flex-col relative h-full transition-all duration-75"
      style={{ width }}
    >
      {/* Header */}
      <div className="p-4 border-b border-gray-200 dark:border-gray-700">
        <h1 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2 mb-4">
          <span>🧊</span> Iceberg Explorer
        </h1>

        {/* Project Selector */}
        <div className="space-y-2">
          <label className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider">
            Project
          </label>

          {!showProjectInput ? (
            <div className="relative">
              <select
                value={selectedProject}
                onChange={(e) => {
                  setSelectedProject(e.target.value);
                  setUseManualProject(false);
                }}
                className="w-full appearance-none px-3 py-2 pr-8 bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded-lg text-sm font-medium text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
                disabled={loadingProjects}
              >
                <option value="">Select Project...</option>
                {projects.map(p => (
                  <option key={p.id} value={p.id}>{p.name}</option>
                ))}
              </select>
              <ChevronDown className="absolute right-2 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-500 pointer-events-none" />
            </div>
          ) : (
            <div className="relative">
              <input
                type="text"
                value={manualProjectId}
                onChange={(e) => {
                  setManualProjectId(e.target.value);
                  setUseManualProject(true);
                }}
                placeholder="Enter Project ID"
                className="w-full px-3 py-2 bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded-lg text-sm font-medium text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
          )}

          <button
            onClick={() => setShowProjectInput(!showProjectInput)}
            className="text-xs text-blue-600 dark:text-blue-400 hover:underline flex items-center gap-1"
          >
            {showProjectInput ? 'Select from list' : 'Enter ID manually'}
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-gray-200 dark:border-gray-700">
        <button
          onClick={() => setActiveTab('storage')}
          className={`flex-1 py-3 text-sm font-medium border-b-2 transition-colors ${activeTab === 'storage'
            ? 'border-blue-500 text-blue-600 dark:text-blue-400'
            : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
        >
          Storage
        </button>
        {/* <button
          onClick={() => setActiveTab('bigquery')}
          className={`flex-1 py-3 text-sm font-medium border-b-2 transition-colors ${activeTab === 'bigquery'
            ? 'border-blue-500 text-blue-600 dark:text-blue-400'
            : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
            }`}
        >
          BigQuery
        </button> */}
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto">
        {/* Favorites Section */}
        {favorites.length > 0 && (
          <div className="p-4 border-b border-gray-200 dark:border-gray-700">
            <h3 className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-2 flex items-center gap-1">
              <Star className="h-3 w-3 fill-yellow-400 text-yellow-400" /> Favorites
            </h3>
            <div className="space-y-1">
              {favorites.map(table => (
                <button
                  key={table.location}
                  onClick={() => onTableSelect(table)}
                  className="w-full text-left px-2 py-1.5 rounded hover:bg-gray-100 dark:hover:bg-gray-700 flex items-center gap-2 group"
                >
                  <Database className="h-3.5 w-3.5 text-blue-500 flex-shrink-0" />
                  <div className="min-w-0 flex-1">
                    <div className="text-sm text-gray-700 dark:text-gray-300 truncate">
                      {table.name}
                    </div>
                    <div className="text-xs text-gray-500 dark:text-gray-400 truncate" title={table.location}>
                      {table.location}
                    </div>
                  </div>
                  <Star
                    className="h-3.5 w-3.5 text-yellow-400 fill-yellow-400 opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer flex-shrink-0"
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleFavorite(table);
                    }}
                  />
                </button>
              ))}
            </div>
          </div>
        )}

        <div className="p-4">
          {activeTab === 'storage' ? (
            <BucketBrowser
              projectId={currentProject}
              onTableSelect={onTableSelect}
              onToggleFavorite={toggleFavorite}
              favorites={favorites}
            />
          ) : (
            // <BigQueryExplorer
            //   projectId={currentProject}
            //   onTableSelect={onTableSelect}
            // />
            null
          )}
        </div>
      </div>
    </aside >
  );
}
