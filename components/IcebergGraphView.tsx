import { useState, useEffect } from 'react';
import { Loader2, X, RefreshCw, Table as TableIcon, Database, ZoomIn, ZoomOut, ChevronRight, ChevronDown, FileText, Layers, Box } from 'lucide-react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { TableInfo, SampleData } from '@/types';
import api from '@/lib/api';
import IcebergTree, { TreeNode } from './IcebergTree';

interface IcebergGraphViewProps {
  tableInfo: TableInfo;
}

interface GraphData {
  tableName: string;
  location: string;
  currentSnapshotId?: string;
  catalog?: {
    name: string;
    type: string;
    description?: string;
  };
  metadataFiles?: {
    file: string;
    version: number;
    currentSnapshotId: string | null;
    previousMetadataFile: string | null;
    timestamp: number;
  }[];
  snapshots: {
    snapshotId: string;
    timestamp: string | null;
    manifestList: string;
    manifests: {
      path: string;
      length: number;
      partitionSpecId: number;
      addedSnapshotId: number;
      added_data_files_count?: number;
      totalPartitionCount?: number;
      partitions?: {
        name: string;
        fileCount: number;
        dataFiles: {
          path: string;
          format: string;
          recordCount: number;
          fileSizeInBytes: number;
        }[];
      }[];
      dataFiles?: {
        path: string;
        format: string;
        recordCount: number;
        fileSizeInBytes: number;
      }[];
    }[];
    summary: Record<string, string>;
    parentId?: string | null;
  }[];
}

export default function IcebergGraphView({ tableInfo }: IcebergGraphViewProps) {
  const [graphData, setGraphData] = useState<GraphData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState<TreeNode | null>(null);

  // Sample Data State
  const [showSampleModal, setShowSampleModal] = useState(false);
  const [sampleData, setSampleData] = useState<SampleData | null>(null);
  const [sampleLoading, setSampleLoading] = useState(false);
  const [sampleError, setSampleError] = useState<string | null>(null);

  useEffect(() => {
    loadGraphData();
  }, [tableInfo]);

  const loadGraphData = async () => {
    setLoading(true);
    setGraphData(null);
    setError(null);
    setSelectedNode(null);
    try {
      // Use the centralized API client
      const response = await api.get('/analyze', {
        params: {
          bucket: tableInfo.bucket,
          path: tableInfo.path,
          project_id: tableInfo.projectId
        }
      });
      setGraphData(response.data);
    } catch (err: any) {
      console.error("Error loading graph data:", err);
      setError(err.response?.data?.detail || err.message || "Failed to load graph data");
    } finally {
      setLoading(false);
    }
  };

  const handleNodeSelect = (node: TreeNode) => {
    setSelectedNode(node);
  };

  const handleSampleData = async () => {
    if (!selectedNode) return;

    setSampleLoading(true);
    setSampleError(null);
    setSampleData(null);
    setShowSampleModal(true);

    try {
      const params: any = {
        bucket: tableInfo.bucket,
        path: tableInfo.path,
        limit: 100,
        project_id: tableInfo.projectId
      };

      if (selectedNode.type === 'snapshot' && selectedNode.data?.snapshotId) {
        params.snapshot_id = selectedNode.data.snapshotId;
      } else if (selectedNode.type === 'manifest' && selectedNode.data?.manifestPath) {
        params.manifest_path = selectedNode.data.manifestPath;
      } else if (selectedNode.type === 'data-file' && selectedNode.data?.filePath) {
        params.file_path = selectedNode.data.filePath;
      } else if (selectedNode.type === 'table') {
        // Default sample (current snapshot)
      } else {
        // For other nodes, maybe just try default or show error?
        // Metadata nodes don't have direct data.
        // Manifest lists don't have direct data (well they have manifests).
      }

      const response = await api.get('/sample', { params });
      setSampleData(response.data);
    } catch (err: any) {
      console.error("Error loading sample data:", err);
      setSampleError(err.response?.data?.detail || err.message || "Failed to load sample data");
    } finally {
      setSampleLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full text-slate-500">
        <Loader2 className="w-6 h-6 animate-spin mr-2" />
        Analyzing table structure...
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-red-500 p-4">
        <p className="font-semibold mb-2">Error loading table data</p>
        <p className="text-sm text-center max-w-lg bg-red-50 p-4 rounded border border-red-100 font-mono">
          {error}
        </p>
        <button
          onClick={loadGraphData}
          className="mt-4 px-4 py-2 bg-white border border-slate-200 rounded hover:bg-slate-50 text-slate-600 text-sm"
        >
          Retry
        </button>
      </div>
    );
  }

  if (!graphData) {
    return (
      <div className="flex items-center justify-center h-full text-slate-400">
        No data available
      </div>
    );
  }

  const canSample = selectedNode && ['table', 'snapshot', 'manifest', 'data-file'].includes(selectedNode.type);

  return (
    <div className="w-full h-full bg-slate-50 relative">
      <IcebergTree
        data={graphData}
        onNodeSelect={handleNodeSelect}
        selectedNodeId={selectedNode?.id}
      />

      {/* Floating Action Panel */}
      {selectedNode && (
        <div className="absolute bottom-6 right-6 bg-white p-4 rounded-lg shadow-lg border border-slate-200 max-w-sm animate-in slide-in-from-bottom-4 fade-in duration-200">
          <div className="flex items-center justify-between mb-2">
            <h3 className="font-semibold text-slate-800">{selectedNode.label}</h3>
            <button onClick={() => setSelectedNode(null)} className="text-slate-400 hover:text-slate-600">
              <X size={16} />
            </button>
          </div>
          <div className="text-xs text-slate-500 mb-4 font-mono break-all">
            {selectedNode.subLabel}
          </div>

          {canSample && (
            <button
              onClick={handleSampleData}
              className="w-full py-2 bg-blue-500 text-white rounded hover:bg-blue-600 flex items-center justify-center gap-2 text-sm font-medium transition-colors"
            >
              <TableIcon size={16} />
              Sample Data (10 rows)
            </button>
          )}

          {['snapshot', 'manifest', 'manifest-list', 'metadata'].includes(selectedNode.type) && (
            <button
              onClick={() => {
                // Add details with path to manifest and manifest list nodes
                const messageContent = selectedNode.type === 'manifest-list' && selectedNode.details?.path
                  ? JSON.stringify({ ...selectedNode.data, ...selectedNode.details, path: selectedNode.details.path }, null, 2)
                  : selectedNode.type === 'manifest' && selectedNode.details?.path
                    ? JSON.stringify({ ...selectedNode.data, ...selectedNode.details, path: selectedNode.details.path }, null, 2)
                    : JSON.stringify(selectedNode.data || selectedNode.details || {}, null, 2);

                setSampleData({
                  rows: [],
                  columns: [],
                  totalRows: 0,
                  filesRead: 0,
                  message: messageContent
                });
                setShowSampleModal(true);
              }}
              className="w-full mt-2 py-2 bg-slate-100 text-slate-700 border border-slate-200 rounded hover:bg-slate-200 flex items-center justify-center gap-2 text-sm font-medium transition-colors"
            >
              <Database size={16} />
              View Metadata
            </button>
          )}
        </div>
      )}

      {/* Sample Data Modal */}
      {showSampleModal && (
        <div className="absolute inset-0 z-50 bg-black/50 flex items-center justify-center p-8 backdrop-blur-sm">
          <div className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl w-full max-w-5xl h-full max-h-[80vh] flex flex-col overflow-hidden border border-gray-200 dark:border-gray-700">
            <div className="flex items-center justify-between p-4 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/50">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg text-blue-600 dark:text-blue-400">
                  <TableIcon size={20} />
                </div>
                <div>
                  <h3 className="font-semibold text-gray-900 dark:text-gray-100">Sample Data</h3>
                  <p className="text-xs text-gray-500 dark:text-gray-400">
                    Source: {selectedNode?.label} ({selectedNode?.type})
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={handleSampleData}
                  className="p-2 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
                  title="Refresh"
                >
                  <RefreshCw size={18} className={sampleLoading ? "animate-spin" : ""} />
                </button>
                <button
                  onClick={() => setShowSampleModal(false)}
                  className="p-2 text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
                >
                  <X size={20} />
                </button>
              </div>
            </div>

            <div className="flex-1 overflow-auto p-0">
              {sampleLoading ? (
                <div className="flex flex-col items-center justify-center h-full text-gray-500 gap-3">
                  <Loader2 className="w-8 h-8 animate-spin text-blue-500" />
                  <p className="text-sm font-medium">Fetching sample data...</p>
                </div>
              ) : sampleError ? (
                <div className="flex flex-col items-center justify-center h-full p-8 text-center">
                  <div className="bg-red-50 dark:bg-red-900/20 p-4 rounded-full mb-4">
                    <X className="w-8 h-8 text-red-500" />
                  </div>
                  <h3 className="font-semibold text-red-600 dark:text-red-400 mb-2">Failed to load data</h3>
                  <p className="text-sm text-gray-600 dark:text-gray-300 max-w-md bg-red-50 dark:bg-red-900/10 p-3 rounded border border-red-100 dark:border-red-800 font-mono">
                    {sampleError}
                  </p>
                </div>
              ) : sampleData && sampleData.rows.length > 0 ? (
                <div className="overflow-auto h-full">
                  <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700 border-collapse">
                    <thead className="bg-gray-50 dark:bg-gray-800 sticky top-0 z-10 shadow-sm">
                      <tr>
                        {sampleData.columns.map((col) => (
                          <th
                            key={col}
                            className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800"
                          >
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
                      {sampleData.rows.map((row, idx) => (
                        <tr key={idx} className="hover:bg-blue-50/50 dark:hover:bg-blue-900/10 transition-colors">
                          {sampleData.columns.map((col) => (
                            <td
                              key={col}
                              className="px-6 py-3 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100 border-r border-transparent last:border-r-0"
                            >
                              {row[col] !== null && row[col] !== undefined ? String(row[col]) : <span className="text-gray-300 dark:text-gray-600 italic">null</span>}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="flex flex-col items-center justify-center h-full text-gray-400 p-8">
                  {sampleData?.message ? (
                    <pre className="text-left w-full h-full overflow-auto text-xs font-mono text-slate-700 dark:text-slate-300 bg-slate-50 dark:bg-slate-900 p-4 rounded border border-slate-200 dark:border-slate-700 whitespace-pre-wrap">
                      {sampleData.message}
                    </pre>
                  ) : (
                    <>
                      <Database className="w-12 h-12 mb-3 opacity-20" />
                      <p>No data found in this selection</p>
                    </>
                  )}
                </div>
              )}
            </div>

            {sampleData && (
              <div className="p-3 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 text-xs text-gray-500 dark:text-gray-400 flex justify-between px-6">
                <span>{sampleData.rows.length} rows</span>
                <span>{sampleData.filesRead} file(s) read</span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
