import React, { useState, useMemo, useEffect } from 'react';
import { Maximize, Minimize, ZoomIn, ZoomOut } from 'lucide-react';

interface GraphData {
  tableName: string;
  location: string;
  currentSnapshotId?: string;
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

interface TreeNode {
  id: string;
  type: 'table' | 'metadata' | 'snapshot' | 'manifest-list' | 'manifest' | 'data-file' | 'more';
  label: string;
  subLabel?: string;
  details?: any;
  children?: TreeNode[];
  x?: number;
  y?: number;
  width?: number;
  height?: number;
  parentId?: string;
  isHistorical?: boolean;
}

interface IcebergTreeProps {
  data: GraphData;
  className?: string;
}

// Node dimensions
const NODE_WIDTH = 180;
const NODE_HEIGHT = 60;
const LEVEL_HEIGHT = 120;
const SIBLING_GAP = 40;

export default function IcebergTree({ data, className }: IcebergTreeProps) {
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set(['table']));
  const [transform, setTransform] = useState({ x: 0, y: 0, scale: 1 });
  const [dragging, setDragging] = useState(false);
  const [lastMouse, setLastMouse] = useState({ x: 0, y: 0 });
  const [showHistory, setShowHistory] = useState(false);

  // Build the tree structure from flat data
  const rootNode = useMemo(() => {
    if (!data) return null;

    const root: TreeNode = {
      id: 'table',
      type: 'table',
      label: data.tableName,
      subLabel: 'Table',
      children: []
    };

    // Metadata Layer (Latest only for now, or all if we want history)
    // To match the diagram "Table -> Metadata", we usually show the current one.
    // Metadata Layer
    if (data.metadataFiles && data.metadataFiles.length > 0) {
      const sortedMeta = [...data.metadataFiles].sort((a, b) => b.version - a.version);

      // Determine which metadata files to show
      const metadataToShow = showHistory ? sortedMeta : [sortedMeta[0]];

      metadataToShow.forEach((metaFile) => {
        const isLatest = metaFile.version === sortedMeta[0].version;

        const metaNode: TreeNode = {
          id: `meta-${metaFile.version}`,
          type: 'metadata',
          label: `v${metaFile.version}.metadata.json`,
          subLabel: new Date(metaFile.timestamp).toLocaleDateString(),
          children: [],
          isHistorical: !isLatest
        };
        root.children?.push(metaNode);

        // Determine which snapshots to show for this metadata file
        let snapshotsForThisMetadata: typeof data.snapshots = [];

        if (isLatest) {
          // Show all snapshots for latest (as they are usually the full history)
          snapshotsForThisMetadata = data.snapshots;
        } else if (metaFile.currentSnapshotId) {
          // For historical metadata, we only want to show the snapshot tree reachable from its currentSnapshotId
          // We need to traverse parentId backwards from currentSnapshotId
          const relevantSnapshotIds = new Set<string>();
          let currentId: string | null | undefined = metaFile.currentSnapshotId;

          // Safety check to prevent infinite loops
          let iterations = 0;
          while (currentId && iterations < 1000) {
            relevantSnapshotIds.add(currentId);
            const snap = data.snapshots.find(s => s.snapshotId === currentId);
            currentId = snap?.parentId;
            if (!snap) break;
            iterations++;
          }

          snapshotsForThisMetadata = data.snapshots.filter(s => relevantSnapshotIds.has(s.snapshotId));
        }

        if (snapshotsForThisMetadata && snapshotsForThisMetadata.length > 0) {
          const sortedSnapshots = [...snapshotsForThisMetadata].sort((a, b) => {
            const tA = a.timestamp ? new Date(a.timestamp).getTime() : 0;
            const tB = b.timestamp ? new Date(b.timestamp).getTime() : 0;
            return tA - tB; // Ascending s0, s1...
          });

          sortedSnapshots.forEach((snap, idx) => {
            const snapNode: TreeNode = {
              id: `snap-${metaFile.version}-${snap.snapshotId}`, // Unique ID per metadata branch
              type: 'snapshot',
              label: `s${snap.snapshotId.substring(0, 4)}...`,
              subLabel: snap.timestamp ? new Date(snap.timestamp).toLocaleTimeString() : '',
              children: []
            };
            metaNode.children?.push(snapNode);

            // Manifest List
            if (snap.manifestList) {
              const mlNode: TreeNode = {
                id: `ml-${metaFile.version}-${snap.snapshotId}`,
                type: 'manifest-list',
                label: 'Manifest List',
                subLabel: `...avro`,
                children: []
              };
              snapNode.children?.push(mlNode);

              // Manifests
              if (snap.manifests) {
                snap.manifests.forEach((m, mIdx) => {
                  const mNode: TreeNode = {
                    id: `m-${metaFile.version}-${snap.snapshotId}-${mIdx}`,
                    type: 'manifest',
                    label: `Manifest ${mIdx}`,
                    subLabel: m.path.split('/').pop()?.substring(0, 15) + '...',
                    children: []
                  };
                  mlNode.children?.push(mNode);

                  // Data Files
                  if (m.dataFiles) {
                    m.dataFiles.forEach((df, dfIdx) => {
                      const dfNode: TreeNode = {
                        id: `df-${metaFile.version}-${snap.snapshotId}-${mIdx}-${dfIdx}`,
                        type: 'data-file',
                        label: 'Data File',
                        subLabel: df.path.split('/').pop()?.substring(0, 15) + '...',
                        children: []
                      };
                      mNode.children?.push(dfNode);
                    });
                  }
                });
              }
            }
          });
        }
      });
    }

    return root;
  }, [data, showHistory]);

  // Calculate layout
  const layoutNodes = useMemo(() => {
    if (!rootNode) return [];

    const nodes: TreeNode[] = [];
    
    // Recursive layout function
    // Returns the total width of the subtree
    const layout = (node: TreeNode, depth: number, startX: number): number => {
      // If not expanded and not root, don't process children
      const isExpanded = expandedIds.has(node.id);
      
      node.y = depth * LEVEL_HEIGHT + 50;
      node.width = NODE_WIDTH;
      node.height = NODE_HEIGHT;
      
      if (!isExpanded || !node.children || node.children.length === 0) {
        node.x = startX + NODE_WIDTH / 2;
        nodes.push(node);
        return NODE_WIDTH + SIBLING_GAP;
      }

      let currentX = startX;
      let totalWidth = 0;
      
      node.children.forEach(child => {
        const childWidth = layout(child, depth + 1, currentX);
        currentX += childWidth;
        totalWidth += childWidth;
      });

      // Center parent over children
      // The children span from startX to currentX - SIBLING_GAP
      const childrenCenter = startX + (totalWidth - SIBLING_GAP) / 2;
      node.x = childrenCenter;
      
      nodes.push(node);
      
      // The node's own width might be wider than its children (unlikely with this layout but possible)
      return Math.max(totalWidth, NODE_WIDTH + SIBLING_GAP);
    };

    layout(rootNode, 0, 0);
    return nodes;
  }, [rootNode, expandedIds]);

  // Center the tree initially
  useEffect(() => {
    if (layoutNodes.length > 0) {
      const xs = layoutNodes.map(n => n.x || 0);
      const minX = Math.min(...xs);
      const maxX = Math.max(...xs);
      const treeWidth = maxX - minX + NODE_WIDTH;
      const containerWidth = window.innerWidth; // Approximate
      
      setTransform(prev => ({
        ...prev,
        x: (containerWidth - treeWidth) / 2 - minX + NODE_WIDTH/2,
        y: 50
      }));
    }
  }, [layoutNodes.length]); // Only re-center when node count changes significantly (initial load)

  const handleWheel = (e: React.WheelEvent) => {
    if (e.ctrlKey || e.metaKey) {
      e.preventDefault();
      const scaleChange = -e.deltaY * 0.001;
      setTransform(prev => ({
        ...prev,
        scale: Math.max(0.1, Math.min(5, prev.scale + scaleChange))
      }));
    } else {
      setTransform(prev => ({
        ...prev,
        x: prev.x - e.deltaX,
        y: prev.y - e.deltaY
      }));
    }
  };

  const handleMouseDown = (e: React.MouseEvent) => {
    setDragging(true);
    setLastMouse({ x: e.clientX, y: e.clientY });
  };

  const handleMouseMove = (e: React.MouseEvent) => {
    if (dragging) {
      const dx = e.clientX - lastMouse.x;
      const dy = e.clientY - lastMouse.y;
      setTransform(prev => ({ ...prev, x: prev.x + dx, y: prev.y + dy }));
      setLastMouse({ x: e.clientX, y: e.clientY });
    }
  };

  const handleMouseUp = () => {
    setDragging(false);
  };

  const toggleNode = (id: string) => {
    const newExpanded = new Set(expandedIds);
    if (newExpanded.has(id)) {
      newExpanded.delete(id);
    } else {
      newExpanded.add(id);
    }
    setExpandedIds(newExpanded);
  };

  const expandAll = () => {
    const allIds = new Set<string>();
    const traverse = (node: TreeNode) => {
      allIds.add(node.id);
      node.children?.forEach(traverse);
    };
    if (rootNode) traverse(rootNode);
    setExpandedIds(allIds);
  };

  const collapseAll = () => {
    setExpandedIds(new Set(['table']));
  };

  // Render helpers
  const renderNodeShape = (node: TreeNode) => {
    switch (node.type) {
      case 'table':
        return (
          <g>
            <path d="M0,15 C0,7 40,0 90,0 C140,0 180,7 180,15 L180,45 C180,53 140,60 90,60 C40,60 0,53 0,45 Z" fill="#EFF6FF" stroke="#3B82F6" strokeWidth="2" />
            <path d="M0,15 C0,23 40,30 90,30 C140,30 180,23 180,15" fill="none" stroke="#3B82F6" strokeWidth="1" />
          </g>
        );
      case 'metadata':
        if (node.isHistorical) {
          return (
            <g>
              <rect x="0" y="0" width="180" height="60" rx="4" fill="#F1F5F9" stroke="#94A3B8" strokeWidth="2" strokeDasharray="4 2" />
              <path d="M140,0 L180,40 L180,60 L140,60 Z" fill="#E2E8F0" />
            </g>
          );
        }
        return (
          <g>
            <rect x="0" y="0" width="180" height="60" rx="4" fill="#F0FDF4" stroke="#22C55E" strokeWidth="2" />
            <path d="M140,0 L180,40 L180,60 L140,60 Z" fill="#DCFCE7" />
          </g>
        );
      case 'snapshot':
        return (
          <rect x="0" y="0" width="180" height="60" rx="30" fill="#FEF3C7" stroke="#F59E0B" strokeWidth="2" />
        );
      case 'manifest-list':
        return (
          <g>
            <rect x="5" y="-5" width="170" height="60" rx="4" fill="#FFF" stroke="#64748B" strokeWidth="1" strokeDasharray="4 2" />
            <rect x="0" y="0" width="180" height="60" rx="4" fill="#F1F5F9" stroke="#64748B" strokeWidth="2" />
          </g>
        );
      case 'manifest':
        return (
          <rect x="0" y="0" width="180" height="60" rx="4" fill="#F8FAFC" stroke="#94A3B8" strokeWidth="2" />
        );
      case 'data-file':
        return (
          <g>
            <rect x="0" y="0" width="180" height="60" rx="2" fill="#F1F5F9" stroke="#CBD5E1" strokeWidth="1" />
            <path d="M150,10 L170,10 L170,50 L150,50" fill="none" stroke="#CBD5E1" strokeWidth="2" />
          </g>
        );
      default:
        return <rect x="0" y="0" width="180" height="60" rx="4" fill="#FFF" stroke="#000" />;
    }
  };

  return (
    <div
      className={`relative w-full h-full overflow-hidden bg-slate-50 text-slate-900 ${className}`}
      style={{
        backgroundImage: 'linear-gradient(#e2e8f0 1px, transparent 1px), linear-gradient(90deg, #e2e8f0 1px, transparent 1px)',
        backgroundSize: '20px 20px'
      }}
    >
      {/* Controls */}
      <div className="absolute top-4 right-4 flex gap-2 z-50">
        <button
          onClick={() => setShowHistory(!showHistory)}
          className={`p-2 rounded shadow text-xs font-medium transition-colors border ${showHistory
              ? 'bg-blue-100 text-blue-700 border-blue-200'
              : 'bg-white text-slate-700 border-slate-200 hover:bg-slate-50'
            }`}
        >
          {showHistory ? 'Hide History' : 'Show History'}
        </button>
        <div className="w-px h-8 bg-slate-300 mx-1" />
        <button onClick={expandAll} className="p-2 bg-white text-slate-700 border border-slate-200 rounded shadow hover:bg-slate-50 text-xs font-medium">Expand All</button>
        <button onClick={collapseAll} className="p-2 bg-white text-slate-700 border border-slate-200 rounded shadow hover:bg-slate-50 text-xs font-medium">Collapse All</button>
        <div className="w-px h-8 bg-slate-300 mx-1" />
        <button onClick={() => setTransform(t => ({ ...t, scale: t.scale * 1.2 }))} className="p-2 bg-white text-slate-700 border border-slate-200 rounded shadow hover:bg-slate-50"><ZoomIn size={16} /></button>
        <button onClick={() => setTransform(t => ({ ...t, scale: t.scale / 1.2 }))} className="p-2 bg-white text-slate-700 border border-slate-200 rounded shadow hover:bg-slate-50"><ZoomOut size={16} /></button>
      </div>

      <svg 
        className="w-full h-full cursor-grab active:cursor-grabbing"
        onWheel={handleWheel}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
      >
        <g transform={`translate(${transform.x}, ${transform.y}) scale(${transform.scale})`}>
          {/* Edges */}
          {layoutNodes.map(node => {
            if (!node.children || !expandedIds.has(node.id)) return null;
            return node.children.map(child => {
              // Find child in layoutNodes (it might not be there if not visible/layouted yet, but layout logic handles it)
              // Actually layoutNodes only contains visible nodes.
              // We need to find the child node object that was computed in layoutNodes
              const childNode = layoutNodes.find(n => n.id === child.id);
              if (!childNode || !node.x || !node.y || !childNode.x || !childNode.y) return null;

              return (
                <path
                  key={`${node.id}-${child.id}`}
                  d={`M${node.x},${node.y + NODE_HEIGHT} C${node.x},${node.y + NODE_HEIGHT + 50} ${childNode.x},${childNode.y - 50} ${childNode.x},${childNode.y}`}
                  fill="none"
                  stroke="#CBD5E1"
                  strokeWidth="2"
                />
              );
            });
          })}

          {/* Nodes */}
          {layoutNodes.map(node => (
            <g 
              key={node.id} 
              transform={`translate(${node.x! - NODE_WIDTH/2}, ${node.y})`}
              onClick={(e) => { e.stopPropagation(); toggleNode(node.id); }}
              className="cursor-pointer hover:opacity-90 transition-opacity"
            >
              {renderNodeShape(node)}
              
              {/* Label */}
              <foreignObject x="0" y="0" width={NODE_WIDTH} height={NODE_HEIGHT}>
                <div className="w-full h-full flex flex-col items-center justify-center text-center p-2 pointer-events-none">
                  <div className="text-xs font-bold text-slate-700 truncate w-full">{node.label}</div>
                  {node.subLabel && <div className="text-[10px] text-slate-500 truncate w-full">{node.subLabel}</div>}
                </div>
              </foreignObject>

              {/* Expand/Collapse Indicator */}
              {node.children && node.children.length > 0 && (
                <circle 
                  cx={NODE_WIDTH/2} 
                  cy={NODE_HEIGHT} 
                  r="8" 
                  fill="white" 
                  stroke="#94A3B8" 
                  strokeWidth="1"
                />
              )}
              {node.children && node.children.length > 0 && (
                <text 
                  x={NODE_WIDTH/2} 
                  y={NODE_HEIGHT + 3} 
                  textAnchor="middle" 
                  fontSize="10" 
                  fill="#64748B"
                  className="pointer-events-none"
                >
                  {expandedIds.has(node.id) ? '-' : '+'}
                </text>
              )}
            </g>
          ))}
        </g>
      </svg>
    </div>
  );
}
