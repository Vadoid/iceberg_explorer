'use client';

import { useState } from 'react';
import BucketBrowser from '@/components/BucketBrowser';
import TableAnalyzer from '@/components/TableAnalyzer';
import ProfileButton from '@/components/ProfileButton';
import { TableInfo } from '@/types';

export default function Home() {
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [bucketName, setBucketName] = useState<string>('');
  const [folderPath, setFolderPath] = useState<string>('');
  const [sidebarWidth, setSidebarWidth] = useState(320); // Default 320px
  const [isResizing, setIsResizing] = useState(false);

  const startResizing = (e: React.MouseEvent) => {
    setIsResizing(true);
    e.preventDefault();
  };

  const stopResizing = () => {
    setIsResizing(false);
  };

  const resize = (e: React.MouseEvent) => {
    if (isResizing) {
      const newWidth = e.clientX;
      if (newWidth >= 250 && newWidth <= 600) {
        setSidebarWidth(newWidth);
      }
    }
  };

  return (
    <main
      className="flex h-screen overflow-hidden bg-gray-50 dark:bg-gray-900"
      onMouseMove={resize}
      onMouseUp={stopResizing}
      onMouseLeave={stopResizing}
    >
      {/* Sidebar */}
      <aside
        className="flex-shrink-0 border-r border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 flex flex-col relative"
        style={{ width: sidebarWidth }}
      >
        <div className="p-4 border-b border-gray-200 dark:border-gray-700">
          <h1 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
            <span>🧊</span> Iceberg Explorer
          </h1>
        </div>

        <div className="flex-1 overflow-y-auto p-4">
          <BucketBrowser
            onTableSelect={setSelectedTable}
            onBucketChange={setBucketName}
            onFolderChange={setFolderPath}
          />
        </div>

        {/* Resize Handle */}
        <div
          className="absolute top-0 right-0 w-1 h-full cursor-col-resize hover:bg-blue-500 transition-colors z-10"
          onMouseDown={startResizing}
        />
      </aside>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <header className="bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 p-4 flex justify-between items-center">
          <div className="flex items-center gap-4">
            <h2 className="text-lg font-semibold text-gray-800 dark:text-white">
              {selectedTable ? `Analysis: ${selectedTable.name}` : 'Table Analysis'}
            </h2>
            {selectedTable && (
              <div className="text-sm text-gray-500 dark:text-gray-400">
                {selectedTable.path}
              </div>
            )}
          </div>
          <ProfileButton />
        </header>

        <main className="flex-1 overflow-auto p-6">
          {selectedTable ? (
            <TableAnalyzer tableInfo={selectedTable} />
          ) : (
            <div className="h-full flex flex-col items-center justify-center text-gray-500 dark:text-gray-400">
              <div className="text-6xl mb-4">🧊</div>
              <h3 className="text-xl font-medium mb-2">Select a Table</h3>
              <p>Choose an Iceberg table from the sidebar to begin analysis</p>
            </div>
          )}
        </main>
      </div>
    </main>
  );
}

