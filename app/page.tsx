'use client';

import { useState } from 'react';
import TableAnalyzer from '@/components/TableAnalyzer';
import ProfileButton from '@/components/ProfileButton';
import Sidebar from '@/components/Sidebar';
import { TableInfo } from '@/types';

export default function Home() {
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
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
      <Sidebar
        width={sidebarWidth}
        onTableSelect={setSelectedTable} 
      />

      {/* Resize Handle */}
      <div
        className="w-1 h-full cursor-col-resize hover:bg-blue-500 transition-colors z-10 flex-shrink-0"
        onMouseDown={startResizing}
      />

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

