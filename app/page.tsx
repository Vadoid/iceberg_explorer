'use client';

import { useState } from 'react';
import BucketBrowser from '@/components/BucketBrowser';
import TableAnalyzer from '@/components/TableAnalyzer';
import { TableInfo } from '@/types';

export default function Home() {
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [bucketName, setBucketName] = useState<string>('');
  const [folderPath, setFolderPath] = useState<string>('');

  return (
    <main className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 dark:from-gray-900 dark:to-gray-800">
      <div className="container mx-auto px-4 py-8">
        <div className="mb-8 text-center">
          <h1 className="text-4xl font-bold text-gray-900 dark:text-white mb-2">
            🧊 Iceberg Explorer
          </h1>
          <p className="text-gray-600 dark:text-gray-300">
            Explore and analyze Apache Iceberg tables from Google Cloud Storage
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-semibold mb-4 text-gray-800 dark:text-white">
              GCS Bucket Browser
            </h2>
            <BucketBrowser
              onTableSelect={setSelectedTable}
              onBucketChange={setBucketName}
              onFolderChange={setFolderPath}
            />
          </div>

          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
            <h2 className="text-2xl font-semibold mb-4 text-gray-800 dark:text-white">
              Table Analysis
            </h2>
            {selectedTable ? (
              <TableAnalyzer tableInfo={selectedTable} />
            ) : (
              <div className="text-center py-12 text-gray-500 dark:text-gray-400">
                <p>Select a table from the bucket browser to view analysis</p>
              </div>
            )}
          </div>
        </div>
      </div>
    </main>
  );
}

