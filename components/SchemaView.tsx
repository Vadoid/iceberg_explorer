'use client';

import { TableMetadata } from '@/types';

interface SchemaViewProps {
  metadata: TableMetadata;
}

export default function SchemaView({ metadata }: SchemaViewProps) {
  const renderField = (field: { id: number; name: string; type: string; required: boolean; doc?: string }, level = 0) => {
    const indent = level * 20;
    return (
      <div key={field.id} className="py-2 border-b border-gray-200 dark:border-gray-700" style={{ paddingLeft: `${indent}px` }}>
        <div className="flex items-center gap-3">
          <span className="font-mono text-sm font-semibold text-blue-600 dark:text-blue-400 min-w-[120px]">
            {field.name}
          </span>
          <span className="text-sm text-gray-600 dark:text-gray-400 font-mono">
            {field.type}
          </span>
          {field.required && (
            <span className="text-xs bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400 px-2 py-1 rounded">
              required
            </span>
          )}
          {!field.required && (
            <span className="text-xs bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 px-2 py-1 rounded">
              optional
            </span>
          )}
        </div>
        {field.doc && (
          <p className="text-xs text-gray-500 dark:text-gray-500 mt-1 ml-0" style={{ paddingLeft: `${indent}px` }}>
            {field.doc}
          </p>
        )}
      </div>
    );
  };

  return (
    <div className="space-y-4">
      <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
        <h3 className="font-semibold text-gray-800 dark:text-white mb-4">Schema Fields</h3>
        <div className="bg-white dark:bg-gray-800 rounded-lg overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-100 dark:bg-gray-700">
                <tr>
                  <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 dark:text-gray-400">Field Name</th>
                  <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 dark:text-gray-400">Type</th>
                  <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 dark:text-gray-400">Required</th>
                  <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 dark:text-gray-400">Description</th>
                </tr>
              </thead>
              <tbody>
                {metadata.schema.map((field) => (
                  <tr key={field.id} className="border-b border-gray-200 dark:border-gray-700">
                    <td className="px-4 py-2 font-mono text-sm font-semibold text-blue-600 dark:text-blue-400">
                      {field.name}
                    </td>
                    <td className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 font-mono">
                      {field.type}
                    </td>
                    <td className="px-4 py-2">
                      {field.required ? (
                        <span className="text-xs bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400 px-2 py-1 rounded">
                          required
                        </span>
                      ) : (
                        <span className="text-xs bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 px-2 py-1 rounded">
                          optional
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-2 text-xs text-gray-500 dark:text-gray-500">
                      {field.doc || '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {metadata.partitionSpec.length > 0 && (
        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <h3 className="font-semibold text-gray-800 dark:text-white mb-4">Partition Specification</h3>
          <div className="space-y-2">
            {metadata.partitionSpec.map((spec, idx) => (
              <div
                key={idx}
                className="bg-white dark:bg-gray-800 rounded p-3 flex items-center gap-3"
              >
                <span className="font-mono text-sm font-semibold text-purple-600 dark:text-purple-400">
                  {spec.name}
                </span>
                <span className="text-sm text-gray-600 dark:text-gray-400">
                  {spec.transform}
                </span>
                <span className="text-xs text-gray-500 dark:text-gray-500">
                  (field ID: {spec.fieldId})
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {metadata.sortOrder.length > 0 && (
        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <h3 className="font-semibold text-gray-800 dark:text-white mb-4">Sort Order</h3>
          <div className="space-y-2">
            {metadata.sortOrder.map((order, idx) => (
              <div
                key={idx}
                className="bg-white dark:bg-gray-800 rounded p-3 flex items-center gap-3"
              >
                <span className="text-sm font-semibold text-gray-700 dark:text-gray-300">
                  Order {order.orderId}:
                </span>
                <span className="text-sm text-gray-600 dark:text-gray-400">
                  {order.direction} ({order.nullOrder} nulls)
                </span>
                <span className="text-xs text-gray-500 dark:text-gray-500">
                  (field ID: {order.sortFieldId})
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

