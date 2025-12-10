import React from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import MetadataView from '../components/MetadataView'
import SchemaView from '../components/SchemaView'
import PartitionView from '../components/PartitionView'
import StatsView from '../components/StatsView'
import SampleDataView from '../components/SampleDataView'
import SnapshotComparisonView from '../components/SnapshotComparisonView'
import axios from 'axios'

// Mock axios
jest.mock('axios')
const mockedAxios = axios as jest.Mocked<typeof axios>

const mockMetadata = {
  tableName: 'test_table',
  location: 'gs://bucket/table',
  formatVersion: 2,
  schema: [
    { id: 1, name: 'id', type: 'int', required: true },
    { id: 2, name: 'data', type: 'string', required: false }
  ],
  partitionSpec: [
    { fieldId: 1000, sourceId: 1, name: 'id_bucket', transform: 'bucket[16]' }
  ],
  sortOrder: [],
  properties: {
    'write.format.default': 'parquet'
  },
  currentSnapshotId: '1',
  snapshots: [
    {
      snapshotId: '1',
      timestamp: '2020-09-13T12:26:40.000Z',
      manifestList: 'gs://bucket/table/metadata/snap-1.avro',
      summary: { operation: 'append' }
    }
  ],
  dataFiles: [
    {
      filePath: 'gs://bucket/table/data/file.parquet',
      fileFormat: 'parquet',
      partition: {},
      recordCount: 100,
      fileSizeInBytes: 1024
    }
  ],
  partitionStats: []
}

const mockTableInfo = {
  name: 'test_table',
  path: 'warehouse/test_table',
  location: 'gs://test-bucket/warehouse/test_table',
  bucket: 'test-bucket',
  projectId: 'test-project'
}

// Polyfill ResizeObserver for recharts
global.ResizeObserver = class ResizeObserver {
  observe() { }
  unobserve() { }
  disconnect() { }
};

describe('Views', () => {
  describe('MetadataView', () => {
    it('renders metadata details', () => {
      render(<MetadataView metadata={mockMetadata} />)
      expect(screen.getByText('test_table')).toBeInTheDocument()
      expect(screen.getByText('gs://bucket/table')).toBeInTheDocument()
      expect(screen.getByText('parquet')).toBeInTheDocument()
    })
  })

  describe('SchemaView', () => {
    it('renders schema fields', () => {
      render(<SchemaView metadata={mockMetadata} />)
      expect(screen.getByText('id')).toBeInTheDocument()
      expect(screen.getByText('int')).toBeInTheDocument()
      expect(screen.getByText('data')).toBeInTheDocument()
      expect(screen.getByText('string')).toBeInTheDocument()
    })
  })

  describe('PartitionView', () => {
    it('renders partition spec', () => {
      render(<PartitionView metadata={mockMetadata} />)
      expect(screen.getByText('id_bucket')).toBeInTheDocument()
      // bucket[16] is the transform, which is not rendered in the view currently
    })
  })

  describe('StatsView', () => {
    it('renders statistics', () => {
      render(<StatsView metadata={mockMetadata} />)
      // Check for presence of "1" (snapshot count, file count, etc.)
      expect(screen.getAllByText('1').length).toBeGreaterThan(0)
      expect(screen.getByText('Total Files')).toBeInTheDocument()
    })
  })

  describe('SampleDataView', () => {
    it('renders sample data', async () => {
      const mockData = [
        { id: 1, data: 'test' }
      ]
      mockedAxios.get.mockResolvedValue({
        data: {
          rows: mockData,
          columns: ['id', 'data'],
          totalRows: 1,
          filesRead: 1
        }
      })

      render(<SampleDataView tableInfo={mockTableInfo} metadata={mockMetadata} />)

      await waitFor(() => {
        expect(screen.getByText('test')).toBeInTheDocument()
      })
    })
  })

  describe('SnapshotComparisonView', () => {
    it('renders snapshot comparison', async () => {
      // Mock comparison response
      mockedAxios.get.mockResolvedValue({
        data: {
          snapshot1: mockMetadata.snapshots[0],
          snapshot2: mockMetadata.snapshots[0],
          summary: { addedCount: 1, removedCount: 0, modifiedCount: 0 },
          statistics: {
            snapshot1: { fileCount: 0, recordCount: 0, totalSize: 0 },
            snapshot2: { fileCount: 1, recordCount: 100, totalSize: 1024 },
            delta: { files: 1, records: 100, size: 1024 }
          },
          addedFiles: [],
          removedFiles: [],
          modifiedFiles: []
        }
      })

      render(<SnapshotComparisonView tableInfo={mockTableInfo} metadata={mockMetadata} />)

      await waitFor(() => {
        // Check for the option text which contains the snapshot ID "1"
        // The text is "1 - <date>"
        expect(screen.getAllByText(/1 -/)[0]).toBeInTheDocument()
      })
    })
  })
})
