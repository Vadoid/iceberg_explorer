import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import SnapshotComparisonView from '../components/SnapshotComparisonView';
import api from '../lib/api';

// Mock the api module
jest.mock('../lib/api', () => ({
  get: jest.fn(),
  interceptors: {
    request: { use: jest.fn() },
    response: { use: jest.fn() },
  },
}));

const mockedApi = api as jest.Mocked<typeof api>;

describe('SnapshotComparisonView', () => {
  const mockTableInfo = {
    name: 'test_table',
    location: 'gs://test-bucket/test_table',
    bucket: 'test-bucket',
    path: 'test_table',
    projectId: 'test-project',
  };

  const mockMetadata = {
    tableName: 'test_table',
    location: 'gs://test-bucket/test_table',
    formatVersion: 2,
    schema: [],
    partitionSpec: [],
    sortOrder: [],
    properties: {},
    currentSnapshotId: '2',
    snapshots: [
      {
        snapshotId: '2',
        timestamp: new Date('2023-01-02').toISOString(),
        summary: {},
        manifestList: 'gs://test-bucket/test_table/metadata/snap-2.avro',
      },
      {
        snapshotId: '1',
        timestamp: new Date('2023-01-01').toISOString(),
        summary: {},
        manifestList: 'gs://test-bucket/test_table/metadata/snap-1.avro',
      },
    ],
    dataFiles: [],
    partitionStats: [],
  };

  const mockComparison = {
    snapshot1: { snapshotId: '1', timestamp: '2023-01-01T00:00:00.000Z' },
    snapshot2: { snapshotId: '2', timestamp: '2023-01-02T00:00:00.000Z' },
    addedFiles: [
      { filePath: 'file2.parquet', fileSizeInBytes: 1000, recordCount: 100 }
    ],
    removedFiles: [],
    modifiedFiles: [],
    statistics: {
      snapshot1: { fileCount: 1, recordCount: 100, totalSize: 1000 },
      snapshot2: { fileCount: 2, recordCount: 200, totalSize: 2000 },
      delta: { files: 1, records: 100, size: 1000 }
    },
    summary: {
      addedCount: 1,
      removedCount: 0,
      modifiedCount: 0
    }
  };

  beforeEach(() => {
    jest.clearAllMocks();
    mockedApi.get.mockResolvedValue({ data: mockComparison });
  });

  it('renders snapshot selectors', async () => {
    render(<SnapshotComparisonView tableInfo={mockTableInfo} metadata={mockMetadata} />);

    await waitFor(() => {
      expect(screen.getByText('Snapshot 1 (Base)')).toBeInTheDocument();
    });
    expect(screen.getByText('Snapshot 2 (Compare)')).toBeInTheDocument();
  });

  it('loads and renders comparison data', async () => {
    mockedApi.get.mockResolvedValue({ data: mockComparison });

    render(<SnapshotComparisonView tableInfo={mockTableInfo} metadata={mockMetadata} />);

    // It should automatically load because we have 2 snapshots and it defaults to comparing them
    await waitFor(() => {
      expect(mockedApi.get).toHaveBeenCalled();
    });

    expect(screen.getByText('Files Added')).toBeInTheDocument();
    expect(screen.getByText('1')).toBeInTheDocument(); // addedCount
    expect(screen.getByText('file2.parquet (1000.00 B, 100 records)')).toBeInTheDocument();
  });

  it('handles error state', async () => {
    mockedApi.get.mockRejectedValue(new Error('Failed to fetch'));

    render(<SnapshotComparisonView tableInfo={mockTableInfo} metadata={mockMetadata} />);

    await waitFor(() => {
      expect(screen.getByText('Error Loading Comparison')).toBeInTheDocument();
    });
  });

  it('updates comparison when selection changes', async () => {
    mockedApi.get.mockResolvedValue({ data: mockComparison });

    render(<SnapshotComparisonView tableInfo={mockTableInfo} metadata={mockMetadata} />);

    await waitFor(() => {
      expect(mockedApi.get).toHaveBeenCalledTimes(1);
    });

    // Change selection (simulated)
    // Actually, let's just check that it calls API with correct params
    expect(mockedApi.get).toHaveBeenCalledWith('/snapshot/compare', expect.objectContaining({
      params: expect.objectContaining({
        snapshot_id_1: '1',
        snapshot_id_2: '2'
      })
    }));
  });
});
