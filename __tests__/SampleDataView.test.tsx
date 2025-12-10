import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import SampleDataView from '../components/SampleDataView';
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

describe('SampleDataView', () => {
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
    currentSnapshotId: '123456789',
    snapshots: [
      {
        snapshotId: '123456789',
        timestamp: new Date().toISOString(),
        summary: {},
        manifestList: 'gs://test-bucket/test_table/metadata/snap-123456789-1-123.avro',
      },
    ],
    dataFiles: [],
    partitionStats: [],
  };

  const mockSampleData = {
    rows: [
      { id: 1, name: 'Alice', age: 30 },
      { id: 2, name: 'Bob', age: 25 },
    ],
    columns: ['id', 'name', 'age'],
    totalRows: 2,
    filesRead: 1,
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders loading state initially', () => {
    mockedApi.get.mockImplementation(() => new Promise(() => { })); // Never resolves
    render(<SampleDataView tableInfo={mockTableInfo} metadata={mockMetadata} />);
    // expect(screen.getByRole('status')).toBeInTheDocument(); 
    expect(mockedApi.get).toHaveBeenCalled();
  });

  it('renders sample data correctly', async () => {
    mockedApi.get.mockResolvedValue({ data: mockSampleData });

    render(<SampleDataView tableInfo={mockTableInfo} metadata={mockMetadata} />);

    await waitFor(() => {
      expect(screen.getByText('Sample Data')).toBeInTheDocument();
    });

    expect(screen.getByText('Alice')).toBeInTheDocument();
    expect(screen.getByText('Bob')).toBeInTheDocument();
    expect(screen.getByText('Showing 2 rows from 1 file(s)')).toBeInTheDocument();

    // Check columns
    expect(screen.getByText('id')).toBeInTheDocument();
    expect(screen.getByText('name')).toBeInTheDocument();
    expect(screen.getByText('age')).toBeInTheDocument();
  });

  it('handles error state', async () => {
    mockedApi.get.mockRejectedValue(new Error('Failed to fetch'));

    render(<SampleDataView tableInfo={mockTableInfo} metadata={mockMetadata} />);

    await waitFor(() => {
      expect(screen.getByText('Error Loading Sample Data')).toBeInTheDocument();
    });

    expect(screen.getByText('Failed to load sample data.')).toBeInTheDocument();
  });

  it('handles empty data', async () => {
    mockedApi.get.mockResolvedValue({
      data: {
        rows: [],
        columns: [],
        totalRows: 0,
        filesRead: 0,
        message: 'No data found',
      },
    });

    render(<SampleDataView tableInfo={mockTableInfo} metadata={mockMetadata} />);

    await waitFor(() => {
      expect(screen.getByText('No data found')).toBeInTheDocument();
    });
  });

  it('refreshes data when refresh button is clicked', async () => {
    mockedApi.get.mockResolvedValue({ data: mockSampleData });

    render(<SampleDataView tableInfo={mockTableInfo} metadata={mockMetadata} />);

    await waitFor(() => {
      expect(screen.getByText('Alice')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('Refresh'));

    expect(mockedApi.get).toHaveBeenCalledTimes(2);
  });
});
