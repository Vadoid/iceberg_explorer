import React from 'react'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import TableAnalyzer from '../components/TableAnalyzer'
import axios from 'axios'

// Mock axios
jest.mock('axios')
const mockedAxios = axios as jest.Mocked<typeof axios>

// Mock sub-components
jest.mock('../components/MetadataView', () => () => <div data-testid="metadata-view">Metadata View</div>)
jest.mock('../components/SchemaView', () => () => <div data-testid="schema-view">Schema View</div>)
jest.mock('../components/PartitionView', () => () => <div data-testid="partition-view">Partition View</div>)
jest.mock('../components/StatsView', () => () => <div data-testid="stats-view">Stats View</div>)
jest.mock('../components/SampleDataView', () => () => <div data-testid="sample-data-view">Sample Data View</div>)
jest.mock('../components/SnapshotComparisonView', () => () => <div data-testid="snapshot-comparison-view">Snapshot Comparison View</div>)
jest.mock('../components/IcebergGraphView', () => () => <div data-testid="iceberg-graph-view">Iceberg Graph View</div>)

const mockTableInfo = {
  name: 'test_table',
  path: 'warehouse/test_table',
  bucket: 'test-bucket',
  projectId: 'test-project'
}

const mockMetadata = {
  format_version: 2,
  table_uuid: 'uuid',
  location: 'gs://bucket/table',
  last_updated_ms: 1600000000000,
  last_column_id: 1,
  schemas: [],
  current_schema_id: 0,
  partition_specs: [],
  default_spec_id: 0,
  last_partition_id: 1000,
  properties: {},
  current_snapshot_id: -1,
  snapshots: [],
  snapshot_log: [],
  metadata_log: [],
  sort_orders: [],
  default_sort_order_id: 0,
  refs: {}
}

describe('TableAnalyzer', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    mockedAxios.get.mockResolvedValue({ data: mockMetadata })
  })

  it('renders loading state initially', async () => {
    // Delay resolution to catch loading state
    mockedAxios.get.mockImplementation(() => new Promise(resolve => setTimeout(() => resolve({ data: mockMetadata }), 100)))

    render(<TableAnalyzer tableInfo={mockTableInfo} />)

    // Check if refresh button is disabled (loading state)
    const refreshBtn = screen.getByTitle('Refresh metadata')
    expect(refreshBtn).toBeDisabled()

    // Wait for loading to finish
    await waitFor(() => expect(screen.getByTestId('metadata-view')).toBeInTheDocument())
  })

  it('renders overview tab by default', async () => {
    render(<TableAnalyzer tableInfo={mockTableInfo} />)
    await waitFor(() => expect(screen.getByTestId('metadata-view')).toBeInTheDocument())
    expect(screen.getByText('Overview')).toHaveClass('text-blue-600')
  })

  it('switches tabs correctly', async () => {
    render(<TableAnalyzer tableInfo={mockTableInfo} />)
    await waitFor(() => expect(screen.getByTestId('metadata-view')).toBeInTheDocument())

    // Switch to Schema
    fireEvent.click(screen.getByText('Schema'))
    expect(screen.getByTestId('schema-view')).toBeInTheDocument()
    expect(screen.queryByTestId('metadata-view')).not.toBeInTheDocument()

    // Switch to Graph
    fireEvent.click(screen.getByText('Graph'))
    expect(screen.getByTestId('iceberg-graph-view')).toBeInTheDocument()
  })

  it('handles error state', async () => {
    mockedAxios.get.mockRejectedValue({ response: { data: { detail: 'Failed to load' } } })
    render(<TableAnalyzer tableInfo={mockTableInfo} />)

    await waitFor(() => {
      expect(screen.getByText('Error Loading Table Metadata')).toBeInTheDocument()
      expect(screen.getByText('Failed to load')).toBeInTheDocument()
    })
  })

  it('refreshes data when refresh button is clicked', async () => {
    render(<TableAnalyzer tableInfo={mockTableInfo} />)
    await waitFor(() => expect(screen.getByTestId('metadata-view')).toBeInTheDocument())

    // Clear mock history
    mockedAxios.get.mockClear()
    mockedAxios.get.mockResolvedValue({ data: mockMetadata })

    // Click refresh
    fireEvent.click(screen.getByTitle('Refresh metadata'))

    await waitFor(() => {
      expect(mockedAxios.get).toHaveBeenCalledTimes(1)
    })
  })
})
