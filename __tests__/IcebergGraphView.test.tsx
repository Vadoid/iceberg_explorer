import React from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import IcebergGraphView from '../components/IcebergGraphView'
import api from '../lib/api'

// Mock api client
jest.mock('../lib/api', () => ({
  get: jest.fn(),
}))
const mockedApi = api as jest.Mocked<typeof api>

// Mock cytoscape since it requires a real DOM with layout capabilities
jest.mock('cytoscape', () => {
  return () => ({
    use: jest.fn(),
    add: jest.fn(),
    layout: jest.fn(() => ({ run: jest.fn() })),
    on: jest.fn(),
    unmount: jest.fn(),
    destroy: jest.fn(),
  })
})

// Mock cytoscape-dagre
jest.mock('cytoscape-dagre', () => ({}))

const mockTableInfo = {
  name: 'test_table',
  path: 'warehouse/test_table',
  location: 'gs://test-bucket/warehouse/test_table',
  bucket: 'test-bucket',
  projectId: 'test-project'
}

const mockGraphData = {
  tableName: 'test_table',
  location: 'gs://test-bucket/warehouse/test_table',
  snapshots: [],
  metadataFiles: []
}

describe('IcebergGraphView', () => {
  it('renders and fetches data', async () => {
    // Setup mock response
    (mockedApi.get as jest.Mock).mockResolvedValue({ data: mockGraphData })

    render(<IcebergGraphView tableInfo={mockTableInfo} />)

    // Should show loading initially
    expect(screen.getByText(/Analyzing table structure/i)).toBeInTheDocument()

    // Wait for data to load
    await waitFor(() => {
      expect(screen.getByText('test_table')).toBeInTheDocument()
    })
  })
})
