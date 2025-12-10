import React from 'react'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import BucketBrowser from '../components/BucketBrowser'


// Mock api client
jest.mock('../lib/api', () => ({
  get: jest.fn(),
  interceptors: {
    request: { use: jest.fn() },
    response: { use: jest.fn() }
  }
}))
import api from '../lib/api'
const mockedApi = api as jest.Mocked<typeof api>

const mockProjects = {
  projects: [
    { id: 'project-1', name: 'Project 1' },
    { id: 'project-2', name: 'Project 2' }
  ]
}

const mockBuckets = {
  buckets: ['bucket-1', 'bucket-2']
}

const mockFolderContents = {
  folders: ['folder-1'],
  tables: [],
  items: [
    { name: 'folder-1', type: 'folder', path: 'folder-1' },
    { name: 'table-1', type: 'iceberg_table', path: 'table-1', table: { name: 'table-1', path: 'table-1', bucket: 'bucket-1', projectId: 'project-1' } }
  ]
}

describe('BucketBrowser', () => {
  const mockOnTableSelect = jest.fn()
  const mockOnBucketChange = jest.fn()
  const mockOnFolderChange = jest.fn()

  beforeEach(() => {
    jest.clearAllMocks()
    mockedApi.get.mockResolvedValue({ data: {} })
  })

  it('loads and displays projects on mount', async () => {
    mockedApi.get.mockResolvedValueOnce({ data: mockProjects })

    render(
      <BucketBrowser
        onTableSelect={mockOnTableSelect}
        onBucketChange={mockOnBucketChange}
        onFolderChange={mockOnFolderChange}
      />
    )

    await waitFor(() => {
      expect(screen.getByText('Project 1 (project-1)')).toBeInTheDocument()
    })
  })

  it('loads buckets when project is selected', async () => {
    mockedApi.get.mockResolvedValueOnce({ data: mockProjects })
    mockedApi.get.mockResolvedValueOnce({ data: mockBuckets })

    render(
      <BucketBrowser
        onTableSelect={mockOnTableSelect}
        onBucketChange={mockOnBucketChange}
        onFolderChange={mockOnFolderChange}
      />
    )

    // Wait for projects to load
    await waitFor(() => {
      expect(screen.getByText('Project 1 (project-1)')).toBeInTheDocument()
    })

    // Buckets should load automatically for the first project
    await waitFor(() => {
      expect(screen.getByText('bucket-1')).toBeInTheDocument()
      expect(screen.getByText('bucket-2')).toBeInTheDocument()
    })
  })

  it('loads folder contents when bucket is selected', async () => {
    mockedApi.get.mockResolvedValueOnce({ data: mockProjects })
    mockedApi.get.mockResolvedValueOnce({ data: mockBuckets })
    mockedApi.get.mockResolvedValueOnce({ data: mockFolderContents })

    render(
      <BucketBrowser
        onTableSelect={mockOnTableSelect}
        onBucketChange={mockOnBucketChange}
        onFolderChange={mockOnFolderChange}
      />
    )

    // Wait for buckets
    await waitFor(() => {
      expect(screen.getByText('bucket-1')).toBeInTheDocument()
    })

    // Click bucket
    fireEvent.click(screen.getByText('bucket-1'))

    // Should call API and show contents
    await waitFor(() => {
      expect(screen.getByText('folder-1')).toBeInTheDocument()
      expect(screen.getByText('table-1')).toBeInTheDocument()
    })
  })

  it('calls onTableSelect when an Iceberg table is clicked', async () => {
    mockedApi.get.mockResolvedValueOnce({ data: mockProjects })
    mockedApi.get.mockResolvedValueOnce({ data: mockBuckets })
    mockedApi.get.mockResolvedValueOnce({ data: mockFolderContents })

    render(
      <BucketBrowser
        onTableSelect={mockOnTableSelect}
        onBucketChange={mockOnBucketChange}
        onFolderChange={mockOnFolderChange}
      />
    )

    // Wait for buckets and click
    await waitFor(() => expect(screen.getByText('bucket-1')).toBeInTheDocument())
    fireEvent.click(screen.getByText('bucket-1'))

    // Wait for contents and click table
    await waitFor(() => expect(screen.getByText('table-1')).toBeInTheDocument())
    fireEvent.click(screen.getByText('table-1'))

    expect(mockOnTableSelect).toHaveBeenCalledWith(expect.objectContaining({
      name: 'table-1',
      bucket: 'bucket-1'
    }))
  })
})
