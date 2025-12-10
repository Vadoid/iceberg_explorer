import React from 'react'
import { render, screen, fireEvent } from '@testing-library/react'
import IcebergTree from '../components/IcebergTree'

const mockData = {
  tableName: 'test_table',
  location: 'gs://bucket/test_table',
  metadataFiles: [
    {
      file: 'v1.metadata.json',
      version: 1,
      currentSnapshotId: 's1',
      previousMetadataFile: null,
      timestamp: 1600000000000
    }
  ],
  snapshots: [
    {
      snapshotId: 's1',
      timestamp: '2020-09-13T12:26:40.000Z',
      manifestList: 'snap-s1.avro',
      manifests: [],
      summary: {}
    }
  ]
}

describe('IcebergTree', () => {
  it('renders table name', () => {
    render(<IcebergTree data={mockData} />)
    expect(screen.getByText('test_table')).toBeInTheDocument()
  })

  it('renders metadata node', () => {
    render(<IcebergTree data={mockData} />)
    expect(screen.getByText('v1.metadata.json')).toBeInTheDocument()
  })
  it('toggles history mode correctly', () => {
    const historyData = {
      ...mockData,
      metadataFiles: [
        {
          file: 'v2.metadata.json',
          version: 2,
          currentSnapshotId: 's2',
          previousMetadataFile: 'v1.metadata.json',
          timestamp: Date.now()
        },
        {
          file: 'v1.metadata.json',
          version: 1,
          currentSnapshotId: 's1',
          previousMetadataFile: null,
          timestamp: Date.now() - 1000
        }
      ]
    }

    render(<IcebergTree data={historyData} />)

    // Initially should only see v2
    expect(screen.getByText('v2.metadata.json')).toBeInTheDocument()
    expect(screen.queryByText('v1.metadata.json')).not.toBeInTheDocument()

    // Click Show History
    fireEvent.click(screen.getByText('Show History'))

    // Now should see both
    expect(screen.getByText('v2.metadata.json')).toBeInTheDocument()
    expect(screen.getByText('v1.metadata.json')).toBeInTheDocument()
  })
})
