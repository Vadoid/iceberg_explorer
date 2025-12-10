import React from 'react'
import { render, screen } from '@testing-library/react'
import AuthProvider from '../components/AuthProvider'

// Mock SessionProvider
jest.mock('next-auth/react', () => ({
  SessionProvider: ({ children }: { children: React.ReactNode }) => <div data-testid="session-provider">{children}</div>
}))

describe('AuthProvider', () => {
  it('renders children wrapped in SessionProvider', () => {
    render(
      <AuthProvider>
        <div data-testid="child">Child Content</div>
      </AuthProvider>
    )

    expect(screen.getByTestId('session-provider')).toBeInTheDocument()
    expect(screen.getByTestId('child')).toBeInTheDocument()
    expect(screen.getByText('Child Content')).toBeInTheDocument()
  })
})
