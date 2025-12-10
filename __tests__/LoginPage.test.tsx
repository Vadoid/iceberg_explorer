import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import LoginPage from '@/app/login/page';
import { signIn } from 'next-auth/react';

// Mock next-auth/react
jest.mock('next-auth/react', () => ({
  signIn: jest.fn(),
}));

describe('LoginPage', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders login page correctly', () => {
    render(<LoginPage />);
    
    expect(screen.getByText('Iceberg Explorer')).toBeInTheDocument();
    expect(screen.getByText('Sign in to access your data lake')).toBeInTheDocument();
    expect(screen.getByText('Continue with Google')).toBeInTheDocument();
  });

  it('calls signIn when button is clicked', async () => {
    render(<LoginPage />);
    
    const button = screen.getByText('Continue with Google').closest('button');
    fireEvent.click(button!);
    
    expect(signIn).toHaveBeenCalledWith('google', { callbackUrl: '/' });
  });

  it('shows loading state when button is clicked', async () => {
    // Mock signIn to return a promise that doesn't resolve immediately
    (signIn as jest.Mock).mockImplementation(() => new Promise(() => {}));
    
    render(<LoginPage />);
    
    const button = screen.getByText('Continue with Google').closest('button');
    fireEvent.click(button!);
    
    // Button should be disabled and show loading spinner (or at least not show text)
    expect(button).toBeDisabled();
    expect(screen.queryByText('Continue with Google')).not.toBeInTheDocument();
  });
});
