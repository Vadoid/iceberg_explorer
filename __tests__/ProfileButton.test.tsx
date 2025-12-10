import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import ProfileButton from '@/components/ProfileButton';
import { useSession, signOut } from 'next-auth/react';

// Mock next-auth/react
jest.mock('next-auth/react', () => ({
  useSession: jest.fn(),
  signOut: jest.fn(),
}));

describe('ProfileButton', () => {
  const mockUseSession = useSession as jest.Mock;

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders nothing when not authenticated', () => {
    mockUseSession.mockReturnValue({ data: null, status: 'unauthenticated' });
    const { container } = render(<ProfileButton />);
    expect(container).toBeEmptyDOMElement();
  });

  it('renders user avatar when authenticated', () => {
    mockUseSession.mockReturnValue({
      data: {
        user: {
          name: 'Test User',
          email: 'test@example.com',
          image: 'https://example.com/avatar.jpg',
        },
      },
      status: 'authenticated',
    });

    render(<ProfileButton />);

    const avatar = screen.getByAltText('Test User');
    expect(avatar).toBeInTheDocument();
    expect(avatar).toHaveAttribute('src', 'https://example.com/avatar.jpg');
  });

  it('renders initials when no avatar image is provided', () => {
    mockUseSession.mockReturnValue({
      data: {
        user: {
          name: 'Test User',
          email: 'test@example.com',
          image: null,
        },
      },
      status: 'authenticated',
    });

    render(<ProfileButton />);

    expect(screen.getByText('TU')).toBeInTheDocument();
  });

  it('toggles dropdown on click', () => {
    mockUseSession.mockReturnValue({
      data: {
        user: {
          name: 'Test User',
          email: 'test@example.com',
        },
      },
      status: 'authenticated',
    });

    render(<ProfileButton />);

    // Dropdown should be closed initially
    expect(screen.queryByText('test@example.com')).not.toBeInTheDocument();

    // Click to open
    fireEvent.click(screen.getByLabelText('User menu'));
    expect(screen.getByText('test@example.com')).toBeInTheDocument();
    expect(screen.getByText('Sign out')).toBeInTheDocument();

    // Click to close
    fireEvent.click(screen.getByLabelText('User menu'));
    expect(screen.queryByText('test@example.com')).not.toBeInTheDocument();
  });

  it('calls signOut when sign out button is clicked', () => {
    mockUseSession.mockReturnValue({
      data: {
        user: {
          name: 'Test User',
          email: 'test@example.com',
        },
      },
      status: 'authenticated',
    });

    render(<ProfileButton />);

    // Open dropdown
    fireEvent.click(screen.getByLabelText('User menu'));

    // Click sign out
    fireEvent.click(screen.getByText('Sign out'));

    expect(signOut).toHaveBeenCalledWith({ callbackUrl: '/login' });
  });

  it('closes dropdown when clicking outside', () => {
    mockUseSession.mockReturnValue({
      data: {
        user: {
          name: 'Test User',
          email: 'test@example.com',
        },
      },
      status: 'authenticated',
    });

    render(
      <div>
        <div data-testid="outside">Outside</div>
        <ProfileButton />
      </div>
    );

    // Open dropdown
    fireEvent.click(screen.getByLabelText('User menu'));
    expect(screen.getByText('test@example.com')).toBeInTheDocument();

    // Click outside
    fireEvent.mouseDown(screen.getByTestId('outside'));

    expect(screen.queryByText('test@example.com')).not.toBeInTheDocument();
  });
});
