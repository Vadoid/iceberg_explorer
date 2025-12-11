# Testing Documentation

This document outlines the testing strategy for the Iceberg Explorer application.

## Overview

The application uses a split testing strategy:
- **Backend**: `pytest` for Python/FastAPI code.
- **Frontend**: `Jest` and `React Testing Library` for Next.js/React code.

## Backend Testing

### Framework
- **Runner**: `pytest`
- **Async Support**: `pytest-asyncio`
- **HTTP Client**: `httpx` (AsyncClient)

### Structure
Tests are located in `backend/tests/`.
- `conftest.py`: Shared fixtures (e.g., async client).
- `test_main.py`: General API endpoint tests.
- `test_auth.py`: Authentication logic tests.
- `test_auth_errors.py`: Error handling for auth.

### Running Tests
```bash
# Activate virtual environment
source .venv/bin/activate

# Run all tests
# Note: We add backend to PYTHONPATH to ensure imports work correctly
export PYTHONPATH=$PYTHONPATH:$(pwd)/backend
cd backend
pytest
```

## Frontend Testing

### Framework
- **Runner**: `Jest`
- **Environment**: `jsdom`
- **Utilities**: `React Testing Library` (`@testing-library/react`)

### Structure
Tests are located in `__tests__/`.
- Component tests (e.g., `TableAnalyzer.test.tsx`)
- Page tests (e.g., `LoginPage.test.tsx`)
- Utility tests

### Running Tests
```bash
# Run all tests
npm test

# Run in watch mode
npm test -- --watch
```

## Testing Philosophy

1.  **Integration over Unit**: We prefer integration tests that verify the interaction between components (e.g., `TableAnalyzer` loading metadata) over strict unit tests of internal functions.
2.  **Mocking**:
    *   **Backend**: We mock GCS and BigQuery clients to avoid external dependencies during testing.
    *   **Frontend**: We mock API calls (`axios`, `api` module) and NextAuth hooks (`useSession`).
3.  **User-Centric**: Frontend tests focus on user interactions (clicks, text visibility) rather than implementation details.

## Recent Additions

- **Dev Login**: Tested in `LoginPage.test.tsx` (verifies button presence in dev mode).
- **Loading States**: Tested in `TableAnalyzer.test.tsx` and `IcebergGraphView.test.tsx` (verifies loading spinner appears on data fetch).
- **BigQuery Search**: Backend endpoint tested in `test_bigquery.py` (mocks BigQuery client).
