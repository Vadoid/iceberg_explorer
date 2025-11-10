import { NextResponse } from 'next/server';

const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:8000';

export async function GET() {
  try {
    const response = await fetch(`${BACKEND_URL}/projects`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      // Don't cache this request
      cache: 'no-store',
    });

    if (!response.ok) {
      const errorText = await response.text();
      return NextResponse.json(
        { 
          error: `Backend returned ${response.status}: ${errorText}`,
          detail: `Backend server at ${BACKEND_URL} returned status ${response.status}. Make sure the backend is running on port 8000.`
        },
        { status: response.status }
      );
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    return NextResponse.json(
      {
        error: 'Failed to connect to backend',
        detail: `Could not reach backend at ${BACKEND_URL}. Error: ${errorMessage}. Make sure the backend server is running: cd backend && uvicorn main:app --reload`
      },
      { status: 503 }
    );
  }
}

