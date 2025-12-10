import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Iceberg Explorer',
  description: 'Explore and analyze Apache Iceberg tables from GCS',
};

import AuthProvider from '@/components/AuthProvider';

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <AuthProvider>
          {children}
        </AuthProvider>
      </body>
    </html>
  );
}

