import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Iceberg Explorer',
  description: 'Explore and analyze Apache Iceberg tables from GCS',
};

import AuthProvider from '@/components/AuthProvider';
import { ThemeProvider } from '@/components/ThemeProvider';

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <AuthProvider>
          <ThemeProvider defaultTheme="system" storageKey="iceberg-explorer-theme">
            {children}
          </ThemeProvider>
        </AuthProvider>
      </body>
    </html>
  );
}

