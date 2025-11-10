import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Iceberg Explorer',
  description: 'Explore and analyze Apache Iceberg tables from GCS',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

