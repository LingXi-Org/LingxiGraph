import type { Metadata } from 'next';
import type { ReactNode } from 'react';
import './globals.css';

export const metadata: Metadata = {
  metadataBase: new URL('https://lingxigraph-docs.pages.dev'),
  title: {
    default: 'LingxiGraph Documentation',
    template: '%s | LingxiGraph',
  },
  description: 'Durable graph runtime for production multi-agent systems.',
  icons: {
    icon: '/favicon.ico',
  },
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="zh-CN" suppressHydrationWarning>
      <body>{children}</body>
    </html>
  );
}
