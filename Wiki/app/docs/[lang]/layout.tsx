import { i18nProvider } from 'fumadocs-ui/i18n';
import { DocsLayout } from 'fumadocs-ui/layouts/docs';
import { RootProvider } from 'fumadocs-ui/provider/next';
import type { ReactNode } from 'react';
import { notFound } from 'next/navigation';
import Search from '@/components/search';
import { baseOptions, translations } from '@/lib/layout.shared';
import { i18n } from '@/lib/i18n';
import { source } from '@/lib/source';

export function generateStaticParams() {
  return i18n.languages.map((lang) => ({ lang }));
}

export default async function DocsLocaleLayout({
  children,
  params,
}: {
  children: ReactNode;
  params: Promise<{ lang: string }>;
}) {
  const { lang } = await params;
  if (!i18n.languages.includes(lang as (typeof i18n.languages)[number])) {
    notFound();
  }

  return (
    <RootProvider
      i18n={i18nProvider(translations, lang)}
      search={{ SearchDialog: Search }}
      theme={{ defaultTheme: 'light', enableSystem: false, forcedTheme: 'light', hotKey: false }}
    >
      <DocsLayout {...baseOptions(lang)} tree={source.getPageTree(lang)}>
        {children}
      </DocsLayout>
    </RootProvider>
  );
}
