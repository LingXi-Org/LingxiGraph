import { loader } from 'fumadocs-core/source';
import { docs } from '@/.source/server';
import { i18n } from '@/lib/i18n';

export const source = loader({
  baseUrl: '/docs',
  i18n,
  source: docs.toFumadocsSource(),
  url(slugs, locale) {
    return `/${['docs', locale ?? i18n.defaultLanguage, ...slugs].join('/')}`;
  },
});
