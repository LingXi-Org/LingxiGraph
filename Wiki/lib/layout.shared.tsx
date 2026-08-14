import { uiTranslations } from 'fumadocs-ui/i18n';
import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { i18n } from '@/lib/i18n';

export const translations = i18n
  .translations()
  .extend(uiTranslations())
  .add({
    zh: {
      displayName: '简体中文',
    },
    en: {
      displayName: 'English',
    },
  });

export function baseOptions(locale: string): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span className="site-brand" aria-label="LingxiGraph">
          <img src="/logo_icon_black.svg" alt="" width={24} height={24} />
          <span>LingxiGraph</span>
        </span>
      ),
      url: `/docs/${locale}/`,
    },
    githubUrl: 'https://github.com/LingXi-Org/LingxiGraph',
    i18n: true,
    themeSwitch: { enabled: false },
  };
}
