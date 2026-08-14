import defaultMdxComponents from 'fumadocs-ui/mdx';
import { Step, Steps } from 'fumadocs-ui/components/steps';
import { Tab, Tabs } from 'fumadocs-ui/components/tabs';
import { Card as FumadocsCard } from 'fumadocs-ui/components/card';
import {
  Braces,
  Code2,
  Download,
  KeyRound,
  Server,
  TriangleAlert,
  Workflow,
  Zap,
} from 'lucide-react';
import type { ComponentProps, ReactNode } from 'react';
import type { MDXComponents } from 'mdx/types';
import { Mermaid } from '@/components/mermaid';

const iconMap = {
  bolt: Zap,
  'brackets-curly': Braces,
  'diagram-project': Workflow,
  download: Download,
  key: KeyRound,
  python: Code2,
  server: Server,
  'triangle-exclamation': TriangleAlert,
} as const;

type CardProps = ComponentProps<typeof FumadocsCard> & {
  icon?: ReactNode | keyof typeof iconMap;
};

function Card({ icon, ...props }: CardProps) {
  const Icon = typeof icon === 'string' ? iconMap[icon as keyof typeof iconMap] : undefined;
  return <FumadocsCard {...props} icon={Icon ? <Icon aria-hidden="true" /> : icon} />;
}

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    Card,
    Step,
    Steps,
    Tab,
    Tabs,
    Mermaid,
    ...components,
  } satisfies MDXComponents;
}

export const useMDXComponents = getMDXComponents;

declare global {
  type MDXProvidedComponents = ReturnType<typeof getMDXComponents>;
}
