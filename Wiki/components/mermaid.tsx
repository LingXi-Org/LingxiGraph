'use client';

import mermaid from 'mermaid';
import { useEffect, useId, useRef, useState } from 'react';

export function Mermaid({ chart }: { chart: string }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const generatedId = useId().replace(/:/g, '');
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let active = true;

    mermaid.initialize({
      startOnLoad: false,
      securityLevel: 'strict',
      theme: 'base',
      themeVariables: {
        fontFamily: 'PingFang SC, PingFang TC, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif',
        primaryColor: '#f6f6f6',
        primaryTextColor: '#434343',
        primaryBorderColor: '#d9d9d9',
        lineColor: '#777777',
        secondaryColor: '#fafafa',
        tertiaryColor: '#ffffff',
      },
    });

    void mermaid
      .render(`mermaid-${generatedId}`, chart)
      .then(({ svg }) => {
        if (!active || !containerRef.current) return;
        containerRef.current.innerHTML = svg;
        setFailed(false);
      })
      .catch(() => {
        if (active) setFailed(true);
      });

    return () => {
      active = false;
      if (containerRef.current) containerRef.current.innerHTML = '';
    };
  }, [chart, generatedId]);

  return (
    <figure className="mermaid-diagram" aria-label="Mermaid diagram">
      <div ref={containerRef}>
        {failed ? <pre className="mermaid-fallback">{chart}</pre> : null}
      </div>
    </figure>
  );
}
