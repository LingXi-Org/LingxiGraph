import Link from 'next/link';

export default function HomePage() {
  return (
    <main className="flex min-h-screen items-center justify-center p-8">
      <div className="text-center">
        <h1 className="text-3xl font-semibold">LingxiGraph</h1>
        <p className="mt-3 text-fd-muted-foreground">
          <Link className="underline" href="/docs/zh/">
            打开中文文档
          </Link>
          {' · '}
          <Link className="underline" href="/docs/en/">
            Open English documentation
          </Link>
        </p>
      </div>
    </main>
  );
}
