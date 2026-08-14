# LingxiGraph documentation

This directory contains the bilingual LingxiGraph documentation site powered by Next.js and Fumadocs. The site is statically exported and deployed to Cloudflare Pages by GitHub Actions.

<nav aria-label="Language navigation">
  <a href="content/docs/zh/index.mdx">中文文档源码</a> ·
  <a href="content/docs/en/index.mdx">English documentation source</a> ·
  <a href="../README.md">中文 README</a> ·
  <a href="../README.en.md">English README</a>
</nav>

The Chinese and English trees mirror each other by relative path. The published site keeps the same relationship through its language switcher: `/docs/zh/` and `/docs/en/` lead to the corresponding documentation sets.

## Local development

```bash
cd Wiki
npm install
npm run dev
```

Open the Chinese site at `http://localhost:3000/docs/zh/` or the English site at `http://localhost:3000/docs/en/`.

Run the full validation and static build before opening a pull request:

```bash
npm run check
npm run build
```

To inspect the generated Pages output with Wrangler:

```bash
npx wrangler pages dev
```

## Cloudflare Pages deployment

The production Pages project is named `lingxigraph-docs`, with `Wiki/out/` as its generated site directory. The default production URL is [lingxigraph-docs.pages.dev](https://lingxigraph-docs.pages.dev/).

Create the Pages project once, using the `main` branch as its production branch:

```bash
npx wrangler pages project create lingxigraph-docs --production-branch main
```

In the GitHub repository settings, add these Actions secrets:

- `CLOUDFLARE_API_TOKEN` — an account token with permission to deploy Pages.
- `CLOUDFLARE_ACCOUNT_ID` — the Cloudflare account ID containing the Pages project.

Pull requests run checks only. A push to `main` or a manual `workflow_dispatch` run builds the static site and deploys it with `cloudflare/wrangler-action@v3`. Cloudflare's built-in Git build is not enabled, so the repository has one build path.

## Structure

- `content/docs/zh/` — Simplified Chinese MDX source.
- `content/docs/en/` — English MDX source.
- `content/docs/*/meta.json` — ordered Fumadocs navigation trees.
- `app/` — Next.js routes, layouts, static export, and search endpoint.
- `components/` — MDX and browser search components.
- `lib/` — source loader, locale configuration, and shared layout options.
- `public/` — favicon, redirects, and response headers.
- `source.config.ts` — Fumadocs MDX collection configuration.

Each user-facing page has a matching path in both languages. When behavior changes, update the two pages in the same pull request and verify code examples against the current public API.

## Writing conventions

- Write for a concrete task and state prerequisites before commands.
- Use complete, copyable examples; never use production secrets.
- Mark development-only authentication and destructive operations explicitly.
- Link to the canonical page rather than duplicating long explanations.
- Keep API field names, lifecycle states, environment variables, and error codes in English.
