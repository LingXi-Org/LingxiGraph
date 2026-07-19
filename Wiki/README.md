# LingxiGraph Wiki

This directory is the source for the bilingual LingxiGraph documentation site powered by [Mintlify](https://mintlify.com/).

## Local preview

```bash
cd Wiki
npx mintlify dev
```

The preview is available at `http://localhost:3000`. Run `npx mintlify broken-links` before opening a pull request.

## Structure

- `docs.json` — site theme and bilingual navigation.
- `zh/` — Simplified Chinese documentation.
- `en/` — English documentation.
- `favicon.svg` — shared site mark.

Each user-facing page has a matching path in both languages. When behavior changes, update the two pages in the same pull request and verify code examples against the current public API.

## Writing conventions

- Write for a concrete task and state prerequisites before commands.
- Use complete, copyable examples; never use production secrets.
- Mark development-only authentication and destructive operations explicitly.
- Link to the canonical page rather than duplicating long explanations.
- Keep API field names, lifecycle states, environment variables, and error codes in English.
