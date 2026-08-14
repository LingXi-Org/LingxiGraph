import { readFile, readdir, stat } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';

const root = path.resolve(process.cwd(), 'content/docs');
const locales = ['zh', 'en'];
const expectedPageCount = 24;

async function walk(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const entryPath = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await walk(entryPath)));
    else if (entry.isFile() && entry.name.endsWith('.mdx')) files.push(entryPath);
  }

  return files;
}

function relativePagePath(locale, filePath) {
  return path.relative(path.join(root, locale), filePath).replaceAll(path.sep, '/').replace(/\.mdx$/, '');
}

function pageCandidates(locale, requestedPath) {
  const cleanPath = requestedPath.replace(/^\//, '').replace(/\/$/, '');
  const localeRoot = path.join(root, locale);
  const candidates = [];

  if (!cleanPath) candidates.push(path.join(localeRoot, 'index.mdx'));
  else {
    candidates.push(path.join(localeRoot, cleanPath));
    candidates.push(path.join(localeRoot, `${cleanPath}.mdx`));
    candidates.push(path.join(localeRoot, cleanPath, 'index.mdx'));
  }

  return candidates;
}

async function exists(filePath) {
  try {
    await stat(filePath);
    return true;
  } catch {
    return false;
  }
}

function stripCodeFences(content) {
  return content.replace(/```[\s\S]*?```/g, '');
}

async function checkLinks(locale, files) {
  const errors = [];
  const linkPattern = /\]\(([^)\s]+)(?:\s+[^)]*)?\)|\bhref=["']([^"']+)["']/g;

  for (const filePath of files) {
    const content = stripCodeFences(await readFile(filePath, 'utf8'));
    const sourcePage = relativePagePath(locale, filePath);
    let match;

    while ((match = linkPattern.exec(content)) !== null) {
      const target = (match[1] ?? match[2]).trim();
      if (!target || target.startsWith('#') || /^[a-z][a-z0-9+.-]*:/i.test(target)) continue;

      const [targetPath] = target.split('#', 1);
      let requestedPath;
      if (targetPath.startsWith('/docs/')) {
        const prefix = `/docs/${locale}`;
        if (!targetPath.startsWith(`${prefix}/`) && targetPath !== prefix) continue;
        requestedPath = targetPath.slice(prefix.length).replace(/^\//, '');
      } else if (targetPath.startsWith('/')) {
        continue;
      } else {
        requestedPath = path.posix.normalize(path.posix.join(path.posix.dirname(sourcePage), targetPath));
      }

      if (path.posix.extname(requestedPath) && !requestedPath.endsWith('.mdx')) continue;
      const candidates = pageCandidates(locale, requestedPath);
      if (!(await Promise.all(candidates.map(exists))).some(Boolean)) {
        errors.push(`${locale}/${sourcePage} -> ${target}`);
      }
    }
  }

  return errors;
}

async function readMetaFiles(locale) {
  const localeRoot = path.join(root, locale);
  const entries = [];

  async function visit(directory) {
    const children = await readdir(directory, { withFileTypes: true });
    for (const child of children) {
      const childPath = path.join(directory, child.name);
      if (child.isDirectory()) await visit(childPath);
      else if (child.isFile() && child.name === 'meta.json') {
        const relative = path.relative(localeRoot, childPath).replaceAll(path.sep, '/');
        const value = JSON.parse(await readFile(childPath, 'utf8'));
        entries.push({ relative, value });
      }
    }
  }

  await visit(localeRoot);
  return entries.sort((a, b) => a.relative.localeCompare(b.relative));
}

async function checkMeta(locale, metaFiles) {
  const errors = [];
  const localeRoot = path.join(root, locale);

  for (const { relative, value } of metaFiles) {
    if (!Array.isArray(value.pages)) {
      errors.push(`${locale}/${relative} must contain a pages array`);
      continue;
    }

    const directory = path.dirname(path.join(localeRoot, relative));
    for (const item of value.pages) {
      if (typeof item !== 'string' || item.startsWith('---') || item.startsWith('...')) continue;
      const candidates = pageCandidates(locale, path.relative(localeRoot, path.join(directory, item)).replaceAll(path.sep, '/'));
      if (!(await Promise.all(candidates.map(exists))).some(Boolean)) {
        errors.push(`${locale}/${relative} references missing page ${item}`);
      }
    }
  }

  return errors;
}

const allFiles = {};
const allMeta = {};
const errors = [];

for (const locale of locales) {
  const files = await walk(path.join(root, locale));
  allFiles[locale] = files;
  allMeta[locale] = await readMetaFiles(locale);

  if (files.length !== expectedPageCount) {
    errors.push(`${locale} has ${files.length} MDX pages; expected ${expectedPageCount}`);
  }

  for (const filePath of files) {
    const content = await readFile(filePath, 'utf8');
    if (!/^---\r?\n[\s\S]*?^title\s*:/m.test(content)) {
      errors.push(`${locale}/${relativePagePath(locale, filePath)} is missing frontmatter title`);
    }
  }

  errors.push(...(await checkMeta(locale, allMeta[locale])));
  errors.push(...(await checkLinks(locale, files)));
}

const zhPaths = new Set(allFiles.zh.map((filePath) => relativePagePath('zh', filePath)));
const enPaths = new Set(allFiles.en.map((filePath) => relativePagePath('en', filePath)));
const missingEnglish = [...zhPaths].filter((page) => !enPaths.has(page));
const missingChinese = [...enPaths].filter((page) => !zhPaths.has(page));
if (missingEnglish.length) errors.push(`Missing English pages: ${missingEnglish.join(', ')}`);
if (missingChinese.length) errors.push(`Missing Chinese pages: ${missingChinese.join(', ')}`);

if (JSON.stringify(allMeta.zh.map(({ relative, value }) => [relative, value.pages])) !== JSON.stringify(allMeta.en.map(({ relative, value }) => [relative, value.pages]))) {
  errors.push('Chinese and English navigation trees do not have the same page order');
}

if (errors.length) {
  console.error('Documentation checks failed:');
  for (const error of errors) console.error(`- ${error}`);
  process.exitCode = 1;
} else {
  console.log(`Validated ${expectedPageCount} aligned MDX pages in zh and en, navigation metadata, and internal links.`);
}
