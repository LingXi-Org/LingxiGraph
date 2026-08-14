import { createFromSource } from 'fumadocs-core/search/server';
import { source } from '@/lib/source';

export const revalidate = false;

// Current Fumadocs uses ZBSearch's multilingual tokenizer for static indexes.
// It handles both English and Chinese without loading a large locale-specific
// tokenizer bundle in the browser.
export const { staticGET: GET } = createFromSource(source, {
  language: 'multilingual',
});
