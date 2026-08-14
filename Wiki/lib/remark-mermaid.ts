type MarkdownNode = {
  type?: string;
  lang?: string | null;
  value?: string;
  children?: MarkdownNode[];
};

function walk(node: MarkdownNode) {
  if (!node.children) return;

  node.children = node.children.map((child) => {
    if (child.type === 'code' && child.lang === 'mermaid') {
      return {
        type: 'mdxJsxFlowElement',
        name: 'Mermaid',
        attributes: [
          {
            type: 'mdxJsxAttribute',
            name: 'chart',
            value: child.value ?? '',
          },
        ],
        children: [],
      } as MarkdownNode;
    }

    walk(child);
    return child;
  });
}

export function remarkMermaid() {
  return (tree: MarkdownNode) => walk(tree);
}
