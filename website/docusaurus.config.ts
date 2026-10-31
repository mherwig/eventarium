import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'Eventarium',
  tagline:
    'CloudEvents in, filtered and projected per subscription, delivered over webhooks and SSE with guaranteed, tracked delivery',
  favicon: 'img/favicon.ico',

  // Future flags, see https://docusaurus.io/docs/api/docusaurus-config#future
  future: {
    v4: true, // Improve compatibility with the upcoming Docusaurus v4
  },

  // GitHub Pages serves the site at https://<org>.github.io/<project>/
  url: 'https://mherwig.github.io',
  baseUrl: '/eventarium/',
  organizationName: 'mherwig',
  projectName: 'eventarium',

  onBrokenLinks: 'throw',

  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  // The documentation lives next to the code it documents, at docs/ in the
  // repository root - not inside website/. Go comments refer to
  // docs/contracts.md and docs/deviations-and-open-points.md by that path,
  // so keeping the content here rather than moving it under website/ keeps
  // those references true without touching a line of code.
  presets: [
    [
      'classic',
      {
        docs: {
          path: '../docs',
          sidebarPath: './sidebars.ts',
          routeBasePath: 'docs',
          // The docs live at docs/ in the repository root, but Docusaurus
          // computes a page's path relative to the *site* directory, so a
          // string editUrl produces ".../tree/main/../docs/intro.md". A
          // function gets the real absolute path and can strip the prefix.
          editUrl: ({docPath}) =>
            `https://github.com/mherwig/eventarium/tree/main/docs/${docPath}`,
        },
        // No blog: an empty blog would only be a nav item with nothing
        // behind it.
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themes: [
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        // Docs-only index; the site has no blog to search.
        indexDocs: true,
        indexBlog: false,
        indexPages: false,
        docsRouteBasePath: 'docs',
        language: ['en'],
        // Hash-based routing keeps the search index paths stable under the
        // /eventarium/ base path.
        hashed: true,
      },
    ],
  ],

  themeConfig: {
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Eventarium',
      logo: {
        alt: 'Eventarium logo',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          to: '/docs/getting-started',
          label: 'Getting started',
          position: 'left',
        },
        {
          href: 'https://github.com/mherwig/eventarium',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Introduction', to: '/docs/intro'},
            {label: 'Getting started', to: '/docs/getting-started'},
            {label: 'Architecture', to: '/docs/architecture'},
            {label: 'Configuration', to: '/docs/configuration'},
          ],
        },
        {
          title: 'Guides',
          items: [
            {label: 'HTTP API walkthrough', to: '/docs/api'},
            {label: 'Reliability', to: '/docs/reliability'},
            {label: 'Performance', to: '/docs/performance'},
            {label: 'Operating', to: '/docs/operations'},
          ],
        },
        {
          title: 'Reference',
          items: [
            {label: 'Specification', to: '/docs/spec'},
            {label: 'Service contracts', to: '/docs/contracts'},
            {
              label: 'Deviations and open points',
              to: '/docs/deviations-and-open-points',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {label: 'GitHub', href: 'https://github.com/mherwig/eventarium'},
            {
              label: 'Issues',
              href: 'https://github.com/mherwig/eventarium/issues',
            },
          ],
        },
      ],
      copyright: `Eventarium is alpha: written by an AI agent, not yet reviewed or audited. Review before trusting it with production data.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'json', 'yaml', 'sql', 'go'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
