import { defineConfig } from 'vitepress'

const description =
  'Durable workflow execution for TypeScript. Multi-step workflows with full type safety, automatic retries, and crash recovery — powered by SQLite, no external services required.'

export default defineConfig({
  title: 'Reflow',
  description,
  lang: 'en-US',
  base: '/reflow-ts/',
  cleanUrls: true,
  lastUpdated: true,
  sitemap: { hostname: 'https://danfry1.github.io/reflow-ts/' },
  head: [
    ['link', { rel: 'icon', href: '/reflow-ts/favicon.svg', type: 'image/svg+xml' }],
    ['meta', { name: 'theme-color', content: '#e46d2f' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:site_name', content: 'Reflow' }],
    ['meta', { property: 'og:image', content: 'https://danfry1.github.io/reflow-ts/og-card.png' }],
    ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
    ['meta', { name: 'twitter:image', content: 'https://danfry1.github.io/reflow-ts/og-card.png' }],
    ['meta', { property: 'og:title', content: 'Reflow — Durable Workflows for TypeScript' }],
    ['meta', { name: 'twitter:title', content: 'Reflow — Durable Workflows for TypeScript' }],
  ],
  themeConfig: {
    search: { provider: 'local' },
    socialLinks: [{ icon: 'github', link: 'https://github.com/danfry1/reflow-ts' }],
    editLink: {
      pattern: 'https://github.com/danfry1/reflow-ts/edit/main/website/:path',
      text: 'Edit this page on GitHub',
    },
    nav: [
      { text: 'Guide', link: '/guide/' },
      { text: 'API', link: '/api/create-workflow' },
      {
        text: 'Resources',
        items: [
          { text: 'Changelog', link: 'https://github.com/danfry1/reflow-ts/blob/main/CHANGELOG.md' },
          { text: 'npm', link: 'https://www.npmjs.com/package/reflow-ts' },
          { text: 'llms.txt', link: '/llms.txt', target: '_blank' },
        ],
      },
    ],
    sidebar: {
      '/': [
        {
          text: 'Introduction',
          items: [
            { text: 'What is Reflow', link: '/guide/' },
            { text: 'Installation', link: '/guide/install' },
            { text: 'Quick Start', link: '/guide/quick-start' },
          ],
        },
        {
          text: 'Core Concepts',
          items: [
            { text: 'Workflows', link: '/guide/workflows' },
            { text: 'The Engine', link: '/guide/engine' },
            { text: 'Retry & Timeouts', link: '/guide/retry' },
            { text: 'Failure Handling', link: '/guide/failure-handling' },
            { text: 'Parallel Steps', link: '/guide/parallel' },
            { text: 'Early Completion', link: '/guide/early-completion' },
            { text: 'Durable Sleep', link: '/guide/sleep' },
            { text: 'Hooks', link: '/guide/hooks' },
            { text: 'Streaming Results', link: '/guide/streaming' },
            { text: 'Cancellation', link: '/guide/cancellation' },
            { text: 'Scheduled Workflows', link: '/guide/scheduling' },
            { text: 'Concurrency', link: '/guide/concurrency' },
            { text: 'Crash Recovery', link: '/guide/crash-recovery' },
            { text: 'Storage', link: '/guide/storage' },
          ],
        },
        {
          text: 'Guides',
          items: [
            { text: 'Testing', link: '/guide/testing' },
            { text: 'Type Safety', link: '/guide/type-safety' },
            { text: 'Error Handling', link: '/guide/error-handling' },
          ],
        },
        {
          text: 'API Reference',
          items: [
            { text: 'createWorkflow', link: '/api/create-workflow' },
            { text: 'Workflow methods', link: '/api/workflow' },
            { text: 'createEngine', link: '/api/create-engine' },
            { text: 'Engine methods', link: '/api/engine' },
            { text: 'Events & Streams', link: '/api/events' },
            { text: 'Storage', link: '/api/storage' },
            { text: 'Errors', link: '/api/errors' },
            { text: 'Types', link: '/api/types' },
          ],
        },
      ],
    },
    outline: { level: [2, 3] },
    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2026 Daniel Fry',
    },
  },
})
