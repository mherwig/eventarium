# Eventarium documentation

This site is built with [Docusaurus](https://docusaurus.io/) and published to
GitHub Pages at
[mherwig.github.io/eventarium](https://mherwig.github.io/eventarium).

The markdown lives in [`docs/`](../docs) at the repository root, not here —
Docusaurus reads it via `path: '../docs'` in `docusaurus.config.ts`. That
keeps paths like `docs/contracts.md` true, since Go comments refer to them by
that path.

## Working on it

```sh
npm install
npm run start     # dev server with live reload
npm run build     # static output in build/
npm run serve     # serve the built site locally
```

`npm run build` fails the build on a broken internal link, so a dangling
reference is caught before it is published.

## Deployment

Pushed automatically by [`.github/workflows/docs.yml`](../.github/workflows/docs.yml)
on any change to `docs/` or `website/`. The site is served under the
`/eventarium/` base path, which is why internal links are written as
`/docs/...` rather than absolute URLs.

For the repository first-time setup: **Settings → Pages → Source → GitHub
Actions**. The `npm run deploy` command from the Docusaurus template is not
used here and would push to a `gh-pages` branch that Pages is not configured
to serve.
