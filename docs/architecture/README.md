# Architecture page

[`index.html`](index.html) is an interactive architecture diagram of the
service, made with [Archify](https://github.com/tt-a1i/archify). Open it in a
browser; every box links to the source lines it describes.

[`architecture.json`](architecture.json) is its source. To regenerate the page
after editing it, from the repository root (needs Node 18+ and Chrome or
Chromium):

```sh
git clone --depth 1 https://github.com/tt-a1i/archify /tmp/archify
node /tmp/archify/archify/bin/archify.mjs finalize architecture \
  docs/architecture/architecture.json docs/architecture/index.html \
  --repo-root . --quality showcase
```

The README's images, `architecture-light.png` and `architecture-dark.png`,
are the diagram exported from the page's Export menu (PNG) in each theme;
re-export them after regenerating.

Set `meta.repository.revision` to a pushed commit first: the source links
point at that commit.
