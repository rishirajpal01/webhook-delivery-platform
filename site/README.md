# Landing page

The static page served at <https://webhook-delivery-platform.rishirajpal.com/>.
Plain HTML and CSS with no build step: Cloudflare Pages serves this folder as is.

Cloudflare Pages settings: production branch `main`, framework preset
`None`, build command empty, build output directory `site`, and build watch
path `site/*` so code and docs commits do not redeploy it.

The diagrams are copies of `docs/architecture/architecture-{light,dark}.png`;
copy them again after regenerating the architecture page. To preview locally:
`python3 -m http.server -d site 8000`.
