// Prefix an internal path with the configured base path (e.g. "/topics" when
// the site is served from a GitHub Pages project subpath) and a trailing slash
// so it matches the static-export output (`trailingSlash: true`).
//
// Nextra's own components (sidebar, Cards, markdown links) are already
// base-path aware; this helper is only for the hand-written raw <a> tags in the
// landing page and the footer. `NEXT_PUBLIC_BASE_PATH` is inlined at build time.
const BASE = process.env.NEXT_PUBLIC_BASE_PATH || ''

export function withBase(path: string): string {
  if (!path.startsWith('/')) return path
  if (path === '/') return `${BASE}/`
  const withSlash = path.endsWith('/') ? path : `${path}/`
  return `${BASE}${withSlash}`
}
