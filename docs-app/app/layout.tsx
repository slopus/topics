import type { ReactNode } from 'react'
import type { Metadata } from 'next'
import { Footer, Layout, Navbar } from 'nextra-theme-docs'
import { Head } from 'nextra/components'
import { getPageMap } from 'nextra/page-map'
import { Logo } from '../components/logo'
import { withBase } from '../components/base'
import './globals.css'
import 'nextra-theme-docs/style.css'

export const metadata: Metadata = {
  title: {
    default: 'topics — a persistent event engine in a single binary',
    template: '%s — topics'
  },
  description:
    'topics is an append-only log service over a clean, JSON-first HTTP API. Topics, routers, multiplexed SSE, lease queues, and per-topic durability — one static binary, one machine. Data loss is always explicit, never silent.'
}

const GITHUB_URL = 'https://github.com/slopus/topics'

export default async function RootLayout({ children }: { children: ReactNode }) {
  const pageMap = await getPageMap()
  const navbar = (
    <Navbar logo={<Logo />} projectLink={GITHUB_URL}>
      <a href={withBase('/api')} className="nav-extra-link">
        /v0 API
      </a>
    </Navbar>
  )
  const footer = (
    <Footer>
      <div className="footer-grid">
        <div className="footer-brand">
          <Logo small />
          <p>
            A persistent event engine in a single binary. Append-only topics, explicit
            loss, durable on local NVMe.
          </p>
        </div>
        <div className="footer-cols">
          <div>
            <span className="footer-head">Product</span>
            <a href={withBase("/getting-started")}>Introduction</a>
            <a href={withBase("/getting-started/quickstart")}>Quickstart</a>
            <a href={withBase("/guarantees")}>Core guarantees</a>
            <a href={withBase("/api")}>API reference</a>
          </div>
          <div>
            <span className="footer-head">Operate</span>
            <a href={withBase("/deployment")}>Deployment</a>
            <a href={withBase("/deployment/configuration")}>Configuration</a>
            <a href={withBase("/deployment/security")}>Security</a>
            <a href={withBase("/how-it-works")}>How it works</a>
          </div>
          <div>
            <span className="footer-head">More</span>
            <a href={withBase("/comparisons")}>Comparisons</a>
            <a href={GITHUB_URL} target="_blank" rel="noreferrer">
              GitHub
            </a>
          </div>
        </div>
      </div>
      <div className="footer-base">
        <span>MIT licensed.</span>
        <span>© {new Date().getFullYear()} the topics authors.</span>
      </div>
    </Footer>
  )

  return (
    <html lang="en" dir="ltr" suppressHydrationWarning>
      <Head
        color={{
          hue: { light: 254, dark: 256 },
          saturation: { light: 70, dark: 85 },
          lightness: { light: 50, dark: 68 }
        }}
        backgroundColor={{ dark: '#0b0c10', light: '#ffffff' }}
      />
      <body>
        <Layout
          navbar={navbar}
          footer={footer}
          pageMap={pageMap}
          docsRepositoryBase={`${GITHUB_URL}/tree/main/docs-app`}
          editLink="Edit this page on GitHub"
          sidebar={{ defaultMenuCollapseLevel: 1, toggleButton: true }}
          toc={{ float: true, backToTop: 'Scroll to top' }}
          darkMode
          nextThemes={{ defaultTheme: 'dark' }}
        >
          {children}
        </Layout>
      </body>
    </html>
  )
}
