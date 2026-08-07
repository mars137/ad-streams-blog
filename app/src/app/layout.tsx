import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Ad-Streams Dashboard",
  description: "Real-time propensity scoring powered by Dynamic Tables",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <nav className="nav">
          <div className="nav-brand">Ad-Streams</div>
          <div className="nav-links">
            <a href="/">Dashboard</a>
            <a href="/campaigns">Campaigns</a>
            <a href="/attribution">Attribution</a>
            <a href="/audiences">Audiences</a>
            <a href="/ai">AI Optimizer</a>
            <a href="/funnel">Funnel</a>
          </div>
        </nav>
        <main className="main">{children}</main>
      </body>
    </html>
  );
}
