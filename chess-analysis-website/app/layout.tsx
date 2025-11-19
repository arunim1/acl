import type { Metadata } from "next";
import "./globals.css";
import { Providers } from "./providers";

export const metadata: Metadata = {
  title: "ChessInsights - Centipawn Loss Analysis",
  description: "Analyze chess game quality through centipawn loss metrics. Explore massive Lichess databases with powerful visualizations and insights.",
  keywords: ["chess", "analysis", "centipawn loss", "lichess", "data visualization", "chess statistics"],
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className="antialiased">
        <Providers>
          {children}
        </Providers>
      </body>
    </html>
  );
}
