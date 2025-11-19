import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Centipawn Loss Analyzer",
  description: "Analyze chess games and explore the relationship between thinking time and move quality",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
