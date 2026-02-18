import type { Metadata } from "next";
import { Barlow_Condensed, DM_Mono } from "next/font/google";
import "./globals.css";

const barlow = Barlow_Condensed({
  subsets: ["latin"],
  weight: ["100", "200", "300", "400", "500", "600", "700", "800", "900"],
  variable: "--font-barlow",
});

const dmMono = DM_Mono({
  subsets: ["latin"],
  weight: ["300", "400", "500"],
  variable: "--font-dm-mono",
});

export const metadata: Metadata = {
  title: "OpenClaw Workspace",
  description: "Agentic AI Data Engineering Platform",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${barlow.variable} ${dmMono.variable} antialiased bg-[#141414] text-[#e3e4d8]`}
      >
        {children}
      </body>
    </html>
  );
}
