import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import { cookies } from "next/headers";
import { I18nProvider } from "@/app/_components/I18nProvider";
import { getDirection, normalizeLanguage, LANG_COOKIE } from "@/lib/i18n";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "ClipsToCS - CS2 Match Intelligence",
  description:
    "Analyze your CS2 matches automatically. Download replays, track stats, and get AI coaching tips.",
  icons: { icon: "/images/Sans%20titre-1.png" },
  openGraph: {
    title: "ClipsToCS - CS2 Match Intelligence",
    description:
      "Analyze your CS2 matches automatically. Download replays, track stats, and get AI coaching tips.",
    type: "website",
  },
  twitter: {
    card: "summary_large_image",
    title: "ClipsToCS - CS2 Match Intelligence",
    description:
      "Analyze your CS2 matches automatically. Download replays, track stats, and get AI coaching tips.",
  },
};

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const lang = normalizeLanguage((await cookies()).get(LANG_COOKIE)?.value);
  const dir = getDirection(lang);
  return (
    <html lang={lang} dir={dir}>
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        <I18nProvider lang={lang}>{children}</I18nProvider>
      </body>
    </html>
  );
}
