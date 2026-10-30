import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'RutaSur - Torre de Control en Tiempo Real',
  description: 'Plataforma Logística Orientada a Eventos con Kafka, MongoDB Replica Set y Next.js',
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="es" className="dark">
      <body className="antialiased bg-[#090d16] text-slate-100 min-h-screen">
        {children}
      </body>
    </html>
  );
}
