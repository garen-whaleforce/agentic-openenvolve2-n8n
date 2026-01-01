import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Earnings Call 分析器',
  description: 'AI 驅動的財報電話會議分析工具，預測 T+30 趨勢',
  icons: {
    icon: '/favicon.ico',
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="zh-TW">
      <body>
        <div className="min-h-screen bg-gray-50">
          {/* Header */}
          <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
            <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
              <div className="flex items-center justify-between h-16">
                <div className="flex items-center gap-3">
                  <span className="text-2xl">📈</span>
                  <div>
                    <h1 className="text-lg font-bold text-gray-900">
                      Earnings Call 分析器
                    </h1>
                    <p className="text-xs text-gray-500 hidden sm:block">
                      AI 驅動 · T+30 趨勢預測 · 86%+ 勝率策略
                    </p>
                  </div>
                </div>
                <div className="text-xs text-gray-400">
                  v1.1-live-safe
                </div>
              </div>
            </div>
          </header>

          {/* Main Content */}
          <main>{children}</main>

          {/* Footer */}
          <footer className="bg-white border-t border-gray-200 mt-auto">
            <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
              <p className="text-center text-xs text-gray-400">
                © 2024 Agentic RAG Earnings Analyzer · 本系統僅供研究參考，非投資建議
              </p>
            </div>
          </footer>
        </div>
      </body>
    </html>
  );
}
