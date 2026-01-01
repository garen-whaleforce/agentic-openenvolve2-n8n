# Earnings Call 分析器 - 前端

AI 驅動的財報電話會議分析工具，整合 Agentic RAG 後端 API，提供 T+30 趨勢預測與買賣建議。

## 功能特色

- 📅 **日期選擇器**：預設為美東時區今天，自動顯示往前 3 天的 Earnings Calls
- 🔍 **智慧搜尋**：支援 Symbol 或公司名稱搜尋、市值排序
- 📊 **即時分析**：點選後即時呼叫後端 AI 分析，顯示預測結果
- 💡 **買賣建議**：基於 86%+ 勝率策略，顯示 BUY / 不處理 建議
- 📱 **RWD 自適應**：桌機左右分欄，手機上下排列

## 技術棧

- **框架**: Next.js 14 (App Router)
- **語言**: TypeScript
- **樣式**: Tailwind CSS
- **資料快取**: SWR + 記憶體快取
- **日期處理**: Luxon (America/New_York 時區)

## 專案結構

```
frontend/
├── src/
│   ├── app/
│   │   ├── api/bff/           # BFF Proxy Route Handlers
│   │   │   ├── analyze/       # POST /api/bff/analyze
│   │   │   └── earnings/
│   │   │       ├── range/     # GET /api/bff/earnings/range
│   │   │       └── today/     # GET /api/bff/earnings/today
│   │   ├── globals.css        # 全域樣式
│   │   ├── layout.tsx         # Root Layout
│   │   └── page.tsx           # 首頁
│   ├── components/
│   │   ├── ui/                # 基礎 UI 元件
│   │   │   ├── Button.tsx
│   │   │   ├── Card.tsx
│   │   │   ├── Badge.tsx
│   │   │   ├── Skeleton.tsx
│   │   │   ├── Input.tsx
│   │   │   └── Select.tsx
│   │   ├── EarningsList.tsx   # Earnings 清單元件
│   │   ├── EarningsItem.tsx   # 單一項目元件
│   │   ├── AnalysisResult.tsx # 分析結果元件
│   │   └── DatePicker.tsx     # 日期選擇器
│   └── lib/
│       ├── api.ts             # API 呼叫封裝 + 快取
│       ├── types.ts           # TypeScript 型別定義
│       ├── constants.ts       # 常數設定
│       └── utils.ts           # 工具函式
├── .env.example               # 環境變數範例
├── package.json
├── tsconfig.json
├── tailwind.config.ts
└── README.md
```

## 快速開始

### 1. 安裝依賴

```bash
cd frontend
npm install
```

### 2. 設定環境變數

```bash
cp .env.example .env
```

編輯 `.env` 檔案：

```env
# 後端 API Base URL（必填）
ANALYSIS_API_BASE=https://your-api.zeabur.app

# 預設最小市值門檻（可選，預設 10 億美元）
DEFAULT_MIN_MARKET_CAP=1000000000
```

### 3. 啟動開發伺服器

```bash
npm run dev
```

開啟 [http://localhost:3000](http://localhost:3000)

### 4. 建置與生產部署

```bash
npm run build
npm run start
```

## Zeabur 部署

### 方法一：透過 Git 連結

1. 將專案推送到 GitHub
2. 在 Zeabur 新增專案，選擇 Git Repository
3. 設定 Root Directory 為 `frontend`
4. 設定環境變數：
   - `ANALYSIS_API_BASE`: 後端 API URL
   - `DEFAULT_MIN_MARKET_CAP`: 市值門檻（可選）

### 方法二：直接部署

1. 在 Zeabur 新增專案
2. 選擇「Deploy from local folder」
3. 上傳 `frontend` 資料夾
4. 設定環境變數

## 可調整參數

所有可調整參數集中在 `src/lib/constants.ts`：

| 參數 | 預設值 | 說明 |
|------|--------|------|
| `LOOKBACK_DAYS` | 3 | 往前查詢天數（含選定日期） |
| `DEFAULT_MIN_MARKET_CAP` | 1000000000 | 預設最小市值門檻（10 億美元） |
| `MAX_REASONS_TO_SHOW` | 3 | 預設顯示的分析理由條數 |
| `ANALYSIS_CACHE_TIME` | 300000 | 分析結果快取時間（5 分鐘） |
| `API_TIMEOUT` | 120000 | API 請求超時時間（120 秒） |

### 修改往前天數

如需修改為「往前 3 天（不含今天）」即 D-3 ~ D-1：

```typescript
// src/lib/constants.ts
export const LOOKBACK_DAYS = 3;

// src/lib/utils.ts - 修改 getDateRangeET
export function getDateRangeET(endDate?: string, days: number = LOOKBACK_DAYS) {
  const end = endDate
    ? DateTime.fromFormat(endDate, DATE_FORMAT, { zone: EASTERN_TIMEZONE })
    : DateTime.now().setZone(EASTERN_TIMEZONE);

  // 修改：從昨天開始往前算
  const actualEnd = end.minus({ days: 1 }); // D-1
  const start = actualEnd.minus({ days: days - 1 }); // D-3

  return {
    startDate: start.toFormat(DATE_FORMAT),
    endDate: actualEnd.toFormat(DATE_FORMAT),
  };
}
```

## API 說明

### BFF Proxy 端點

前端透過 BFF Proxy 與後端溝通，避免 CORS 問題並隱藏後端 URL：

| 前端端點 | 後端端點 | 說明 |
|----------|----------|------|
| `GET /api/bff/earnings/range` | `/api/earnings-calendar/range` | 取得日期區間 Earnings |
| `GET /api/bff/earnings/today` | `/api/earnings-calendar/today` | 取得單日 Earnings |
| `POST /api/bff/analyze` | `/api/analyze` | 執行分析 |

### 分析結果結構

```typescript
{
  symbol: "AAPL",
  transcript_date: "2025-01-31",
  agentic_result: {
    prediction: "UP",        // UP / DOWN / UNKNOWN
    confidence: 0.78,        // 0~1
    summary: "...",          // 分析摘要
    reasons: ["...", "..."], // 分析理由
    trade_long: true,        // 是否建議買入
    long_eligible_json: {
      DirectionScore: 8      // 0~10 方向評分
    }
  }
}
```

## 常見問題

### Q: 分析很慢？

A: 後端 AI 分析需要時間，預設超時為 120 秒。可在 `constants.ts` 調整 `API_TIMEOUT`。

### Q: 顯示「PENDING：尚未取得 Transcript」？

A: 財報電話會議紀錄可能尚未公開。通常在財報發布後數小時至一天內會有紀錄。

### Q: 如何修改時區？

A: 目前固定使用美東時區（America/New_York）。如需修改，請更新 `constants.ts` 中的 `EASTERN_TIMEZONE`。

## License

MIT
