# Earnings Call Notifier

每日 Earnings Call 分析推播服務，整合 LINE Messaging API。

## 功能特色

- ⏰ **每日排程**：美東時間 06:00 自動執行
- 📊 **自動分析**：呼叫後端 API 分析最新 Earnings Calls
- 📱 **LINE 推播**：分析結果即時推送到指定群組/用戶
- 🛠️ **管理介面**：HTTP API + CLI 工具

## 技術棧

- **Runtime**: Node.js 20
- **語言**: TypeScript
- **框架**: Express
- **排程**: node-cron
- **HTTP Client**: Axios
- **驗證**: Zod
- **日誌**: Pino

## 專案結構

```
notifier/
├── src/
│   ├── config.ts       # 環境變數設定與驗證
│   ├── logger.ts       # 日誌模組
│   ├── types.ts        # TypeScript 型別定義
│   ├── analysisApi.ts  # 後端 API 客戶端
│   ├── line.ts         # LINE Messaging API 封裝
│   ├── runner.ts       # 每日掃描主流程
│   ├── server.ts       # Express 伺服器
│   ├── cron.ts         # 排程模組
│   ├── index.ts        # 應用程式入口
│   └── cli.ts          # CLI 工具
├── .env.example        # 環境變數範例
├── package.json
├── tsconfig.json
├── Dockerfile
└── README.md
```

## 快速開始

### 1. 安裝依賴

```bash
cd notifier
npm install
```

### 2. 設定環境變數

```bash
cp .env.example .env
```

編輯 `.env`：

```env
# 必填
ANALYSIS_API_BASE=https://your-api.zeabur.app
LINE_CHANNEL_ACCESS_TOKEN=your_channel_access_token
LINE_TO=U1234567890abcdef...
ADMIN_TOKEN=your_secure_admin_token

# 可選（有預設值）
MIN_MARKET_CAP=1000000000
MAX_SYMBOLS=15
LOOKBACK_DAYS=7
CONF_THRESHOLD=0.65
REQUEST_DELAY_MS=300
PORT=3000
LOG_LEVEL=info
```

### 3. 開發模式

```bash
npm run dev
```

### 4. 生產建置

```bash
npm run build
npm start
```

## 測試 LINE 推播

### 方法一：CLI

```bash
npm run test:line
```

### 方法二：HTTP API

```bash
curl -X POST http://localhost:3000/admin/test-line \
  -H "Content-Type: application/json" \
  -H "x-admin-token: your_admin_token" \
  -d '{"text": "自訂測試訊息"}'
```

### 方法三：自訂訊息

```bash
curl -X POST http://localhost:3000/admin/test-line \
  -H "Content-Type: application/json" \
  -H "x-admin-token: your_admin_token"
```

## 手動觸發掃描

### 方法一：CLI

```bash
npm run run:once
```

### 方法二：HTTP API

```bash
curl -X POST http://localhost:3000/admin/run-scan \
  -H "x-admin-token: your_admin_token"
```

## API 端點

| 方法 | 路徑 | 說明 | 認證 |
|------|------|------|------|
| GET | `/healthz` | 健康檢查 | 無 |
| POST | `/admin/test-line` | 測試 LINE 推播 | x-admin-token |
| POST | `/admin/run-scan` | 手動觸發掃描 | x-admin-token |
| GET | `/admin/status` | 取得服務狀態 | x-admin-token |

## Zeabur 部署

### 1. 建立新服務

- 選擇「Git Repository」
- 設定 Root Directory 為 `notifier`

### 2. 設定環境變數

在 Zeabur 控制台設定：

```
ANALYSIS_API_BASE=https://your-api.zeabur.app
LINE_CHANNEL_ACCESS_TOKEN=xxx
LINE_TO=xxx
ADMIN_TOKEN=xxx
PORT=3000
```

### 3. 啟動命令

Zeabur 會自動偵測 `npm start`。

或手動設定：

```bash
npm run build && npm start
```

### 4. 驗證部署

```bash
# 健康檢查
curl https://your-notifier.zeabur.app/healthz

# 測試 LINE
curl -X POST https://your-notifier.zeabur.app/admin/test-line \
  -H "x-admin-token: your_admin_token"
```

## Docker 部署

### 建置映像

```bash
docker build -t earnings-notifier .
```

### 執行容器

```bash
docker run -d \
  --name earnings-notifier \
  -p 3000:3000 \
  -e ANALYSIS_API_BASE=https://your-api.zeabur.app \
  -e LINE_CHANNEL_ACCESS_TOKEN=xxx \
  -e LINE_TO=xxx \
  -e ADMIN_TOKEN=xxx \
  earnings-notifier
```

## 排程說明

- **時間**：每天美東時間 06:00
- **Cron 表達式**：`0 6 * * *`
- **時區**：America/New_York

### 排程流程

1. 計算日期範圍（昨天往前 LOOKBACK_DAYS 天）
2. 取得 Earnings Calendar
3. 找出最新有資料的日期
4. 取得該日期前 MAX_SYMBOLS 檔（依市值排序）
5. 逐檔呼叫分析 API
6. 推播清單訊息 + 分析結果

## LINE 訊息格式

### 清單訊息

```
📅 Earnings Call 清單

美東時間：2025-01-31 06:00:00
目標日期：2025-01-30
符合條件：15 檔

Tickers：AAPL, MSFT, GOOGL, ...

即將分析前 15 檔...
```

### 結果訊息

```
📊 Earnings Call 分析結果

目標日期：2025-01-30
分析時間：2025-01-31 06:05:00
分析檔數：15

✅ BUY：5
⚪ NO ACTION：8
⏳ PENDING：2
❌ ERROR：0

━━━━━━━━━━━━━━━━
✅ BUY 建議清單
━━━━━━━━━━━━━━━━

📈 AAPL (78%) [D8]
Apple Inc.
• 營收成長超預期...
• iPhone 銷售強勁...

📈 MSFT (82%) [D9]
Microsoft Corporation
• 雲端業務持續成長...
• AI 投資回報顯現...
```

## 環境變數說明

| 變數 | 必填 | 預設值 | 說明 |
|------|------|--------|------|
| ANALYSIS_API_BASE | ✅ | - | 後端 API URL |
| LINE_CHANNEL_ACCESS_TOKEN | ✅ | - | LINE Channel Token |
| LINE_TO | ✅ | - | 推播目標 (userId/groupId) |
| ADMIN_TOKEN | ✅ | - | Admin API 認證 Token |
| MIN_MARKET_CAP | ❌ | 1000000000 | 最小市值門檻 (10億美元) |
| MAX_SYMBOLS | ❌ | 15 | 每日最多分析股數 |
| LOOKBACK_DAYS | ❌ | 7 | 往前查找天數 |
| CONF_THRESHOLD | ❌ | 0.65 | 信心度門檻 |
| REQUEST_DELAY_MS | ❌ | 300 | API 請求間隔 |
| PORT | ❌ | 3000 | 伺服器埠號 |
| LOG_LEVEL | ❌ | info | 日誌等級 |

## 取得 LINE Channel Access Token

1. 前往 [LINE Developers Console](https://developers.line.biz/console/)
2. 建立 Provider（如果沒有）
3. 建立 Messaging API Channel
4. 在 Channel 設定中找到「Channel access token」
5. 點擊「Issue」產生 Token

## 取得 LINE User ID / Group ID

### User ID
- 在 LINE Official Account Manager 查看
- 或透過 Webhook 事件取得

### Group ID
- 需要透過 Webhook 事件取得
- 當 Bot 被加入群組時會收到事件

## 故障排除

### LINE 推播失敗

1. 確認 `LINE_CHANNEL_ACCESS_TOKEN` 正確
2. 確認 `LINE_TO` 格式正確（U 開頭為 userId，C 開頭為 groupId）
3. 確認 Bot 已加入目標群組

### 分析 API 錯誤

1. 確認 `ANALYSIS_API_BASE` 正確
2. 確認後端服務正常運行
3. 檢查 API rate limit

### 排程沒有執行

1. 確認服務持續運行
2. 確認時區設定正確（America/New_York）
3. 檢查日誌

## License

MIT
