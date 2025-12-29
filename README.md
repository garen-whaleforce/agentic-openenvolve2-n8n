# Agentic RAG + Whaleforce Services Integration

財報電話會議 (Earnings Call) 分析與股價預測系統，整合 Whaleforce 內部服務。

## 架構概覽

```
                    ┌─────────────────┐
                    │   LiteLLM       │ ← 統一 LLM 推論
                    └────────┬────────┘
                             │
┌────────────────────────────┼────────────────────────────┐
│            Agentic RAG System                           │
│  ┌─────────┐  ┌─────────────┐  ┌──────────────────┐    │
│  │FMP API  │  │ Main Agent  │  │ Helper Agents    │    │
│  │(財務數據)│→ │ (協調中心)   │←→│ Comparative      │    │
│  └─────────┘  └──────┬──────┘  │ Historical       │    │
│                      │         │ Performance      │    │
│                      │         │ SEC Filings      │←───┼──→ SEC Filings Service
│                      ↓         └──────────────────┘    │
│             ┌────────────────┐                         │
│             │ Neo4j KG       │←──────────────────────────→ Neo4j 172.23.22.100:7687
│             └────────────────┘                         │
└────────────────────────┬───────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        ↓                ↓                ↓
┌──────────────┐  ┌─────────────┐  ┌───────────────┐
│Backtester API│  │Performance  │  │MinIO Storage  │
│(回測驗證)     │  │Metrics      │  │(結果儲存)      │
└──────────────┘  └─────────────┘  └───────────────┘
```

## 整合的 Whaleforce 服務

| 服務 | 端點 | 用途 |
|------|------|------|
| SEC Filings Service | `http://172.23.22.100:8001` | 查詢 10-K, 10-Q, 13F 申報文件 |
| Backtester API | `https://backtest.api.whaleforce.dev` | 股價回測、OHLCV 數據 |
| Performance Metrics | `http://172.23.22.100:8100` | Sharpe Ratio、超額報酬計算 |
| Neo4j Knowledge Graph | `172.23.22.100:7687` | 財報知識圖譜 |
| MinIO Storage | `https://minio.api.whaleforce.dev` | S3 兼容物件儲存 |
| LiteLLM | `https://litellm.whaleforce.dev` | 統一 LLM 推論服務 |

## 快速開始

### 1. 安裝依賴

```bash
pip install -r requirements.txt
```

### 2. 設定環境變數

```bash
cp .env.example .env
# 編輯 .env 填入您的 API keys
```

### 3. 執行服務

```bash
python main.py
```

### 4. 測試服務整合

```bash
python test_services_integration.py
```

## API 端點

### 核心分析 API

| 方法 | 路徑 | 說明 |
|------|------|------|
| POST | `/api/analyze` | 標準財報分析 |
| POST | `/api/analyze-with-services` | 整合 Whaleforce 服務的分析 |

### 服務整合 API

| 方法 | 路徑 | 說明 |
|------|------|------|
| GET | `/api/services/health` | 檢查所有服務健康狀態 |
| GET | `/api/services/sec-filings/search` | 搜尋公司 CIK |
| GET | `/api/services/sec-filings/filings` | 取得 SEC 申報文件 |
| GET | `/api/services/sec-filings/context` | 取得財報分析用 SEC 上下文 |
| GET | `/api/services/performance-metrics` | 計算績效指標 |
| GET | `/api/services/performance-metrics/post-earnings` | 財報後績效指標 |
| GET | `/api/services/backtester/ohlcv` | 取得 OHLCV 數據 |
| GET | `/api/services/backtester/post-earnings-return` | 計算財報後報酬 |
| POST | `/api/services/backtester/validate-prediction` | 驗證預測結果 |

## 使用範例

### 整合分析請求

```bash
curl -X POST "http://localhost:8000/api/analyze-with-services" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "AAPL",
    "year": 2024,
    "quarter": 3,
    "holding_days": 30
  }'
```

### 檢查服務健康狀態

```bash
curl "http://localhost:8000/api/services/health"
```

### 取得績效指標

```bash
curl "http://localhost:8000/api/services/performance-metrics?ticker=AAPL&start_date=2024-01-01&end_date=2024-06-30"
```

## 目錄結構

```
agentic-openenvolve/
├── main.py                     # FastAPI 主服務
├── analysis_engine.py          # 分析引擎 (整合 Whaleforce 服務)
├── agentic_rag_bridge.py       # RAG 橋接層
├── fmp_client.py               # FMP API 客戶端
├── storage.py                  # 資料儲存層
├── redis_cache.py              # Redis 快取
├── neo4j_ingest.py             # Neo4j 整合
├── services/                   # Whaleforce 服務客戶端
│   ├── __init__.py
│   ├── sec_filings_client.py   # SEC Filings 服務
│   ├── backtester_client.py    # Backtester API
│   ├── performance_metrics_client.py  # Performance Metrics
│   └── minio_client.py         # MinIO 儲存
├── EarningsCallAgenticRag/     # 核心 Agentic RAG 模組
│   ├── agents/
│   │   ├── mainAgent.py
│   │   ├── comparativeAgent.py
│   │   ├── historicalEarningsAgent.py
│   │   ├── historicalPerformanceAgent.py
│   │   └── secFilingsAgent.py  # SEC Filings Agent (新增)
│   └── ...
├── .env.example                # 環境變數範本
├── requirements.txt            # 依賴套件
└── test_services_integration.py  # 服務整合測試
```

## 環境變數

詳見 `.env.example`，主要設定：

- `FMP_API_KEY` - Financial Modeling Prep API Key
- `OPENAI_API_KEY` - OpenAI API Key
- `NEO4J_URI` - Neo4j 連線 URI
- `SEC_FILINGS_API_URL` - SEC Filings 服務端點
- `BACKTESTER_API_URL` - Backtester API 端點
- `PERFORMANCE_METRICS_API_URL` - Performance Metrics 服務端點
- `ENABLE_*` - 功能開關

## License

MIT License
