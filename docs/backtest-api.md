# 回測 API 使用說明

## 基本資訊

- **Base URL**: `https://agentic-rag-full-v2-30.zeabur.app`
- **Profile**: `garen1212v4`
- **資料範圍**: 2020Q1 - 2025Q2，約 3000 筆成功樣本

---

## API 端點

### 1. 取得系統資訊

```
GET /api/backtest/info
```

**範例**:
```bash
curl https://agentic-rag-full-v2-30.zeabur.app/api/backtest/info
```

**回傳**:
```json
{
  "profile": "garen1212v4",
  "total_records": 3500,
  "success_records": 2988,
  "years": [2020, 2021, 2022, 2023, 2024, 2025],
  "quarters": [1, 2, 3, 4],
  "symbol_count": 200
}
```

---

### 2. 執行回測

```
POST /api/backtest/run
```

**參數**:

| 參數 | 類型 | 預設值 | 說明 |
|------|------|--------|------|
| `up_threshold` | int | 6 | UP 預測閾值 (direction_score >= threshold) |
| `only_up` | bool | true | 只做 UP 預測，DOWN 視為 NEUTRAL |
| `symbols` | list | null | 指定股票列表，如 `["AAPL", "MSFT"]` |
| `year_from` | int | null | 起始年份 |
| `year_to` | int | null | 結束年份 |
| `quarters` | list | null | 指定季度，如 `[3]` 表示只看 Q3 |
| `limit` | int | null | 限制回傳筆數 |

**範例**:

```bash
# UP >= 6 策略（預設）
curl -X POST https://agentic-rag-full-v2-30.zeabur.app/api/backtest/run \
  -H "Content-Type: application/json" \
  -d '{"up_threshold": 6, "only_up": true}'

# UP >= 7 策略（較高勝率）
curl -X POST https://agentic-rag-full-v2-30.zeabur.app/api/backtest/run \
  -H "Content-Type: application/json" \
  -d '{"up_threshold": 7, "only_up": true}'

# 只看 Q3（勝率最高 ~70%）
curl -X POST https://agentic-rag-full-v2-30.zeabur.app/api/backtest/run \
  -H "Content-Type: application/json" \
  -d '{"up_threshold": 6, "only_up": true, "quarters": [3]}'

# 指定股票 + 年份範圍
curl -X POST https://agentic-rag-full-v2-30.zeabur.app/api/backtest/run \
  -H "Content-Type: application/json" \
  -d '{"symbols": ["AAPL", "MSFT", "GOOGL"], "year_from": 2023, "up_threshold": 7}'
```

**回傳**:
```json
{
  "profile": "garen1212v4",
  "strategy": {
    "up_threshold": 7,
    "only_up": true
  },
  "filters": {
    "symbols": null,
    "year_from": null,
    "year_to": null,
    "quarters": null
  },
  "statistics": {
    "total_samples": 2988,
    "hit": 729,
    "miss": 444,
    "skip": 1815,
    "valid_predictions": 1173,
    "accuracy": 62.15,
    "coverage": 39.26
  },
  "results": [
    {
      "symbol": "AAPL",
      "year": 2024,
      "quarter": 3,
      "earnings_date": "2024-08-01",
      "direction_score": 8,
      "prediction": "UP",
      "t30_actual": 12.5,
      "hit_result": "HIT"
    }
  ]
}
```

---

### 3. 列出預設策略

```
GET /api/backtest/strategies
```

**範例**:
```bash
curl https://agentic-rag-full-v2-30.zeabur.app/api/backtest/strategies
```

**回傳**:
```json
{
  "profile": "garen1212v4",
  "strategies": [
    {
      "name": "original",
      "description": "原始 (UP>=6, DOWN<=4)",
      "up_threshold": 6,
      "only_up": false,
      "hit": 1473,
      "miss": 1045,
      "skip": 470,
      "accuracy": 58.5,
      "coverage": 84.27
    },
    {
      "name": "up_only_6",
      "description": "只UP (>=6)",
      "up_threshold": 6,
      "only_up": true,
      "accuracy": 60.12,
      "coverage": 53.82
    },
    {
      "name": "up_only_7",
      "description": "只UP (>=7)",
      "up_threshold": 7,
      "only_up": true,
      "accuracy": 62.15,
      "coverage": 39.26
    },
    {
      "name": "up_only_8",
      "description": "只UP (>=8)",
      "up_threshold": 8,
      "only_up": true,
      "accuracy": 64.5,
      "coverage": 25.1
    }
  ]
}
```

---

## Python 使用範例

```python
import requests

BASE_URL = "https://agentic-rag-full-v2-30.zeabur.app"

# 1. 取得系統資訊
info = requests.get(f"{BASE_URL}/api/backtest/info").json()
print(f"總樣本: {info['success_records']}")

# 2. 執行回測
resp = requests.post(f"{BASE_URL}/api/backtest/run", json={
    "up_threshold": 7,
    "only_up": True
})
data = resp.json()
print(f"勝率: {data['statistics']['accuracy']}%")
print(f"有效預測: {data['statistics']['valid_predictions']} 筆")

# 3. 只看 Q3 季度
resp = requests.post(f"{BASE_URL}/api/backtest/run", json={
    "up_threshold": 6,
    "only_up": True,
    "quarters": [3]
})
q3_data = resp.json()
print(f"Q3 勝率: {q3_data['statistics']['accuracy']}%")

# 4. 列出所有策略
strategies = requests.get(f"{BASE_URL}/api/backtest/strategies").json()
for s in strategies['strategies']:
    print(f"{s['name']}: {s['accuracy']}% ({s['hit']}/{s['hit']+s['miss']})")
```

---

## 策略建議

| 策略 | 勝率 | 覆蓋率 | 適用場景 |
|------|------|--------|----------|
| UP >= 6 | ~60% | ~54% | 平衡型，適合一般使用 |
| UP >= 7 | ~62% | ~39% | 較高勝率，出手次數較少 |
| UP >= 6 + Q3 only | ~70% | ~18% | 最高勝率，但僅限 Q3 季度 |
| UP >= 8 | ~65% | ~25% | 高信心預測，出手最少 |

---

## 注意事項

1. **T+30 回報**: `t30_actual` 表示財報後 30 個交易日的股價變化百分比
2. **Direction Score**: 0-10 分，分數越高表示 LLM 越看好
3. **覆蓋率 (Coverage)**: 有效預測數佔總樣本的比例
4. **DOWN 預測不可靠**: 歷史數據顯示 DOWN 預測準確率接近隨機 (~49%)，建議只做 UP 預測
