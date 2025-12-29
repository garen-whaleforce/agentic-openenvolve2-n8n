# FMP Stable API Reference (Project White‑list)

本專案 **只允許使用 FMP stable endpoints**。  
任何不在本檔案列出的 endpoint 或參數，一律視為「未經允許」。

---

## 0. Global rules

- **Base URL**
  - `https://financialmodelingprep.com`
- **Authentication**
  - 所有請求都必須帶 `apikey` query 參數。
  - 若 URL 尚沒有 query：`?apikey=YOUR_API_KEY`
  - 若 URL 已有 query 參數：`&apikey=YOUR_API_KEY` 
- **版本規則（非常重要）**
  - ✅ 只使用路徑以 `/stable/` 開頭的 endpoint  
    （例如：`/stable/income-statement`）
  - ❌ 嚴禁使用：`/api/v3`、`/api/v4`、`/v3`、`/v4`、`/sandbox`、`/demo`。
- **準則**
  - 當需要新增 FMP 資料但本檔沒列出對應 endpoint：
    1. **先更新本檔**（新增 whitelist 條目），
    2. 再撰寫或修改程式碼。
  - 不得「猜測」 endpoint 或參數名稱。

---

## 1. Company Search

- `GET /stable/search-symbol?query={text}`
  - 依 ticker 查詢（多市場）。
- `GET /stable/search-name?query={text}`
  - 依公司名稱/部分名稱查 ticker。
- `GET /stable/search-cik?cik={cik}`
  - 依 CIK 查公司。
- `GET /stable/search-cusip?cusip={cusip}`
  - 依 CUSIP 查證券資訊。
- `GET /stable/search-isin?isin={isin}`
  - 依 ISIN 查證券資訊。
- `GET /stable/company-screener`
  - 股票篩選器，條件透過 query 參數（市值、國家、產業…）。
- `GET /stable/search-exchange-variants?symbol={symbol}`
  - 查某 symbol 在不同交易所對應。

---

## 2. Stock Directory

- `GET /stable/stock-list`
  - 全部股票／金融商品清單。
- `GET /stable/financial-statement-symbol-list`
  - 有財報資料的 symbol 清單。
- `GET /stable/cik-list?page={page}&limit={limit}`
  - CIK 清單。
- `GET /stable/symbol-change`
  - symbol 變更紀錄。
- `GET /stable/etf-list`
  - ETF 清單。
- `GET /stable/actively-trading-list`
  - 目前有在交易的標的清單。
- `GET /stable/earnings-transcript-list`
  - 有 earnings transcript 的公司清單。
- `GET /stable/available-exchanges`
  - 支援的交易所列表。
- `GET /stable/available-sectors`
  - 可用的 sector 列表。
- `GET /stable/available-industries`
  - 可用的 industry 列表。
- `GET /stable/available-countries`
  - 有股票的國家列表。

---

## 3. Company Information

- `GET /stable/profile?symbol={symbol}`
  - 公司基本資料（by symbol）。
- `GET /stable/profile-cik?cik={cik}`
  - 公司基本資料（by CIK）。
- `GET /stable/company-notes?symbol={symbol}`
  - 公司發行的 notes 資訊。
- `GET /stable/stock-peers?symbol={symbol}`
  - 同產業同市值級距同儕列表。
- `GET /stable/delisted-companies?page={page}&limit={limit}`
  - 已下市公司列表。
- `GET /stable/employee-count?symbol={symbol}`
  - 員工人數（最新）。
- `GET /stable/historical-employee-count?symbol={symbol}`
  - 員工人數歷史資料。
- `GET /stable/market-capitalization?symbol={symbol}`
  - 公司市值（可帶日期）。
- `GET /stable/market-capitalization-batch?symbols={comma_separated_symbols}`
  - 多公司市值。
- `GET /stable/historical-market-capitalization?symbol={symbol}`
  - 公司市值歷史資料。
- `GET /stable/shares-float?symbol={symbol}`
  - 單一公司的流通在外股數等。
- `GET /stable/shares-float-all?page={page}&limit={limit}`
  - 所有公司 shares float 列表。
- `GET /stable/mergers-acquisitions-latest?page={page}&limit={limit}`
  - 最新 M&A 紀錄。
- `GET /stable/mergers-acquisitions-search?name={text}`
  - 依公司名稱搜尋 M&A。
- `GET /stable/key-executives?symbol={symbol}`
  - 公司重要主管資訊。
- `GET /stable/governance-executive-compensation?symbol={symbol}`
  - 公司主管薪酬細節。
- `GET /stable/executive-compensation-benchmark`
  - 各產業主管薪酬平均。

---

## 4. Quote（即時行情）

- `GET /stable/quote?symbol={symbol}`
  - 單一股票即時報價。
- `GET /stable/quote-short?symbol={symbol}`
  - 精簡版即時報價。
- `GET /stable/aftermarket-trade?symbol={symbol}`
  - 盤後成交明細。
- `GET /stable/aftermarket-quote?symbol={symbol}`
  - 盤後報價。
- `GET /stable/stock-price-change?symbol={symbol}`
  - 價格變化（各種期間）。
- `GET /stable/batch-quote?symbols={comma_separated_symbols}`
  - 多股票即時報價。
- `GET /stable/batch-quote-short?symbols={comma_separated_symbols}`
  - 多股票精簡報價。
- `GET /stable/batch-aftermarket-trade?symbols={comma_separated_symbols}`
  - 多股票盤後成交。
- `GET /stable/batch-aftermarket-quote?symbols={comma_separated_symbols}`
  - 多股票盤後報價。
- `GET /stable/batch-exchange-quote?exchange={exchange_code}`
  - 指定交易所全部股票報價。
- `GET /stable/batch-mutualfund-quotes`
  - 共同基金報價。
- `GET /stable/batch-etf-quotes`
  - ETF 報價。
- `GET /stable/batch-commodity-quotes`
  - 商品（原油、黃金…）報價。
- `GET /stable/batch-crypto-quotes`
  - 加密貨幣報價。
- `GET /stable/batch-forex-quotes`
  - 外匯 pair 報價。
- `GET /stable/batch-index-quotes`
  - 指數報價。

---

## 5. Statements（財報與衍生指標）

> 若未指定 `period`，預設請依官方文件。常用：
> - `period=annual` or `quarter`

**核心三表**

- `GET /stable/income-statement?symbol={symbol}`
- `GET /stable/balance-sheet-statement?symbol={symbol}`
- `GET /stable/cash-flow-statement?symbol={symbol}`

**最新 / TTM**

- `GET /stable/latest-financial-statements?page={page}&limit={limit}`
- `GET /stable/income-statement-ttm?symbol={symbol}`
- `GET /stable/balance-sheet-statement-ttm?symbol={symbol}`
- `GET /stable/cash-flow-statement-ttm?symbol={symbol}`

**比率 & key metrics**

- `GET /stable/key-metrics?symbol={symbol}`
- `GET /stable/ratios?symbol={symbol}`
- `GET /stable/key-metrics-ttm?symbol={symbol}`
- `GET /stable/ratios-ttm?symbol={symbol}`
- `GET /stable/financial-scores?symbol={symbol}`  <!-- Altman, Piotroski... -->
- `GET /stable/owner-earnings?symbol={symbol}`
- `GET /stable/enterprise-values?symbol={symbol}`

**成長分析**

- `GET /stable/income-statement-growth?symbol={symbol}`
- `GET /stable/balance-sheet-statement-growth?symbol={symbol}`
- `GET /stable/cash-flow-statement-growth?symbol={symbol}`
- `GET /stable/financial-growth?symbol={symbol}`

**財報原文 / 10‑K / segmentation**

- `GET /stable/financial-reports-dates?symbol={symbol}`
- `GET /stable/financial-reports-json?symbol={symbol}&year={yyyy}&period={FY|Q1...}`
- `GET /stable/financial-reports-xlsx?symbol={symbol}&year={yyyy}&period={FY|Q1...}`
- `GET /stable/revenue-product-segmentation?symbol={symbol}`
- `GET /stable/revenue-geographic-segmentation?symbol={symbol}`

**As‑reported（未標準化版本）**

- `GET /stable/income-statement-as-reported?symbol={symbol}`
- `GET /stable/balance-sheet-statement-as-reported?symbol={symbol}`
- `GET /stable/cash-flow-statement-as-reported?symbol={symbol}`
- `GET /stable/financial-statement-full-as-reported?symbol={symbol}`

---

## 6. Charts（股價歷史）

**EOD**

- `GET /stable/historical-price-eod/light?symbol={symbol}`
- `GET /stable/historical-price-eod/full?symbol={symbol}`
- `GET /stable/historical-price-eod/non-split-adjusted?symbol={symbol}`
- `GET /stable/historical-price-eod/dividend-adjusted?symbol={symbol}`

**Intraday**

- `GET /stable/historical-chart/1min?symbol={symbol}`
- `GET /stable/historical-chart/5min?symbol={symbol}`
- `GET /stable/historical-chart/15min?symbol={symbol}`
- `GET /stable/historical-chart/30min?symbol={symbol}`
- `GET /stable/historical-chart/1hour?symbol={symbol}`
- `GET /stable/historical-chart/4hour?symbol={symbol}`

---

## 7. Economics

- `GET /stable/treasury-rates`
- `GET /stable/economic-indicators?name={indicator_name}`
- `GET /stable/economic-calendar`
- `GET /stable/market-risk-premium`

---

## 8. Earnings, Dividends, Splits

- `GET /stable/dividends?symbol={symbol}`
- `GET /stable/dividends-calendar`
- `GET /stable/earnings?symbol={symbol}`
- `GET /stable/earnings-calendar`
- `GET /stable/ipos-calendar`
- `GET /stable/ipos-disclosure`
- `GET /stable/ipos-prospectus`
- `GET /stable/splits?symbol={symbol}`
- `GET /stable/splits-calendar`

---

## 9. Earnings Transcript

- `GET /stable/earning-call-transcript-latest`
- `GET /stable/earning-call-transcript?symbol={symbol}&year={yyyy}&quarter={q}`
- `GET /stable/earning-call-transcript-dates?symbol={symbol}`
- `GET /stable/earnings-transcript-list`  <!-- 也出現在 Stock Directory 區塊 -->

---

## 10. News

- `GET /stable/fmp-articles?page={page}&limit={limit}`
- `GET /stable/news/general-latest?page={page}&limit={limit}`
- `GET /stable/news/press-releases-latest?page={page}&limit={limit}`
- `GET /stable/news/stock-latest?page={page}&limit={limit}`
- `GET /stable/news/crypto-latest?page={page}&limit={limit}`
- `GET /stable/news/forex-latest?page={page}&limit={limit}`
- `GET /stable/news/press-releases?symbols={symbols}`
- `GET /stable/news/stock?symbols={symbols}`
- `GET /stable/news/crypto?symbols={symbols}`
- `GET /stable/news/forex?symbols={symbols}`

---

## 11. Form 13F / Institutional Ownership

- `GET /stable/institutional-ownership/latest?page={page}&limit={limit}`
- `GET /stable/institutional-ownership/extract?cik={cik}&year={yyyy}&quarter={q}`
- `GET /stable/institutional-ownership/dates?cik={cik}`
- `GET /stable/institutional-ownership/extract-analytics/holder?symbol={symbol}&year={yyyy}&quarter={q}&page={page}&limit={limit}`
- `GET /stable/institutional-ownership/holder-performance-summary?cik={cik}&page={page}`
- `GET /stable/institutional-ownership/holder-industry-breakdown?cik={cik}&year={yyyy}&quarter={q}`
- `GET /stable/institutional-ownership/symbol-positions-summary?symbol={symbol}&year={yyyy}&quarter={q}`
- `GET /stable/institutional-ownership/industry-summary?year={yyyy}&quarter={q}`

---

## 12. Analyst

- `GET /stable/analyst-estimates?symbol={symbol}&period={annual|quarter}&page={page}&limit={limit}`
- `GET /stable/ratings-snapshot?symbol={symbol}`
- `GET /stable/ratings-historical?symbol={symbol}`
- `GET /stable/price-target-summary?symbol={symbol}`
- `GET /stable/price-target-consensus?symbol={symbol}`
- `GET /stable/grades?symbol={symbol}`
- `GET /stable/grades-historical?symbol={symbol}`
- `GET /stable/grades-consensus?symbol={symbol}`

---

## 13. Market Performance

- `GET /stable/sector-performance-snapshot?date={yyyy-mm-dd}`
- `GET /stable/industry-performance-snapshot?date={yyyy-mm-dd}`
- `GET /stable/historical-sector-performance?sector={name}`
- `GET /stable/historical-industry-performance?industry={name}`
- `GET /stable/sector-pe-snapshot?date={yyyy-mm-dd}`
- `GET /stable/industry-pe-snapshot?date={yyyy-mm-dd}`
- `GET /stable/historical-sector-pe?sector={name}`
- `GET /stable/historical-industry-pe?industry={name}`
- `GET /stable/biggest-gainers`
- `GET /stable/biggest-losers`
- `GET /stable/most-actives`

---

## 14. Technical Indicators（常用）

> 所有 technical endpoints 都有類似參數：  
> `symbol`, `periodLength`, `timeframe`（例如 `1day`）。

- `GET /stable/technical-indicators/sma?symbol={symbol}&periodLength={n}&timeframe={timeframe}`
- `GET /stable/technical-indicators/ema?symbol={symbol}&periodLength={n}&timeframe={timeframe}`
- `GET /stable/technical-indicators/wma?symbol={symbol}&periodLength={n}&timeframe={timeframe}`
- `GET /stable/technical-indicators/dema?symbol={symbol}&periodLength={n}&timeframe={timeframe}`
- `GET /stable/technical-indicators/tema?symbol={symbol}&periodLength={n}&timeframe={timeframe}`
- `GET /stable/technical-indicators/rsi?symbol={symbol}&periodLength={n}&timeframe={timeframe}`
- `GET /stable/technical-indicators/adx?symbol={symbol}&periodLength={n}&timeframe={timeframe}`

> 若需要使用其他指標，**必須先更新本檔，新增對應 endpoint 條目**。

---

## 15. ETF & Mutual Funds（結構 / 持股）

- `GET /stable/etf/holdings?symbol={symbol}`
- `GET /stable/etf/info?symbol={symbol}`
- `GET /stable/etf/country-weightings?symbol={symbol}`
- （其餘 ETF/Mutual Funds 關聯 endpoint 若要用，請先加到本白名單）

---

## 16. SEC Filings（節選，供本專案使用）

- `GET /stable/sec-profile?symbol={symbol}`
  - 公司 SEC 檔案摘要與分類。
- `GET /stable/sec-industry-classification-symbol?symbol={symbol}`
- `GET /stable/sec-industry-classification-filings?cik={cik}`
- `GET /stable/sec-industry-classification-search?keyword={text}`

> 其他 filings 相關 endpoints（完整 filings list、calendar 等）  
> 如需要，先更新此檔案再使用。

---

## 17. Indexes & Market Hours

- `GET /stable/index-list`
- `GET /stable/sp500-constituent`
- `GET /stable/nasdaq-constituent`
- `GET /stable/dowjones-constituent`
- `GET /stable/historical-sp500-constituent`
- `GET /stable/historical-nasdaq-constituent`
- `GET /stable/historical-dowjones-constituent`
- `GET /stable/exchange-market-hours?exchange={exchange_code}`
- `GET /stable/holidays-by-exchange?exchange={exchange_code}`
- `GET /stable/all-exchange-market-hours`

---

## 18. Commodity / Forex / Crypto（主要路徑）

**Commodity**

- `GET /stable/batch-commodity-quotes`  （已列在 Quote 區）
- 其他商品相關歷史資料如需使用，先加白名單。

**Forex**

- `GET /stable/forex-pairs`
- `GET /stable/historical-forex?pair={pair}&from={date}&to={date}`
- `GET /stable/intraday-forex?pair={pair}&timeframe={1min|5min...}`

**Crypto**

- `GET /stable/crypto-list`
- `GET /stable/historical-crypto?symbol={symbol}&from={date}&to={date}`
- `GET /stable/intraday-crypto?symbol={symbol}&timeframe={1min|5min...}`

> 其他衍生 endpoints（占空間較大）同樣採「先新增到本檔，再在程式中使用」的流程。

---

## 19. ESG / Senate / Fundraisers / Commitment of Traders（簡要）

**ESG**

- `GET /stable/esg-ratings?symbol={symbol}`
- `GET /stable/esg-benchmark?symbol={benchmark_symbol}`

**Senate / Congressional trading**

- `GET /stable/congressional-trading/symbol?symbol={symbol}`
- `GET /stable/congressional-trading/latest`
- `GET /stable/congressional-trading/summary?symbol={symbol}`

**Fundraisers**

- `GET /stable/funding-rounds?symbol={symbol}`
- `GET /stable/funding-rounds-latest`

**Commitment of Traders**

- `GET /stable/commitment-of-traders/list`
- `GET /stable/commitment-of-traders/cot?code={code}`
- `GET /stable/commitment-of-traders/cot-summary?code={code}`

---

## 20. Bulk APIs（原則）

Bulk / export 類 endpoint 多為 **POST**，用於一次下載大批資料。  
本專案如需使用 bulk：

1. 先到官方文件確認該 bulk endpoint 路徑與 body schema。  
2. 在本檔新增一條類似：

   - `POST /stable/bulk-historical-price-eod`  
     - body: `{"symbolsList":["AAPL","MSFT"],"from":"YYYY-MM-DD","to":"YYYY-MM-DD"}`

3. 才能在程式碼中使用。

---

## 21. Local conventions for this project

- 嚴禁在程式碼中寫死 `/api/v3` 或 `/api/v4`。  
- 若你在 PR 中新增任何 FMP 呼叫：
  - 必須標註參考了哪一條 `FMP_STABLE_API_REFERENCE` 條目。
- 如需新增 endpoint：
  - 優先開 PR 先更新本檔，再更新 code。
