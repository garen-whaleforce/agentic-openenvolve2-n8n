# AGENTS.md

## FMP / FinancialModelingPrep usage rules

- 本專案只允許使用 **FMP stable endpoints**。
- 所有 FMP 呼叫都必須遵守 `docs/FMP_STABLE_API_REFERENCE.md` 的白名單：
  - 只能使用檔案中列出的 `/stable/...` 路徑。
  - 嚴禁使用任何 `/api/v3`, `/api/v4`, `/v3`, `/v4`, `/sandbox`, `/demo` 路徑。
- 每當你準備新增或修改 FMP API 呼叫時：
  1. 先打開 `docs/FMP_STABLE_API_REFERENCE.md`。
  2. 找出對應 endpoint 與參數。
  3. 若找不到，**先提議更新該檔案**，再新增程式碼，不要自己猜 endpoint。

## Coding conventions

- 所有 FMP 呼叫必須透過共用的 client / helper（例如 `fmpClient.ts`），避免在各處亂拚 URL。
- 若呼叫失敗（4xx/5xx），優先檢查：
  - 是否使用 `/stable` 路徑
  - 是否帶正確 `apikey` 參數
  - 是否遵守白名單中的參數名稱

## How to talk to the user

- 當使用者要求「新增某種 FMP 資料」但白名單中沒有對應 endpoint 時：
  - 先回報「目前白名單沒有該 endpoint」，並提出更新 `docs/FMP_STABLE_API_REFERENCE.md` 的建議。
  - 不要隨意使用未列出的 endpoint。
