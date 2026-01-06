# ============================================
# Earnings Call Notifier - Dockerfile
# ============================================

FROM node:20-slim AS builder

WORKDIR /app

# 複製 package files
COPY package*.json ./

# 安裝所有依賴（包含 devDependencies 用於 build）
RUN npm ci

# 複製原始碼
COPY . .

# 編譯 TypeScript
RUN npm run build

# ============================================
# Production stage
# ============================================
FROM node:20-slim AS runner

WORKDIR /app

# 安裝 curl 用於健康檢查
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

ENV NODE_ENV=production

# 複製必要檔案
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules

# 建立資料目錄（用於存儲待分析佇列）
RUN mkdir -p /app/data && chown -R node:node /app/data
VOLUME ["/app/data"]

# 暴露埠號（實際 port 由 PORT 環境變數決定）
EXPOSE 3000

# 健康檢查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:${PORT:-3000}/healthz || exit 1

# 啟動命令
CMD ["node", "dist/index.js"]
