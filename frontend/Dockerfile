# ============================================
# Earnings Call Analyzer Frontend - Dockerfile
# ============================================

FROM node:20-slim AS builder

WORKDIR /app

# 複製 package files
COPY package*.json ./

# 安裝所有依賴（包含 devDependencies 用於 build）
RUN npm ci

# 複製原始碼
COPY . .

# 編譯 Next.js
RUN npm run build

# ============================================
# Production stage
# ============================================
FROM node:20-slim AS runner

WORKDIR /app

ENV NODE_ENV=production

# 複製必要檔案
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/node_modules ./node_modules

# 建立 public 資料夾（如果需要）
RUN mkdir -p ./public

# 設定非 root 使用者
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 nextjs && \
    chown -R nextjs:nodejs /app

USER nextjs

# 暴露埠號（實際 port 由 PORT 環境變數決定）
EXPOSE 3000

# 健康檢查
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:${PORT:-3000}/ || exit 1

# 啟動命令
CMD ["npm", "start"]
