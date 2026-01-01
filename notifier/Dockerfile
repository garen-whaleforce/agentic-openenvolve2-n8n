# ============================================
# Earnings Call Notifier - Dockerfile
# ============================================

FROM node:20-slim

# 設定工作目錄
WORKDIR /app

# 複製 package files
COPY package*.json ./

# 安裝依賴
RUN npm ci --only=production

# 複製原始碼
COPY . .

# 編譯 TypeScript
RUN npm run build

# 設定環境變數
ENV NODE_ENV=production
ENV PORT=3000

# 暴露埠號
EXPOSE 3000

# 健康檢查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:3000/healthz || exit 1

# 啟動命令
CMD ["node", "dist/index.js"]
