FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ANALYSIS_DB_PATH=/tmp/analysis.db \
    PIP_EXTRA_INDEX_URL=https://download.pytorch.org/whl/cpu

WORKDIR /app

# 系統需求：git（仍保留，若未來需要 debug 或拉取其他資源）
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

# 先安裝依賴
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 複製應用程式碼
COPY . /app

# 啟動命令（使用平台提供的 PORT，預設 8000），先生成 credentials.json
RUN chmod +x /app/entrypoint.sh
CMD ["/app/entrypoint.sh"]
