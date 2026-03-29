FROM python:3.11-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

FROM python:3.11-slim

WORKDIR /app

# 只复制安装好的包，不复制pip等工具
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY . .

# 健康检查
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8520/api/health', timeout=3)" || exit 1

EXPOSE 8520

CMD ["python", "run.py", "web", "--host", "0.0.0.0", "--port", "8520"]
