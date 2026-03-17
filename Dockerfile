# Usar uma imagem base do Python
FROM python:3.11-slim

# Definir o diretório de trabalho
WORKDIR /app

# Instalar dependências do sistema necessárias para PDF processing
RUN apt-get update && apt-get install -y \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Copiar os arquivos de requisitos primeiro para aproveitar o cache do Docker
COPY requirements.txt .

# Instalar as dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o resto do código
COPY . .

# Expor a porta que a API vai usar
EXPOSE 8535

# OpenTelemetry configuration
ENV OTEL_EXPORTER_OTLP_ENDPOINT=http://otelcollectorhttp.10.0.122.91.sslip.io
ENV OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
ENV OTEL_SERVICE_NAME=api-processo-sei
ENV OTEL_LOGS_EXPORTER=otlp
ENV OTEL_METRICS_EXPORTER=otlp
ENV OTEL_TRACES_EXPORTER=otlp

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8535/')" || exit 1

# Comando para executar a aplicação com otimizações
CMD ["uvicorn", "api.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8535", \
     "--workers", "4", \
     "--loop", "uvloop", \
     "--http", "httptools", \
     "--limit-concurrency", "100", \
     "--limit-max-requests", "10000", \
     "--timeout-keep-alive", "30"]
