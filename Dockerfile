# Usar uma imagem base do Python
FROM python:3.11-slim

# Definir o diretório de trabalho
WORKDIR /app

# Copiar os arquivos de requisitos primeiro para aproveitar o cache do Docker
COPY requirements.txt .

# Instalar as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o resto do código
COPY . .

# Expor a porta que a API vai usar
EXPOSE 8535

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
