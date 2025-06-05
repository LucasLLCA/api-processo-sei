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
EXPOSE 8443

# Comando para executar a aplicação
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8443"]