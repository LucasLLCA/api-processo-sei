# Usar uma imagem base do Python
FROM python:3.11

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

# Comando para executar a aplicação
CMD ["uvicorn", "api.main:app", "--port", "8535"]