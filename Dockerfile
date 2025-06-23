# Dockerfile para actor de Apify con Python
FROM python:3.10-slim

WORKDIR /usr/src/app

# Copiar y instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código
COPY . .

# Comando por defecto al iniciar el container
CMD ["python", "scraper.py"]
