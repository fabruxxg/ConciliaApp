# 1. Usar una versión estándar y segura de Python
FROM python:3.11-slim

# 2. Crear una carpeta llamada /app en el servidor
WORKDIR /app

# 3. Instalar las librerías de tu lista
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiar todo el código de tu carpeta actual al servidor
COPY . .

# 5. Comando para encender tu aplicación
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]