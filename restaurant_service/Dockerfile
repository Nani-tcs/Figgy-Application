FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Copy common utilities
COPY common /app/common

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
