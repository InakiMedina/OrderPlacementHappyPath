FROM python:3.12-slim

WORKDIR /app

# 1. Copy only requirements first to cache dependency layers
COPY requirements.txt .

# 2. Install dependencies (only re-runs if requirements.txt changes)
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy application source code
COPY . .

EXPOSE 5000

CMD ["python", "app.py"]
