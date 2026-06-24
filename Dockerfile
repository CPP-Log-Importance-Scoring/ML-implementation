FROM python:3.11-slim

WORKDIR /app

# Copy only requirements first so Docker can cache dependencies
COPY requirements-base.txt .
COPY requirements-ml.txt .
COPY requirements-dashboard.txt .

RUN pip install --default-timeout=400 --no-cache-dir -r requirements-dashboard.txt

# Copy the rest of the project
COPY . .

CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]