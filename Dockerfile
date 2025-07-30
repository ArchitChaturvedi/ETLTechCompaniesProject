FROM python3.10

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Manually install Python packages inline
RUN pip install --no-cache-dir apache-airflow==2.8.1\
    pandas==2.2.2

# Copy all project files
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Entry point
CMD ["python", "scripts/extract.py"]
