# Use official lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (needed for checking connectivity etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Environment variables should be passed at runtime (docker run -e ...)
# CMD to run the script. Ideally this container might be run as a cronjob host or scheduled task.
# If just running once:
CMD ["python", "main.py"]
