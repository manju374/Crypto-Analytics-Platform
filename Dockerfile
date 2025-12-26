FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y libpq-dev gcc

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Copy and prepare the startup script
COPY run.sh .
RUN chmod +x run.sh

# Run the script when the container starts
CMD ["./run.sh"]
