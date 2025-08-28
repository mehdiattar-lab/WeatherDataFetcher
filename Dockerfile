# Dockerfile
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

# System deps (optional, keeps image small)
RUN pip install --no-cache-dir requests paho-mqtt

# Copy only the app (adjust name if different)
COPY WeatherDataFetcher.py /app/WeatherDataFetcher.py

# Default command (location comes from compose's args)
CMD ["python", "WeatherDataFetcher.py"]
