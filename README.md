PredictiveGridOptimization (PGO)

Author:

Mehdi Attar

Tampere University

Finland

# Introduction

the main code in this repository is WeatherDatafetcher.py. It Fetches weather realted data (solar irradiance and temperature) from a server, and publishes them to MQTT broker.

## WeatherDataFetcher (FMI → MQTT)

Publishes **current observations** (temperature & solar irradiance) and **36-hour forecasts** from the **Finnish Meteorological Institute (FMI)** to **MQTT** as JSON.

- Observations: every **minute**
- Forecasts: every **hour** (36 hourly values)

---

## Quick start (Docker Compose)

This repo includes ready-to-use Compose files. Just clone, build, and run.

git clone <YOUR_REPO_URL>
cd <YOUR_REPO_DIR>

# build the fetcher image
docker build -t weather-data-fetcher:latest .

# bring up the stack (broker + fetcher + printer)
docker compose up -d

# see only the JSON payloads from the printer container
docker logs -f mqtt-printer


## Use only the weather-data-fetcher image

You can run the image standalone against any MQTT broker as well. The MQTT-Printer.py and Broker.py have been developed only for the sake of WeatherDataFetcher.py testing.
