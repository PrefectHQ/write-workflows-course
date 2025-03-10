# You are building an ETL pipeline for a weather forecasting service. The goal is to fetch ****weather forecast data for your current location, from multiple weather models all provided through the [Open-Meteo API](https://open-meteo.com/en/docs) . Average the temperature predictions and write the average prediction for the next hour and write it to a .csv file.

import os
import csv
import requests
from datetime import datetime, timedelta
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from typing import Dict, List, Tuple


@task(cache_key_fn=task_input_hash, retries=3, retry_delay_seconds=10)
def get_location_coordinates() -> Tuple[float, float]:
    """
    Get the coordinates for the current location using the ipinfo.io API.
    Returns a tuple of (latitude, longitude).
    """
    try:
        response = requests.get("https://ipinfo.io/json")
        data = response.json()
        # The location comes as a string like "37.7749,-122.4194"
        loc_parts = data["loc"].split(",")
        latitude = float(loc_parts[0])
        longitude = float(loc_parts[1])
        print(f"Current location: {data.get('city', 'Unknown')}, {data.get('region', 'Unknown')}")
        return latitude, longitude
    except Exception as e:
        print(f"Error getting location: {e}")
        # Default to San Francisco if location detection fails
        return 37.7749, -122.4194


@task(retries=2)
def fetch_weather_forecast(latitude: float, longitude: float, model: str) -> Dict:
    """
    Fetch weather forecast data from Open-Meteo API for a specific model.
    """
    base_url = "https://api.open-meteo.com/v1/forecast"
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m",
        "forecast_days": 1,
        "models": model,
        "temperature_unit": "fahrenheit"
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        print(f"Successfully fetched forecast data from model: {model}")
        return data
    except Exception as e:
        print(f"Error fetching forecast for model {model}: {e}")
        return None


@task
def extract_next_hour_temp(forecast_data: Dict) -> float:
    """
    Extract the temperature prediction for the next hour from the forecast data.
    """
    if not forecast_data:
        return None
    
    try:
        # Get current hour
        current_time = datetime.now()
        next_hour = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        next_hour_str = next_hour.strftime("%Y-%m-%dT%H:00")
        
        # Find the index of the next hour in the hourly timestamps
        hourly_times = forecast_data["hourly"]["time"]
        if next_hour_str in hourly_times:
            index = hourly_times.index(next_hour_str)
            temp = forecast_data["hourly"]["temperature_2m"][index]
            return temp
        else:
            # If exact match not found, use the first hour in the forecast
            return forecast_data["hourly"]["temperature_2m"][0]
    except Exception as e:
        print(f"Error extracting temperature: {e}")
        return None


@task
def calculate_average_temp(temperatures: List[float]) -> float:
    """
    Calculate the average temperature from multiple predictions.
    """
    valid_temps = [t for t in temperatures if t is not None]
    if not valid_temps:
        return None
    
    avg_temp = sum(valid_temps) / len(valid_temps)
    return round(avg_temp, 1)


@task
def save_to_csv(timestamp: str, location: str, avg_temp: float, models: List[str], temps: List[float]):
    """
    Save the average temperature prediction to a CSV file.
    """
    os.makedirs("data", exist_ok=True)
    file_path = "data/temperature_predictions.csv"
    file_exists = os.path.isfile(file_path)
    
    with open(file_path, mode='a', newline='') as file:
        fieldnames = ['timestamp', 'location', 'average_temp_f', 'models_used', 'individual_temps']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow({
            'timestamp': timestamp,
            'location': location,
            'average_temp_f': avg_temp,
            'models_used': ','.join(models),
            'individual_temps': ','.join(map(str, temps))
        })
    
    print(f"Saved temperature prediction to {file_path}")
    return file_path


@flow(name="weather-forecast-etl", log_prints=True)
def weather_forecast_etl():
    """
    ETL pipeline to fetch weather forecasts from multiple models,
    average the temperature predictions, and save to a CSV file.
    """
    # List of weather models to use
    models = ["best_match", "gfs_seamless", "ecmwf_ifs04"]
    
    # Get current location
    latitude, longitude = get_location_coordinates()
    
    # Fetch forecasts from each model
    forecasts = []
    for model in models:
        forecast = fetch_weather_forecast(latitude, longitude, model)
        if forecast:
            forecasts.append((model, forecast))
    
    # Extract next hour temperature from each forecast
    temperatures = []
    valid_models = []
    for model, forecast in forecasts:
        temp = extract_next_hour_temp(forecast)
        if temp is not None:
            temperatures.append(temp)
            valid_models.append(model)
    
    # Calculate average temperature
    avg_temp = calculate_average_temp(temperatures)
    
    # Get location name for the CSV
    try:
        location_response = requests.get("https://ipinfo.io/json")
        location_data = location_response.json()
        location = f"{location_data.get('city', 'Unknown')}, {location_data.get('region', 'Unknown')}"
    except:
        location = f"Lat: {latitude}, Long: {longitude}"
    
    # Current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Save to CSV
    if avg_temp is not None:
        file_path = save_to_csv(timestamp, location, avg_temp, valid_models, temperatures)
        print(f"Average temperature prediction for next hour: {avg_temp}°F")
        print(f"Data saved to {file_path}")
    else:
        print("Failed to get temperature predictions from any model.")


if __name__ == "__main__":
    # Deploy the flow to be scheduled to run every hour
    weather_forecast_etl.serve(
        name="hourly-weather-forecast",
        cron="0 * * * *"  # Run at the top of every hour
    )