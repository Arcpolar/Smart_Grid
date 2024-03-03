from fastapi import FastAPI, Form, Query
from fastapi.middleware.cors import CORSMiddleware
import requests
from datetime import datetime, timedelta, timezone
import random
import pandas as pd
import numpy as np
import json
import boto3
import io
import awswrangler as wr
import os
import logging

logger = logging.getLogger("uvicorn.info")
logger.setLevel(logging.DEBUG)

app = FastAPI()

# CORS middleware to allow requests from your React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

 # OpenWeather API call for London's hourly forecast
OPENWEATHER_API_KEY = os.environ['OPENWEATHER_API_KEY']
LAT = "51.5074"  # Latitude for London
LON = "-0.1278"  # Longitude for London
URL = f"http://api.openweathermap.org/data/2.5/forecast?lat={LAT}&lon={LON}&appid={OPENWEATHER_API_KEY}"

aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
aws_session_token=os.environ['AWS_SESSION_TOKEN']

endpoint_name = 'SmartGrid-lightgbm-regression-model--2024-02-25-03-46-35-075'

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name='us-east-1'
   )

def query_endpoint(endpoint_name, data):
    # Convert the data to a DataFrame and then to a CSV string
    df = pd.DataFrame(data)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, header=False) 
    csv_string = csv_buffer.getvalue()
    
    # Use the session to create a client for SageMaker Runtime
    sagemaker_runtime = session.client('sagemaker-runtime', region_name='us-east-1')
    
    # Invoke the SageMaker endpoint 
    response = sagemaker_runtime.invoke_endpoint(
        EndpointName=endpoint_name,
        ContentType='text/csv',
        Body=csv_string
    )
    
    # Deserialize the JSON response
    response_body = json.loads(response['Body'].read().decode())
    
    # Extract predictions from the JSON object
    predictions = response_body.get('prediction', [])
    
    return predictions

# CSV File Load and Model Run Method
def load_csv_run_model_and_output(filename: str):
    # Load the CSV file
    df = pd.read_csv(filename)
    
    # Assuming the first column is the timestamp
    df['timestamp'] = pd.to_datetime(df.iloc[:, 0])
    df.set_index('timestamp', inplace=True)

    # Identify the last timestamp in the dataset
    last_timestamp = df.index[-1]

    # Generate additional rows for the next two weeks
    future_timestamps = pd.date_range(start=last_timestamp + timedelta(hours=1), periods=14*24, freq='H')
    future_df = pd.DataFrame(index=future_timestamps)
    future_df['weekday'] = future_timestamps.weekday
    future_df['hour'] = future_timestamps.hour
    future_df['holiday'] = -1  # Assuming no holidays for simplicity

    # Call the OpenWeather API to get the forecast
    response = requests.get(URL)
    weather_data = response.json()

    # Free API key has a limit of 5 days forecast data with 3-hour intervals
    if 'list' in weather_data:
        # Extract temperatures for each 3-hour interval
        three_hour_temperatures = [(entry['main']['temp'] - 273.15) for entry in weather_data['list']]
        
        # Interpolate to get hourly temperatures
        hourly_temperatures = []
        for i in range(len(three_hour_temperatures) - 1):
            current_temp = three_hour_temperatures[i]
            next_temp = three_hour_temperatures[i + 1]
            temp_diff = (next_temp - current_temp) / 3
            # Generate temperatures for each hour
            hourly_temperatures.extend([current_temp, current_temp + temp_diff, current_temp + 2 * temp_diff])
        hourly_temperatures.append(three_hour_temperatures[-1])  # Add the last temperature
        
        # Calculate how many times the data should be repeated to cover 14 days
        repeat_factor = (14 * 24) // len(hourly_temperatures)
        # Ensure we cover any remaining hours not perfectly divisible
        additional_hours = (14 * 24) % len(hourly_temperatures)
        
        # Extend the hourly temperatures to cover 14 days
        extended_hourly_temperatures = hourly_temperatures * repeat_factor + hourly_temperatures[:additional_hours]
        
        # Assign extended hourly temperatures to 'temperature' column in future_df
        future_df['temperature'] = extended_hourly_temperatures[:len(future_df)]
    else:
        print("Forecast data not found in the response.")
        # Ensure future_df['temperature'] is correctly populated here
        future_df['temperature'] = random.sample(range(0, 30), len(future_df))
        
    # Prepare the future data for model predictions
    future_df['weekday'] = future_df.index.weekday
    future_df['hour'] = future_df.index.hour
    future_features_for_endpoint = future_df[['weekday', 'hour', 'temperature', 'holiday']].to_dict(orient='records')

    # Query the endpoint for future data predictions
    future_predictions = query_endpoint(endpoint_name, future_features_for_endpoint)
    print(future_predictions)  # Add this to inspect the predictions
    
    # Append future predictions
    future_df['predictions'] = future_predictions
    
    if future_predictions is not None and len(future_predictions) > 0:
        print("Appending predictions to the DataFrame.")
    else:
        print("No predictions received or predictions list is empty.")

    # Save the DataFrame with predictions
    complete_df = pd.concat([df, future_df])
    # complete_df.to_csv('output.csv', index=True)
    
    return complete_df.reset_index().to_dict(orient='records')

# Parquet File Load and Model Run Method
def load_parquet_run_model_and_output(bucket: str):
    # Define S3 path to the Parquet dataset
    path = f"s3://{bucket}/meterdataset.parquet/"
    
    # Define partition filters for training and validation datasets
    def train_filter(partitions):
        return (int(partitions["year"]) == 2012) and (int(partitions["month"]) <= 7)

    def val_filter(partitions):
        return (int(partitions["year"]) == 2012) and (int(partitions["month"]) <= 12) and (int(partitions["month"]) > 7)
    
    # Read the training dataset from Parquet
    df_parquet_train = wr.s3.read_parquet(
        path, 
        partition_filter=train_filter, 
        dataset=True
    )
    
    # Find the last timestamp in the dataset
    last_timestamp = df_parquet_train['timestamp'].max()
    
    # Generate additional rows for the next two weeks, similar to your existing logic
    future_timestamps = pd.date_range(start=last_timestamp + timedelta(hours=1), periods=14*24, freq='H')
    future_df = pd.DataFrame(index=future_timestamps)
    future_df['weekday'] = future_timestamps.weekday
    future_df['hour'] = future_timestamps.hour
    future_df['holiday'] = -1  # Assuming no holidays for simplicity
    
    # Call the OpenWeather API to get the forecast
    response = requests.get(URL)
    weather_data = response.json()

    # Free API key has a limit of 5 days forecast data with 3-hour intervals
    if 'list' in weather_data:
        # Extract temperatures for each 3-hour interval
        three_hour_temperatures = [(entry['main']['temp'] - 273.15) for entry in weather_data['list']]

        
        # Interpolate to get hourly temperatures
        hourly_temperatures = []
        for i in range(len(three_hour_temperatures) - 1):
            current_temp = three_hour_temperatures[i]
            next_temp = three_hour_temperatures[i + 1]
            temp_diff = (next_temp - current_temp) / 3
            # Generate temperatures for each hour
            hourly_temperatures.extend([current_temp, current_temp + temp_diff, current_temp + 2 * temp_diff])
        hourly_temperatures.append(three_hour_temperatures[-1])  # Add the last temperature
        
        # Calculate how many times the data should be repeated to cover 14 days
        repeat_factor = (14 * 24) // len(hourly_temperatures)
        # Ensure we cover any remaining hours not perfectly divisible
        additional_hours = (14 * 24) % len(hourly_temperatures)
        
        # Extend the hourly temperatures to cover 14 days
        extended_hourly_temperatures = hourly_temperatures * repeat_factor + hourly_temperatures[:additional_hours]
        
        # Assign extended hourly temperatures to 'temperature' column in future_df
        future_df['temperature'] = extended_hourly_temperatures[:len(future_df)]
    else:
        print("Forecast data not found in the response.")
        # Ensure future_df['temperature'] is correctly populated here
        future_df['temperature'] = random.sample(range(0, 30), len(future_df))
        
    # Prepare the future data for model predictions
    future_df['weekday'] = future_df.index.weekday
    future_df['hour'] = future_df.index.hour
    future_features_for_endpoint = future_df[['weekday', 'hour', 'temperature', 'holiday']].to_dict(orient='records')

    # Query the endpoint for future data predictions
    future_predictions = query_endpoint(endpoint_name, future_features_for_endpoint)
    print(future_predictions)  # Add this to inspect the predictions
    
    # Append future predictions
    future_df['predictions'] = future_predictions
    
    if future_predictions is not None and len(future_predictions) > 0:
        print("Appending predictions to the DataFrame.")
    else:
        print("No predictions received or predictions list is empty.")

    # Save the DataFrame with predictions
    complete_df = pd.concat([df, future_df])
    print("Saving the complete DataFrame with predictions.")
    complete_df.to_csv('output3.csv', index=True)
    
    return complete_df.reset_index().to_dict(orient='records')
    
    
# Preprocessing method to ensure the data is formatted correctly for JSON output
def preprocess_data_for_json_compliance(data):
    if data is None:
        return [] 
    processed_data = []
    for row in data:
        if not isinstance(row, dict):
            print(f"Skipping non-dict item: {row}")  # For debugging
            continue  # Skip non-dictionary items
        processed_row = {}
        for key, value in row.items():
            if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
                processed_row[key] = None
            else:
                processed_row[key] = value
        processed_data.append(processed_row)
    return processed_data

# API endpoint to get the data with predictions
@app.get("/data/")
async def get_data_with_predictions():
    data_with_predictions = load_csv_run_model_and_output('./modified_test_data.csv')
    
    json_compliant_data = preprocess_data_for_json_compliance(data_with_predictions)
    
    return json_compliant_data

# Parquet API endpoint to get the data with predictions
# @app.get("/data/")
# async def get_data_with_predictions(startDate: str = Query(None), endDate: str = Query(None)):
#     print(f"Received startDate: {startDate}, endDate: {endDate}")
#     bucket = "sagemaker-us-east-1-645257835735"
#     data_with_predictions = load_parquet_run_model_and_output(bucket)
    
#     json_compliant_data = preprocess_data_for_json_compliance(data_with_predictions)
    
#     print("Preprocessed data: ")
#     return json_compliant_data