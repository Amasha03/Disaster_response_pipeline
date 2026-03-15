import pandas as pd
import os

def run():

    raw_file = "data_lake/raw_disaster_data.csv"

    df = pd.read_csv(raw_file)

    print("Raw data loaded")

    # Data Type Conversion

    df['date'] = pd.to_datetime(df['date'])

    # Feature Engineering

    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    # Loss per casualty
    df['loss_per_casualty'] = df['economic_loss_usd'] / (df['casualties'] + 1)

    # Aid efficiency
    df['aid_efficiency'] = df['aid_amount_usd'] / (df['economic_loss_usd'] + 1)

    # Response speed category
    def categorize_response(hours):

        if hours < 24:
            return "Fast"
        elif hours < 72:
            return "Moderate"
        else:
            return "Slow"

    df['response_category'] = df['response_time_hours'].apply(categorize_response)

    # Data Standardization

    df['economic_loss_millions'] = df['economic_loss_usd'] / 1000000
    df['aid_amount_millions'] = df['aid_amount_usd'] / 1000000

    # Data Aggregation

    country_summary = df.groupby(['country', 'year']).agg({

        'casualties': 'sum',
        'economic_loss_usd': 'sum',
        'severity_index': 'mean',
        'recovery_days': 'mean'

    }).reset_index()

    # Save processed data

    os.makedirs("processed_data", exist_ok=True)

    df.to_csv("processed_data/clean_disaster_data.csv", index=False)
    country_summary.to_csv("processed_data/country_summary.csv", index=False)

    print("ETL processing completed")