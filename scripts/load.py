import pandas as pd
from sqlalchemy import create_engine

def run():

    clean_data = pd.read_csv("processed_data/clean_disaster_data.csv")
    country_summary = pd.read_csv("processed_data/country_summary.csv")

    engine = create_engine("sqlite:///processed_data/disaster_warehouse.db")

    clean_data.to_sql("disaster_records", engine, if_exists="replace", index=False)
    country_summary.to_sql("country_summary", engine, if_exists="replace", index=False)

    print("Data successfully loaded into warehouse")