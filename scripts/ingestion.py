import shutil
import os

def run():

    source_file = "data_source/global_disaster_response_2018_2024.csv"
    destination_folder = "data_lake"

    os.makedirs(destination_folder, exist_ok=True)

    destination_file = os.path.join(destination_folder, "raw_disaster_data.csv")

    shutil.copy(source_file, destination_file)

    print("Data successfully ingested to Data Lake")