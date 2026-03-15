from scripts import ingestion
from scripts import etl
from scripts import load

def run_pipeline():

    print("Pipeline started")

    ingestion.run()

    etl.run()

    load.run()

    print("Pipeline completed successfully")


if __name__ == "__main__":

    run_pipeline()