import sys
import logging
import pandas as pd
import requests
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # Get the directory of the current script

load_dotenv(os.path.join(BASE_DIR, ".env")) # Load environment variables from .env file

LOG_PATH = os.path.join(BASE_DIR, "zipco_etl.log") # Define the log file path
logging.basicConfig(filename=LOG_PATH, level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s'
                    ) # Configure logging information to the log file


# Fetch environment variables
url = os.getenv("API_URL")
api_key = os.getenv("API_KEY")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = 5432
db = os.getenv("DB_NAME")

# Define headers for the API request
headers = {
    "accept": "application/json",
    "X-Api-Key": api_key
}

eng = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{db}')

def extract_data(url):
    # This function gets a response from the assigned url and extracts data from that url. 
    # If the connection is successful, the status code will display 200; otherwise, it will throw an error.
    response = requests.get(url, headers=headers)
    if response.status_code == 200:        
        data = response.json()
        df = pd.json_normalize(data)
        # print("Data extraction successful.")
        df.to_sql('raw_properties', eng, if_exists='replace', index=False)
        # print("Data loaded into database successfully.")
        return df
    else:
        raise Exception(f"Error in Pipeline: {response.status_code} - {response.text}")
    
df = extract_data(url) # Extract data from the API and load it into the database

def transform_data(df):
    # This function transforms the extracted data by selecting relevant columns and renaming them.
    prop_clean = df[['id','formattedAddress','addressLine1','addressLine2','city','state','zipCode','county','latitude','longitude',
        'propertyType','bedrooms','bathrooms','squareFootage','yearBuilt','assessorID','features.floorCount','legalDescription',
        'subdivision','ownerOccupied','features.architectureType']].copy()
    prop_clean.rename(columns={
        'formattedAddress':'full_address',
        'zipCode':'postal_code',
        'propertyType':'property_type',
        'yearBuilt':'year_built',
        'squareFootage':'square_footage',
        'legalDescription':'legal_description',
        'ownerOccupied':'owner_occupied',
        'features.floorCount':'floor_count',
        'features.architectureType':'architecture_type'}, inplace=True)
    prop_clean.columns = (
        prop_clean.columns
        .str.lower() # Convert to lowercase
        .str.strip() # Remove leading/trailing whitespace
        .str.strip('_') # Remove leading/trailing underscores
        .str.replace(' ', '_') # Replace spaces with underscores
        .str.replace(r'([A-Z])', r'_\1', regex=True) # Convert camelCase to snake_case
        .str.replace(r'[^a-z0-9_]', r'_', regex=True) # Remove special characters
        .str.replace('__', '_') # Replace double underscores with single underscore      
        )
    # print("Data transformed successful.")
    prop_clean.to_sql('transform_properties', eng, if_exists='replace', index=False)
    # print("Data loaded into database successfully.")
    return prop_clean

transform_data(df) # Transform the data and load it into the database

def main():
    # Main function to run the ETL pipeline
    logging.info("ETL Pipeline started.")
    try:
        df = extract_data(url)
        df.to_sql('raw_properties', eng, if_exists='replace', index=False)

        df_clean = transform_data(df)
        df_clean.to_sql('transform_properties', eng, if_exists='replace', index=False)

        logging.info("Rows: raw_properties=%s, transform_properties=%s", len(df), len(df_clean))
        logging.info("ETL Pipeline completed successfully.")
        return True
    except Exception as e:
        logging.error(f"ETL Pipeline failed: {e}")
        return False
    
if __name__ == "__main__":
    sys.exit(main())

