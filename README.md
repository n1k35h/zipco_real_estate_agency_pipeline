# Zipco Real Estate Agency ETL Pipeline
Building an Efficient ETL Pipeline for Property Records in Real Estate using PostgresSQL Database for Zipco Real Estate Agency

This project provides an ETL (Extract, Transform, Load) pipeline for collecting, cleaning, and storing real estate property data from an external API into a PostgreSQL database. The pipeline is implemented in Python and is designed to be easy to configure and run.

## Features

- **Extract:** Fetches property data from a REST API using secure API keys (`requests`).
- **Transform:** Cleans and standardizes the data, selecting relevant columns and applying consistent naming conventions (`pandas`).
- **Load:** Saves both the raw and transformed data into PostgreSQL tables (`SQLAlchemy`).
    - A star schema is created:
        - Dimension Tables: 
            - dim_location
            - dim_property
            - dim_occupied
        - Fact Table:
            - property_facts
- **Logging:** Logs pipeline progress and errors to a log file for easy monitoring and troubleshooting.
- **Environment Variables:** Uses a `.env` file to securely manage sensitive credentials and configuration.

## Requirements

- Python 3.7+
- PostgreSQL database
- Python packages:
  - pandas
  - requests
  - SQLAlchemy
  - python-dotenv

Install dependencies with:

```sh
pip install pandas requests sqlalchemy python-dotenv psycopg2-binary
```

## Setup & execution of the file

1. **Clone the repository** and navigate to the project directory:
    
    ```sh
    git clone https://github.com/n1k35h/zipco_real_estate_agency_pipeline.git
    cd zipco-pipeline
    ```

2. **Create a Virtual Environment** 
    ```sh
    python -m venv my_venv
    source my_venv/Scripts/activate
    ```

3. **Create a `.env` file** 
    ```
    API_URL = https://api.rentcast.io/v1/properties?limit=500
    API_KEY = your_api_key
    DB_USERNAME = your_db_username
    DB_PASSWORD = your_password
    DB_HOST = localhost
    DB_NAME = your_db_name

4. **Ensure the PostgreSQL database is running** and the credentials in `.env` are correct.

    ## Usage

    Run the ETL pipeline with:

    ```sh
    python zipco_etl.py
    ```

    The script will:

    - Extract data from the API and load it into the `raw_properties` table.
    - Transform and clean the data, then load it into the `transform_properties` table.
    - Log all actions and errors to `zipco_etl.log`.

## Project Structure
   
    zipco_real_estate_agency_pipeline/
    |
    |-- zipco_etl.py
    |-- zipco_etl.log
    |-- run_zipco_etl.bat
    |-- sql
        |-- zipco_insert_data.sql
        |-- zipco_queries.sql
        |-- zipco_table_creation.sql
    |-- .gitignore
    |-- requirement.txt
    |__ README.md

## Database Schema
The project follows a star schema:

Dimensions:

```
dim_location --> state, city, county, postal_code
dim_property --> property_type, bedrooms, bathrooms, square_footage, year_built, floor_count
dim_occupied --> owner_occupied, architecture
```

Fact Table:

```
property_facts --> full_address, latitude, longitude, legal_description, sub_division
```

Author:

If you like this project, please give this project a ⭐ as it will encourage me to produce more projects
