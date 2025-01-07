import csv
import mysql.connector
import configparser
import os

# Path to the configuration file
CONFIG_FILE = '/path/to/your/config.ini'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)

DB_CONFIG = {
    'host': config['mysql']['host'],
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'database': config['mysql']['database'],
}

# Path to the uploaded file
FILE_PATH = '/mnt/data/20250107_ISO10383_MIC.csv'

# Map CSV headers to database columns
CSV_TO_DB_MAPPING = {
    'MIC': 'MIC',
    'OPERATING MIC': 'OperatingMIC',
    'OPRT/SGMT': 'OPRTSGMT',
    'MARKET NAME-INSTITUTION DESCRIPTION': 'MarketName',
    'LEGAL ENTITY NAME': 'LegalEntity',
    'LEI': 'LEI',
    'MARKET CATEGORY CODE': 'MktCatCode',
    'ACRONYM': 'Acronym',
    'ISO COUNTRY CODE (ISO 3166)': 'ISOCountry',
    'CITY': 'City',
    'WEBSITE': 'Website',
    'STATUS': 'ExchStatus',
    'CREATION DATE': 'CreationDate',
    'LAST UPDATE DATE': 'MICLastUpdate',
    'LAST VALIDATION DATE': 'LastValidDate',
    'EXPIRY DATE': 'ExpiryDate',
    'COMMENTS': 'Comments',
}

# Function to insert data into the MICCodes table
def insert_into_miccodes(data):
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO MICCodes (
            MIC, OperatingMIC, OPRTSGMT, MarketName, LegalEntity, LEI, MktCatCode,
            Acronym, ISOCountry, City, Website, ExchStatus, CreationDate,
            MICLastUpdate, LastValidDate, ExpiryDate, Comments, InsertUser, FileName,
            DataSource
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for row in data:
            cursor.execute(insert_query, row)

        connection.commit()
        print(f"Inserted {cursor.rowcount} rows successfully.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Read the CSV file and map headers to database columns
def read_csv_and_insert():
    with open(FILE_PATH, 'r', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter='\t')
        data = []

        for row in csv_reader:
            mapped_row = [
                row.get(header, None) for header in CSV_TO_DB_MAPPING.keys()
            ]
            data.append(mapped_row + [
                config['general']['insert_user'],  # Retrieved from the config file
                '20250107_ISO10383_MIC.csv',
                'ISO10383'  # Replace with the appropriate data source
            ])

        insert_into_miccodes(data)

# Execute the function
if __name__ == "__main__":
    read_csv_and_insert()
