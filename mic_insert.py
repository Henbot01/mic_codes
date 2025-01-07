import csv
import mysql.connector
import configparser
import os
from datetime import datetime

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

# Path to the log file
LOG_FILE = '/path/to/log.csv'

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

# Function to log activity
def log_activity(message):
    with open(LOG_FILE, 'a', encoding='utf-8') as logfile:
        log_writer = csv.writer(logfile)
        log_writer.writerow([datetime.now().strftime('%Y-%m-%d %H:%M:%S'), message])

# Function to upsert data into the MICCodes table
def upsert_into_miccodes(data):
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        upsert_query = """
        INSERT INTO MICCodes (
            MIC, OperatingMIC, OPRTSGMT, MarketName, LegalEntity, LEI, MktCatCode,
            Acronym, ISOCountry, City, Website, ExchStatus, CreationDate,
            MICLastUpdate, LastValidDate, ExpiryDate, Comments, InsertUser, FileName,
            DataSource
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            OperatingMIC = VALUES(OperatingMIC),
            OPRTSGMT = VALUES(OPRTSGMT),
            MarketName = VALUES(MarketName),
            LegalEntity = VALUES(LegalEntity),
            LEI = VALUES(LEI),
            MktCatCode = VALUES(MktCatCode),
            Acronym = VALUES(Acronym),
            ISOCountry = VALUES(ISOCountry),
            City = VALUES(City),
            Website = VALUES(Website),
            ExchStatus = VALUES(ExchStatus),
            CreationDate = VALUES(CreationDate),
            MICLastUpdate = VALUES(MICLastUpdate),
            LastValidDate = VALUES(LastValidDate),
            ExpiryDate = VALUES(ExpiryDate),
            Comments = VALUES(Comments),
            InsertUser = VALUES(InsertUser),
            FileName = VALUES(FileName),
            DataSource = VALUES(DataSource)
        """

        for row in data:
            cursor.execute(upsert_query, row)

        connection.commit()
        log_activity(f"Upserted {cursor.rowcount} rows successfully.")

    except mysql.connector.Error as err:
        log_activity(f"Error: {err}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Read the CSV file and map headers to database columns
def read_csv_and_insert():
    try:
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

            upsert_into_miccodes(data)

    except Exception as e:
        log_activity(f"Failed to process file {FILE_PATH}: {e}")

# Execute the function
if __name__ == "__main__":
    log_activity("Script execution started.")
    read_csv_and_insert()
    log_activity("Script execution finished.")
