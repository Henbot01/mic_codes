import requests
from datetime import datetime
import os
import csv

# URL of the CSV file
url = "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"

# Get the current date in YYYYMMDD format
current_date = datetime.now().strftime("%Y%m%d")

# Output directory
output_directory = os.path.expanduser("~/downloads")
os.makedirs(output_directory, exist_ok=True)

# File name with date prefix
file_name = f"{current_date}_ISO10383_MIC.csv"
file_path = os.path.join(output_directory, file_name)

# Log file path
log_file_path = os.path.expanduser("~/log.csv")

# Function to log events
def log_event(status, message):
    with open(log_file_path, "a", newline="") as log_file:
        log_writer = csv.writer(log_file)
        log_writer.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), status, message])

try:
    # Download the file
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad status codes

    # Save the file
    with open(file_path, "wb") as file:
        file.write(response.content)

    print(f"File successfully downloaded and saved as {file_path}")
    log_event("SUCCESS", f"File downloaded and saved as {file_path}")

except requests.exceptions.RequestException as e:
    error_message = f"An error occurred while downloading the file: {e}"
    print(error_message)
    log_event("ERROR", error_message)