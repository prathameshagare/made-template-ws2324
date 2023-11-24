import pandas as pd
import requests

# Function to download the dataset
def download_dataset(url, file_name):
    response = requests.get(url)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        with open(file_name, 'wb') as file:
            file.write(response.content)
        print(f"Dataset downloaded successfully as {file_name}")
    else:
        print(f"Failed to download dataset. Status code: {response.status_code}")

# Function to perform data transformations and fix errors
def transform_and_fix_errors(input_file, output_file):
    # Read the dataset into a DataFrame
    df = pd.read_csv(input_file)

    # Perform your data transformations and error fixes here
    # Example: Fixing missing values
    df.fillna(value=0, inplace=True)

    # Save the transformed dataset
    df.to_csv(output_file, index=False)
    print(f"Transformed dataset saved as {output_file}")

# Function to perform data cleaning
def clean_dataset(input_file, output_file):
    # Read the dataset into a DataFrame
    df = pd.read_csv(input_file)

    # Perform data cleaning operations
    # Example: Drop duplicates
    df.drop_duplicates(inplace=True)

    # Save the cleaned dataset
    df.to_csv(output_file, index=False)
    print(f"Cleaned dataset saved as {output_file}")

if __name__ == "__main__":
    # URLs of the datasets to download
    death_road_accident = "https://www.landesdatenbank.nrw.de/ldbnrwws/downloader/00/tables/23211-03d_00.csv"
    accident = "https://www.landesdatenbank.nrw.de/ldbnrwws/downloader/00/tables/46241-06i_00.csv"

    # Local file names
    death_road_accident_data = "death_road_accident_data.csv"
    accident_data = "accident_data.csv"
    death_road_accident_raw_data = "death_road_accident_data_raw.csv"
    accident_raw_data = "accident_data_raw.csv"
    modified_data_1 = "cleaned_dataset1.csv"
    modified_data_2 = "cleaned_dataset2.csv"

    # Step 1: Download the datasets
    download_dataset(death_road_accident, death_road_accident_data)
    download_dataset(accident, accident_data)

    # Step 2: Transform the datasets and fix errors
    transform_and_fix_errors(death_road_accident_data, death_road_accident_raw_data)
    transform_and_fix_errors(accident_data, accident_raw_data)

    # Step 3: Clean the datasets
    clean_dataset(death_road_accident_raw_data, modified_data_1)
    clean_dataset(accident_raw_data, modified_data_2)
