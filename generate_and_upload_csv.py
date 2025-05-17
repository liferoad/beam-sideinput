import argparse
import csv
import os
import random
import shutil
import uuid
from google.cloud import storage

MAX_RECORDS_PER_FILE = 1000
LOCAL_TEMP_DIR = "generated_csvs"

CATEGORIES = ["CAT1", "CAT2", "CAT3", "CAT4", "CAT5"]

def generate_mydata_record(record_id):
    """Generates a single MyData record."""
    name = f"Item-{record_id.split('-')[0]}" # Use a portion of UUID for name
    value = round(random.uniform(1.0, 1000.0), 2)
    category = random.choice(CATEGORIES)
    return [record_id, name, value, category]

def generate_price_record(record_id):
    """Generates a single Price record."""
    price = round(random.uniform(10.0, 210.0), 2)
    return [record_id, price]

def write_csv_batch(writer, header, records_generator_func, ids_batch):
    """Writes a batch of records to a CSV file."""
    writer.writerow(header)
    for record_id in ids_batch:
        writer.writerow(records_generator_func(record_id))

def upload_blob(bucket_name, source_file_name, destination_blob_name, storage_client):
    """Uploads a file to the bucket."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")
    except Exception as e:
        print(f"Error uploading {source_file_name} to {bucket_name}/{destination_blob_name}: {e}")
        raise

def main(num_total_records, data_bucket_name, data_gcs_prefix, price_bucket_name, price_gcs_prefix):
    """Generates CSV files and uploads them to GCS."""

    if os.path.exists(LOCAL_TEMP_DIR):
        shutil.rmtree(LOCAL_TEMP_DIR)
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)

    storage_client = storage.Client()

    all_generated_ids = [str(uuid.uuid4()) for _ in range(num_total_records)]
    data_file_counter = 0
    price_file_counter = 0

    # Generate and upload Data CSV files
    print(f"\nGenerating and uploading Data CSV files to gs://{data_bucket_name}/{data_gcs_prefix}")
    for i in range(0, num_total_records, MAX_RECORDS_PER_FILE):
        batch_ids = all_generated_ids[i:i + MAX_RECORDS_PER_FILE]
        if not batch_ids:
            continue

        data_file_counter += 1
        local_file_path = os.path.join(LOCAL_TEMP_DIR, f"data_batch_{data_file_counter}.csv")
        destination_blob_name = os.path.join(data_gcs_prefix, f"data_batch_{data_file_counter}.csv").replace("\\", "/") # Ensure forward slashes for GCS

        with open(local_file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            write_csv_batch(writer, ["id", "name", "value", "category"], generate_mydata_record, batch_ids)

        upload_blob(data_bucket_name, local_file_path, destination_blob_name, storage_client)

    # Generate and upload Price CSV files
    # Prices for about 70% of the items, plus some extra random ones
    num_price_records_from_data = int(num_total_records * 0.7)
    num_extra_price_records = int(num_total_records * 0.1)

    price_ids = random.sample(all_generated_ids, min(num_price_records_from_data, len(all_generated_ids)))
    for _ in range(num_extra_price_records):
        price_ids.append(str(uuid.uuid4())) # Add some IDs not in the original data set

    random.shuffle(price_ids) # Shuffle to mix them up

    print(f"\nGenerating and uploading Price CSV files to gs://{price_bucket_name}/{price_gcs_prefix}")
    for i in range(0, len(price_ids), MAX_RECORDS_PER_FILE):
        batch_price_ids = price_ids[i:i + MAX_RECORDS_PER_FILE]
        if not batch_price_ids:
            continue

        price_file_counter += 1
        local_file_path = os.path.join(LOCAL_TEMP_DIR, f"price_batch_{price_file_counter}.csv")
        destination_blob_name = os.path.join(price_gcs_prefix, f"price_batch_{price_file_counter}.csv").replace("\\", "/") # Ensure forward slashes

        with open(local_file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            write_csv_batch(writer, ["id", "price"], generate_price_record, batch_price_ids)

        upload_blob(price_bucket_name, local_file_path, destination_blob_name, storage_client)

    # Clean up local temporary directory
    shutil.rmtree(LOCAL_TEMP_DIR)
    print(f"\nCleaned up local directory: {LOCAL_TEMP_DIR}")
    print("\nScript finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate CSV files for data and prices and upload them to GCS.")
    parser.add_argument("--num_records", type=int, required=True, help="Total number of records to generate.")
    parser.add_argument("--data_bucket", type=str, required=True, help="GCS bucket name for data files.")
    parser.add_argument("--data_prefix", type=str, default="", help="GCS prefix (folder) for data files (e.g., 'input/data/'). Ensure it ends with a '/' if it's a folder.")
    parser.add_argument("--price_bucket", type=str, required=True, help="GCS bucket name for price files.")
    parser.add_argument("--price_prefix", type=str, default="", help="GCS prefix (folder) for price files (e.g., 'input/prices/'). Ensure it ends with a '/' if it's a folder.")

    args = parser.parse_args()

    # Ensure prefixes end with a slash if they are not empty and not already ending with one
    data_prefix = args.data_prefix
    if data_prefix and not data_prefix.endswith('/'):
        data_prefix += '/'

    price_prefix = args.price_prefix
    if price_prefix and not price_prefix.endswith('/'):
        price_prefix += '/'

    print("Starting CSV generation and upload process...")
    print(f"Number of records: {args.num_records}")
    print(f"Data GCS Target: gs://{args.data_bucket}/{data_prefix}")
    print(f"Price GCS Target: gs://{args.price_bucket}/{price_prefix}")
    print("---")
    print("NOTE: Ensure you have authenticated with GCP and the google-cloud-storage library is installed ('pip install google-cloud-storage').")
    print("The script will create a local temporary directory './generated_csvs' and remove it upon completion.")
    print("---")


    main(args.num_records, args.data_bucket, data_prefix, args.price_bucket, price_prefix)

# pip install google-cloud-storage
# Example usage:
# python generate_and_upload_csv.py \
#   --num_records 10000 \
#   --data_bucket your-data-bucket-name \
#   --data_prefix input/data/ \
#   --price_bucket your-price-bucket-name \
#   --price_prefix input/prices/
