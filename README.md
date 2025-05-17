# beam-sideinput
Test SideInput with Runner V2

## Building the Project

To build the project, run the following Maven command:

```bash
mvn clean install
```

## Running with DirectRunner (Local Test)

This section describes how to run the pipeline locally using the DirectRunner for testing purposes. Even though it runs locally, the pipeline is configured to interact with BigQuery, so some GCP setup is still required.

### 1. Prerequisites

Ensure you have the Google Cloud SDK installed and configured.

### 2. Set GCP Project and Create BigQuery Dataset

The pipeline requires a GCP project ID and a BigQuery dataset. If you haven't already, set your active GCP project and create the necessary dataset.

**Note:** BigQuery dataset IDs must be alphanumeric and can include underscores (`_`), but not hyphens (`-`).

```bash
# Set your active GCP project (replace 'manav-jit-test' if different)
gcloud config set project manav-jit-test

# Create the BigQuery dataset (replace 'dummy_dataset_sideinput' if you prefer a different name)
bq mk dummy_dataset_sideinput
```

If the dataset already exists, the `bq mk` command will indicate that.

### 3. Run the Pipeline with DirectRunner

Execute the following Maven command to compile and run the pipeline:

```bash
mvn compile exec:java -Dexec.mainClass=com.example.SimpleBatchPipeline -Dexec.args="--runner=DirectRunner --projectId=manav-jit-test --datasetName=dummy_dataset_sideinput --tablePrefix=dummy-table --tempLocation=gs://tmp_xqhu/sideinput/ --numRecords=10"
```

**Explanation of Arguments:**

*   `--runner=DirectRunner`: Specifies that the pipeline should run locally.
*   `--projectId=manav-jit-test`: Your GCP project ID.
*   `--datasetName=dummy_dataset_sideinput`: The BigQuery dataset to use (must match the one created above).
*   `--tablePrefix=dummy-table`: A prefix for any tables that might be created in BigQuery.
*   `--tempLocation=gs://tmp_xqhu/sideinput/`: A GCS path for Beam to store temporary files. Ensure this bucket and path are accessible by your GCP user/service account.

This command will execute the pipeline, and you should see log output indicating its progress. Since it's using `DirectRunner`, the actual data processing happens on your local machine.

## Generating Test Data with Python Script

A Python script `generate_and_upload_csv.py` is provided to create sample CSV files for data and price items and upload them to Google Cloud Storage. This can be useful for populating GCS paths that the Beam pipeline can then read from when using the GCS input options.

### 1. Prerequisites for the Python Script

*   Python 3.x installed.
*   The `google-cloud-storage` Python library. Install it using pip:
    ```bash
    pip install google-cloud-storage
    ```
*   Authenticated with Google Cloud. The script uses Application Default Credentials. If you haven't already, you might need to run:
    ```bash
    gcloud auth application-default login
    ```

### 2. Running the Python Script

Execute the script from your terminal, providing the required arguments.

**Example Command:**

```bash
python generate_and_upload_csv.py \
  --num_records 5000000 \
  --data_bucket tmp_xqhu \
  --data_prefix input/data/ \
  --price_bucket tmp_xqhu \
  --price_prefix input/prices/
```

**Explanation of Arguments:**

*   `--num_records`: Total number of data records to generate. Price records will be generated based on this number.
*   `--data_bucket`: The GCS bucket name where data CSV files will be uploaded (e.g., `tmp_xqhu`).
*   `--data_prefix`: The GCS prefix (folder path) within the data bucket (e.g., `input/data/`).
*   `--price_bucket`: The GCS bucket name where price CSV files will be uploaded (e.g., `tmp_xqhu`).
*   `--price_prefix`: The GCS prefix (folder path) within the price bucket (e.g., `input/prices/`).

The script will create CSV files locally in a `generated_csvs` directory (which it cleans up afterwards) and upload them in batches to the specified GCS locations. Each batch file will contain up to 1000 records.

Once the files are uploaded, you can use their GCS paths (e.g., `gs://tmp_xqhu/input/data/data_batch_*.csv` and `gs://tmp_xqhu/input/prices/price_batch_*.csv`) with the Beam pipeline's `--dataInputGcsPath` and `--priceInputGcsPath` options.

## Running with GCS Data Source

This section provides examples of how to run the Beam pipeline configured to read data and price information directly from CSV files stored in Google Cloud Storage. This is an alternative to generating synthetic data within the pipeline. Ensure you have first generated and uploaded these files to GCS (e.g., using the Python script described above).

### 1. Using DirectRunner with GCS Input

Execute the following Maven command to compile and run the pipeline locally, reading from GCS:

```bash
mvn compile exec:java -Dexec.mainClass=com.example.SimpleBatchPipeline -Dexec.args="--runner=DirectRunner --projectId=manav-jit-test --datasetName=dummy_dataset_sideinput --tablePrefix=gcs_input_table --tempLocation=gs://tmp_xqhu/sideinput/ --dataInputGcsPath=gs://tmp_xqhu/input/data/data_batch_*.csv --priceInputGcsPath=gs://tmp_xqhu/input/prices/price_batch_*.csv"
```

**Key GCS Input Arguments:**

*   `--dataInputGcsPath=gs://tmp_xqhu/input/data/data_batch_*.csv`: Specifies the GCS path pattern for the input data CSV files. The `*` acts as a wildcard.
*   `--priceInputGcsPath=gs://tmp_xqhu/input/prices/price_batch_*.csv`: Specifies the GCS path pattern for the input price CSV files.
*   The `--numRecords` argument is not strictly necessary here for data generation if GCS paths are valid and contain files, as the pipeline will prioritize reading from GCS.

All other arguments (`--projectId`, `--datasetName`, `--tablePrefix`, `--tempLocation`) are similar to the synthetic data example.

### 2. Using DataflowRunner with GCS Input

Execute the following Maven command to compile and run the pipeline on Dataflow, reading from GCS:

```bash
mvn compile exec:java -Dexec.vmArgs="-Xmx32g" -Dexec.mainClass=com.example.SimpleBatchPipeline -Dexec.args="--runner=DataflowRunner --projectId=manav-jit-test --region=us-central1 --tempLocation=gs://tmp_xqhu/sideinput/ --datasetName=dummy_dataset_sideinput --tablePrefix=gcs_input_table_df --dataInputGcsPath=gs://tmp_xqhu/input/data/data_batch_*.csv --priceInputGcsPath=gs://tmp_xqhu/input/prices/price_batch_*.csv"
```

**Key GCS Input Arguments:**

*   `--dataInputGcsPath=gs://tmp_xqhu/input/data/data_batch_*.csv`: GCS path pattern for input data files.
*   `--priceInputGcsPath=gs://tmp_xqhu/input/prices/price_batch_*.csv`: GCS path pattern for input price files.
*   Again, `--numRecords` is optional if GCS files are the intended source.

This command submits the pipeline to Google Cloud Dataflow, configured to read its main and side inputs from the specified GCS locations.

## Running with DataflowRunner (Google Cloud)

This section describes how to run the pipeline on Google Cloud Dataflow.

### 1. Prerequisites

*   Ensure you have the Google Cloud SDK installed and configured.
*   Ensure your `pom.xml` is configured with the `beam-runners-dataflow-java` dependency and a valid Apache Beam version (e.g., using the Beam BOM).
*   Successfully build the project using `mvn clean install`.
*   Authenticate with Google Cloud: `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS`.
*   Ensure the GCS bucket for `tempLocation` (e.g., `gs://tmp_xqhu/sideinput/`) exists and is writable.
*   Ensure the BigQuery dataset (e.g., `dummy_dataset_sideinput`) exists in your GCP project.

### 2. Run the Pipeline with DataflowRunner

Execute the following Maven command to compile and run the pipeline on Dataflow:

```bash
mvn compile exec:java -Dexec.vmArgs="-Xmx32g" -Dexec.mainClass=com.example.SimpleBatchPipeline -Dexec.args="--runner=DataflowRunner --projectId=manav-jit-test --region=us-central1 --tempLocation=gs://tmp_xqhu/sideinput/ --datasetName=dummy_dataset_sideinput --tablePrefix=dummy-table --numRecords=50000"
```

**Explanation of Arguments:**

*   `--runner=DataflowRunner`: Specifies that the pipeline should run on Google Cloud Dataflow.
*   `--projectId=manav-jit-test`: Your GCP project ID.
*   `--region=us-central1`: The GCP region where the Dataflow job will run.
*   `--tempLocation=gs://tmp_xqhu/sideinput/`: A GCS path for Dataflow to stage temporary files and the pipeline JAR.
*   `--datasetName=dummy_dataset_sideinput`: The BigQuery dataset to use.
*   `--tablePrefix=dummy-table`: A prefix for any tables that might be created in BigQuery.

This command will submit the pipeline to Google Cloud Dataflow. You can monitor its progress in the GCP console.

### 3. Verifying Output Record Counts (Example)

After the Dataflow job has successfully completed, and before cleaning up the dataset, you can verify the total number of records written to the BigQuery tables. The `SimpleBatchPipeline` generates a number of records specified by the `--numRecords` pipeline option (defaulting to 100) and writes them to tables named `[tablePrefix]_[category]`.

Assuming the default categories (`CAT1` to `CAT5`), project `manav-jit-test`, dataset `dummy_dataset_sideinput`, and table prefix `dummy-table`, you can use the following `bq query` command to get the sum of records across these tables:

```bash
bq query --nouse_legacy_sql \
'SELECT SUM(total_rows) AS grand_total_rows FROM (
  SELECT COUNT(*) AS total_rows FROM `manav-jit-test.dummy_dataset_sideinput.dummy-table_cat1` UNION ALL
  SELECT COUNT(*) AS total_rows FROM `manav-jit-test.dummy_dataset_sideinput.dummy-table_cat2` UNION ALL
  SELECT COUNT(*) AS total_rows FROM `manav-jit-test.dummy_dataset_sideinput.dummy-table_cat3` UNION ALL
  SELECT COUNT(*) AS total_rows FROM `manav-jit-test.dummy_dataset_sideinput.dummy-table_cat4` UNION ALL
  SELECT COUNT(*) AS total_rows FROM `manav-jit-test.dummy_dataset_sideinput.dummy-table_cat5`
)
'
```

For GCS sources,

```bash
bq query --nouse_legacy_sql \
'SELECT SUM(total_rows) AS grand_total_rows FROM (
  SELECT COUNT(*) AS total_rows FROM `manav-jit-test.dummy_dataset_sideinput.gcs_input_table_df_cat1` UNION ALL
  SELECT COUNT(*) AS total_rows FROM `manav-jit-test.dummy_dataset_sideinput.gcs_input_table_df_cat2` UNION ALL
  SELECT COUNT(*) AS total_rows FROM `manav-jit-test.dummy_dataset_sideinput.gcs_input_table_df_cat3` UNION ALL
  SELECT COUNT(*) AS total_rows FROM `manav-jit-test.dummy_dataset_sideinput.gcs_input_table_df_cat4` UNION ALL
  SELECT COUNT(*) AS total_rows FROM `manav-jit-test.dummy_dataset_sideinput.gcs_input_table_df_cat5`
)
'
```

The output `grand_total_rows` should match the `--numRecords` value used for the pipeline run.

**Note:**
*   If the pipeline used different categories, or if any category had no data, you might need to adjust the table names in the query or handle potential "table not found" errors.
*   This query assumes the dataset and tables exist. If they have been cleaned up, the query will fail.

## Cleaning Up

To remove the `dummy_dataset_sideinput` BigQuery dataset created for testing, you can use the following `bq` command. This command will delete the dataset and all tables within it from the `manav-jit-test` project.

```bash
bq rm -r -f --dataset manav-jit-test:dummy_dataset_sideinput
```

**Explanation of Arguments:**

*   `bq rm`: The BigQuery command to remove (delete) a resource.
*   `-r`: The recursive flag. If the dataset contains tables, this flag is required to delete the dataset and its contents.
*   `-f`: The force flag. This skips interactive confirmation prompts.
*   `--dataset manav-jit-test:dummy_dataset_sideinput`: Specifies the dataset to delete, using the format `PROJECT_ID:DATASET_ID`.

**Caution:** This command permanently deletes the dataset and its data. Ensure you are targeting the correct dataset in the correct project.
