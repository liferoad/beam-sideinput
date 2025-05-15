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
mvn compile exec:java -Dexec.mainClass=com.example.SimpleBatchPipeline -Dexec.args="--runner=DirectRunner --projectId=manav-jit-test --datasetName=dummy_dataset_sideinput --tablePrefix=dummy-table --tempLocation=gs://tmp_xqhu/sideinput/"
```

**Explanation of Arguments:**

*   `--runner=DirectRunner`: Specifies that the pipeline should run locally.
*   `--projectId=manav-jit-test`: Your GCP project ID.
*   `--datasetName=dummy_dataset_sideinput`: The BigQuery dataset to use (must match the one created above).
*   `--tablePrefix=dummy-table`: A prefix for any tables that might be created in BigQuery.
*   `--tempLocation=gs://tmp_xqhu/sideinput/`: A GCS path for Beam to store temporary files. Ensure this bucket and path are accessible by your GCP user/service account.

This command will execute the pipeline, and you should see log output indicating its progress. Since it's using `DirectRunner`, the actual data processing happens on your local machine.
