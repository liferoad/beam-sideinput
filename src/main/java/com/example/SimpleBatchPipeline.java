package com.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

public class SimpleBatchPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleBatchPipeline.class);
    private static final Random RANDOM = new Random();

    // 1. Simple Data Class
    public static class MyData implements Serializable {
        String id;
        String name;
        double value;
        String category;

        public MyData(String id, String name, double value, String category) {
            this.id = id;
            this.name = name;
            this.value = value;
            this.category = category;
        }
        // Default constructor for Beam's Coder
        public MyData() {}


        @Override
        public String toString() {
            return "MyData{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", value=" + value +
                    ", category='" + category + '\'' +
                    '}';
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MyData myData = (MyData) o;
            return Double.compare(myData.value, value) == 0 && Objects.equals(id, myData.id) && Objects.equals(name, myData.name) && Objects.equals(category, myData.category);
        }
        @Override
        public int hashCode() {
            return Objects.hash(id, name, value, category);
        }
    }

    // 2. DoFn to transform MyData to TableRow, now with price enrichment
    public static class MyDataToTableRowFn extends DoFn<MyData, TableRow> {
        private final String createdBy;
        private final LocalDate snapshotDate;
        private final PCollectionView<Map<String, Double>> priceSideInput;

        public MyDataToTableRowFn(String createdBy, LocalDate snapshotDate, PCollectionView<Map<String, Double>> priceSideInput) {
            this.createdBy = createdBy;
            this.snapshotDate = snapshotDate;
            this.priceSideInput = priceSideInput;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            MyData data = c.element();
            Map<String, Double> priceMap = c.sideInput(priceSideInput);

            TableRow row = new TableRow();
            row.set("data_id", data.id);
            row.set("data_name", data.name);
            row.set("data_value", data.value);
            row.set("data_category", data.category);

            Double price = priceMap.getOrDefault(data.id, 0.0);
            row.set("data_price", price);

            row.set("created_by", createdBy);
            row.set("snapshot_dt", snapshotDate.format(DateTimeFormatter.ISO_LOCAL_DATE));
            c.output(row);
        }
    }

    // 3. Simple Dynamic Destination Logic
    public static class SimpleDynamicDestinations extends DynamicDestinations<TableRow, KV<String, String>> {
        private final String projectId;
        private final String datasetName;
        private final String tablePrefix;

        public SimpleDynamicDestinations(String projectId, String datasetName, String tablePrefix) {
            this.projectId = projectId;
            this.datasetName = datasetName;
            this.tablePrefix = tablePrefix;
        }

        @Override
        public KV<String, String> getDestination(ValueInSingleWindow<TableRow> element) {
            TableRow row = element.getValue();
            String category = "unknown_category"; // Default if "data_category" is not found or not a string
            if (row != null && row.get("data_category") instanceof String) {
                category = ((String) row.get("data_category")).replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
            }
            return KV.of(category, String.format("%s_%s", tablePrefix, category));
        }

        @Override
        public TableDestination getTable(KV<String, String> destination) {
            String tableName = destination.getValue();
            return new TableDestination(projectId + ":" + datasetName + "." + tableName,
                    "Table for category: " + destination.getKey());
        }

        @Override
        public TableSchema getSchema(KV<String, String> destination) {
            List<TableFieldSchema> fields = new ArrayList<>();
            fields.add(new TableFieldSchema().setName("data_id").setType("STRING").setMode("REQUIRED"));
            fields.add(new TableFieldSchema().setName("data_name").setType("STRING"));
            fields.add(new TableFieldSchema().setName("data_value").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("data_category").setType("STRING"));
            fields.add(new TableFieldSchema().setName("data_price").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("created_by").setType("STRING"));
            fields.add(new TableFieldSchema().setName("snapshot_dt").setType("DATE"));
            // Field added by withFormatFunction
            fields.add(new TableFieldSchema().setName("processed_timestamp").setType("TIMESTAMP"));
            return new TableSchema().setFields(fields);
        }
        @Override
        public Coder<KV<String, String>> getDestinationCoder() {
            return KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
        }
    }

    // 4. Basic Pipeline Options
    public interface SimplePipelineOptions extends PipelineOptions {
        @Description("GCP Project ID for BigQuery")
        @Validation.Required
        String getProjectId();
        void setProjectId(String value);

        @Description("BigQuery Dataset Name")
        @Validation.Required
        String getDatasetName();
        void setDatasetName(String value);

        @Description("BigQuery Table Name Prefix")
        @Validation.Required
        String getTablePrefix();
        void setTablePrefix(String value);

        @Description("Number of synthetic records to generate")
        @Default.Integer(100)
        Integer getNumRecords();
        void setNumRecords(Integer value);
    }

    public static void main(String[] args) {
        SimplePipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SimplePipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        int numRecords = options.getNumRecords();

        // Generate Synthetic Data for MyData
        List<MyData> dataItems = new ArrayList<>(numRecords);
        String[] categories = {"CAT1", "CAT2", "CAT3", "CAT4", "CAT5"};
        for (int i = 0; i < numRecords; i++) {
            String id = UUID.randomUUID().toString();
            String name = "Item-" + (i + 1);
            double value = RANDOM.nextDouble() * 1000; // Random value between 0 and 1000
            String category = categories[RANDOM.nextInt(categories.length)];
            dataItems.add(new MyData(id, name, value, category));
        }
        PCollection<MyData> syntheticData = pipeline.apply("CreateSyntheticData", Create.of(dataItems))
                .setCoder(SerializableCoder.of(MyData.class));

        // Generate Synthetic Price Information (Side Input)
        List<KV<String, Double>> priceItems = new ArrayList<>();
        // Create prices for a subset of the generated data items
        int numPriceRecords = Math.max(1, numRecords / 2); // Prices for about half the items
        for (int i = 0; i < numPriceRecords; i++) {
            if (i < dataItems.size()) { // Ensure we use existing IDs
                String idToPrice = dataItems.get(i).id;
                double price = Math.round((RANDOM.nextDouble() * 200 + 10) * 100.0) / 100.0; // Random price $10-$210
                priceItems.add(KV.of(idToPrice, price));
            }
        }
        // Add a few extra prices for IDs that might not be in the main dataItems list to simulate disjoint data
        for (int i = 0; i < numRecords / 10; i++) {
             priceItems.add(KV.of(UUID.randomUUID().toString(), Math.round((RANDOM.nextDouble() * 50 + 5) * 100.0) / 100.0));
        }

        PCollection<KV<String, Double>> priceInfo = pipeline.apply("CreatePriceData", Create.of(priceItems))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));

        PCollectionView<Map<String, Double>> priceSideInputView = priceInfo.apply("ViewPricesAsMap", View.asMap());

        PCollection<TableRow> tableRows = syntheticData.apply("ConvertToTableRows",
                ParDo.of(new MyDataToTableRowFn("SimpleBatchJob", LocalDate.now(), priceSideInputView))
                        .withSideInputs(priceSideInputView));

        tableRows.apply("WriteToBigQuery", BigQueryIO
                .<TableRow>write()
                .to(new SimpleDynamicDestinations(options.getProjectId(), options.getDatasetName(), options.getTablePrefix()))
                .withFormatFunction(row -> {
                    TableRow clonedRow = row.clone();
                    clonedRow.set("processed_timestamp", System.currentTimeMillis() / 1000.0); // Use double for BQ TIMESTAMP
                    return clonedRow;
                })
                .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        );

        LOG.info("Starting pipeline. To run against BigQuery, ensure Application Default Credentials are set " +
                 "or provide appropriate service account credentials. Also, ensure the BigQuery API is enabled.");
        LOG.info("Example command-line arguments: --projectId=your-gcp-project --datasetName=your_dataset --tablePrefix=my_data_table --numRecords=500");

        pipeline.run().waitUntilFinish();
        LOG.info("Pipeline finished.");
    }
}
