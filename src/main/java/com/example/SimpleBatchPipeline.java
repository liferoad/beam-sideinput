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
import org.apache.beam.sdk.io.TextIO;
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

        @Description("GCS path for input data files (e.g., gs://bucket/path/to/data*.csv)")
        @Default.String("")
        String getDataInputGcsPath();
        void setDataInputGcsPath(String value);

        @Description("GCS path for input price files (e.g., gs://bucket/path/to/prices*.csv)")
        @Default.String("")
        String getPriceInputGcsPath();
        void setPriceInputGcsPath(String value);
    }

    // DoFn to parse MyData from CSV
    public static class ParseMyDataFromCsvFn extends DoFn<String, MyData> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<MyData> out) {
            String[] parts = line.split(",");
            if (parts.length == 4) {
                try {
                    String id = parts[0].trim();
                    String name = parts[1].trim();
                    double value = Double.parseDouble(parts[2].trim());
                    String category = parts[3].trim();
                    out.output(new MyData(id, name, value, category));
                } catch (NumberFormatException e) {
                    LOG.warn("Skipping malformed line for MyData: " + line, e);
                }
            } else {
                LOG.warn("Skipping malformed line for MyData (incorrect number of fields): " + line);
            }
        }
    }

    // DoFn to parse PriceItem from CSV
    public static class ParsePriceItemFromCsvFn extends DoFn<String, KV<String, Double>> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<KV<String, Double>> out) {
            String[] parts = line.split(",");
            if (parts.length == 2) {
                try {
                    String id = parts[0].trim();
                    double price = Double.parseDouble(parts[1].trim());
                    out.output(KV.of(id, price));
                } catch (NumberFormatException e) {
                    LOG.warn("Skipping malformed line for PriceItem: " + line, e);
                }
            } else {
                LOG.warn("Skipping malformed line for PriceItem (incorrect number of fields): " + line);
            }
        }
    }


    public static void main(String[] args) {
        SimplePipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SimplePipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<MyData> myDataPCollection;
        if (options.getDataInputGcsPath() != null && !options.getDataInputGcsPath().isEmpty()) {
            LOG.info("Reading MyData from GCS path: " + options.getDataInputGcsPath());
            myDataPCollection = pipeline.apply("ReadMyDataFromGCS", TextIO.read().from(options.getDataInputGcsPath()))
                                     .apply("ParseMyData", ParDo.of(new ParseMyDataFromCsvFn()))
                                     .setCoder(SerializableCoder.of(MyData.class));
        } else {
            LOG.info("Generating synthetic MyData.");
            int numRecords = options.getNumRecords();
            List<MyData> dataItems = new ArrayList<>(numRecords);
            String[] categories = {"CAT1", "CAT2", "CAT3", "CAT4", "CAT5"};
            for (int i = 0; i < numRecords; i++) {
                String id = UUID.randomUUID().toString();
                String name = "Item-" + (i + 1);
                double value = RANDOM.nextDouble() * 1000; // Random value between 0 and 1000
                String category = categories[RANDOM.nextInt(categories.length)];
                dataItems.add(new MyData(id, name, value, category));
            }
            myDataPCollection = pipeline.apply("CreateSyntheticData", Create.of(dataItems))
                    .setCoder(SerializableCoder.of(MyData.class));
        }

        PCollection<KV<String, Double>> priceInfoPCollection;
        if (options.getPriceInputGcsPath() != null && !options.getPriceInputGcsPath().isEmpty()) {
            LOG.info("Reading PriceInfo from GCS path: " + options.getPriceInputGcsPath());
            priceInfoPCollection = pipeline.apply("ReadPriceInfoFromGCS", TextIO.read().from(options.getPriceInputGcsPath()))
                                        .apply("ParsePriceInfo", ParDo.of(new ParsePriceItemFromCsvFn()))
                                        .setCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));
        } else {
            LOG.info("Generating synthetic PriceInfo.");
            int numRecords = options.getNumRecords(); // Use same numRecords for consistency if generating
            List<KV<String, Double>> priceItems = new ArrayList<>();
            // For synthetic price generation, we need access to the IDs from myDataPCollection if it was also synthetic.
            // This part is tricky if MyData is from GCS and PriceInfo is synthetic or vice-versa.
            // For simplicity, if PriceInfo is synthetic, we'll generate random IDs or assume MyData was also synthetic.
            // A more robust solution would involve joining or ensuring ID consistency.

            // If myDataPCollection was generated synthetically, we can try to use its items.
            // However, myDataPCollection might not be materialized here.
            // So, we'll generate independent synthetic prices if GCS path for data is not given.
            // If data path IS given, this synthetic price generation might not align well.
            // This example assumes if one is GCS, the other might be too, or synthetic prices are okay with random IDs.

            List<String> dataItemIdsForPricing = new ArrayList<>();
            if (options.getDataInputGcsPath() == null || options.getDataInputGcsPath().isEmpty()) {
                // If data is synthetic, we can generate prices based on those synthetic items.
                // This requires access to the generated dataItems list, which is in the 'else' block above.
                // To simplify, we'll re-generate some random IDs for pricing here if data is synthetic.
                // This is not ideal as it won't match the actual synthetic data IDs perfectly.
                // A better approach for fully synthetic data would be to generate dataItems first, then prices from those.
                for (int i = 0; i < numRecords; i++) {
                    dataItemIdsForPricing.add(UUID.randomUUID().toString()); // Placeholder IDs
                }
            }


            int numPriceRecords = Math.max(1, numRecords / 2);
            for (int i = 0; i < numPriceRecords; i++) {
                String idToPrice;
                if (!dataItemIdsForPricing.isEmpty() && i < dataItemIdsForPricing.size()) {
                    idToPrice = dataItemIdsForPricing.get(i);
                } else {
                     // Fallback if dataItemIdsForPricing is empty (e.g. data came from GCS)
                    idToPrice = UUID.randomUUID().toString();
                }
                double price = Math.round((RANDOM.nextDouble() * 200 + 10) * 100.0) / 100.0;
                priceItems.add(KV.of(idToPrice, price));
            }
            // Add a few extra prices for IDs that might not be in the main dataItems list
            for (int i = 0; i < numRecords / 10; i++) {
                 priceItems.add(KV.of(UUID.randomUUID().toString(), Math.round((RANDOM.nextDouble() * 50 + 5) * 100.0) / 100.0));
            }
            priceInfoPCollection = pipeline.apply("CreatePriceData", Create.of(priceItems))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), DoubleCoder.of()));
        }

        PCollectionView<Map<String, Double>> priceSideInputView = priceInfoPCollection.apply("ViewPricesAsMap", View.asMap());

        PCollection<TableRow> tableRows = myDataPCollection.apply("ConvertToTableRows",
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
        LOG.info("Example command-line arguments: --projectId=your-gcp-project --datasetName=your_dataset --tablePrefix=my_data_table --numRecords=500 " +
                 "[--dataInputGcsPath=gs://your-bucket/data-*.csv] [--priceInputGcsPath=gs://your-bucket/prices-*.csv]");

        pipeline.run().waitUntilFinish();
        LOG.info("Pipeline finished.");
    }
}
