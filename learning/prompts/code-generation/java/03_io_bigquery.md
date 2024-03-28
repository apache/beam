Prompt:
Write a sample Java code snippet that writes data to a Google BigQuery table using Apache Beam.

Response:
Your Apache Beam pipeline can write data to a BigQuery table using the Apache Beam BigQueryIO connector.

Here is an example of how to use the BigQueryIO connector to accomplish this:

```java
 package bigquery;
 import com.google.api.services.bigquery.model.TableFieldSchema;
 import com.google.api.services.bigquery.model.TableRow;
 import com.google.api.services.bigquery.model.TableSchema;
 import java.util.ArrayList;
 import java.util.Arrays;
 import java.util.List;
 import org.apache.beam.sdk.Pipeline;
 import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
 import org.apache.beam.sdk.options.Description;
 import org.apache.beam.sdk.options.PipelineOptions;
 import org.apache.beam.sdk.options.PipelineOptionsFactory;
 import org.apache.beam.sdk.transforms.Create;
 import org.apache.beam.sdk.transforms.DoFn;
 import org.apache.beam.sdk.transforms.ParDo;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

// Pipeline to write data to a BigQuery table using Apache Beam
 public class WriteBigQueryTable {

     private static final Logger LOG = LoggerFactory.getLogger(WriteBigQueryTable.class);

    // Pipeline options to configure the pipeline
     public interface WriteBigQueryTableOptions extends PipelineOptions {
         @Description("Table name in BigQuery to write to")
         String getTableName();

         void setTableName(String value);
     }

     public static void main(String[] args) {

        // Parse the pipeline options from the command line
        WriteBigQueryTableOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteBigQueryTableOptions.class);

        // Sample data to write to the BigQuery table
         final List<String> elements = Arrays.asList(
                 "1, January, $1000",
                 "2, February, $2000",
                 "3, March, $3000"
         );

        // Define the schema for the BigQuery table
         List<TableFieldSchema> fields = new ArrayList<>();
         fields.add(new TableFieldSchema().setName("id").setType("INTEGER"));
         fields.add(new TableFieldSchema().setName("month").setType("STRING"));
         fields.add(new TableFieldSchema().setName("amount").setType("STRING"));
         TableSchema schema = new TableSchema().setFields(fields);

        // Create a pipeline using the Apache Beam SDK
         Pipeline p = Pipeline.create(options);

         p
                // Create a list of strings to write to the BigQuery table
                 .apply(Create.of(elements))
                 // Convert the strings to TableRow objects
                 .apply("to BigQuery TableRow", ParDo.of(new DoFn<String, TableRow>() {
                     @ProcessElement
                     public void processElement(ProcessContext c) {
                         String[] columns = c.element().split(", ");
                        // Create a TableRow object for each row in the input string
                         TableRow row = new TableRow();

                        // Set the values for each column in the TableRow object
                         row.set("id", columns[0]);
                         row.set("month", columns[1]);
                         row.set("amount", columns[2]);
                        // Emit the TableRow object to the output PCollection
                         c.output(row);
                     }
                 }))
                 // Write the data to the BigQuery table using the BigQueryIO connector
                 .apply(BigQueryIO.writeTableRows()
                         .withSchema(schema)
                        // Set the table name to write to
                         .to(options.getTableName())
                         .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                         .withNumFileShards(1000)
                         .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                         .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        // Execute the pipeline
         p.run();
     }
 }
```

This code snippet utilizes the pipeline options pattern to parse command-line arguments.
