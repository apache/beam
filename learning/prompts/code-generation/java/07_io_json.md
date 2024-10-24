Prompt:
Write a sample Java code snippet that writes data to a JSON file using Apache Beam.

Response:
Your Apache Beam pipeline can write data to JSON files using the Apache Beam JsonIO connector.

Here is an example of how to use the JsonIO connector to accomplish this:

```java
package gcs;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.json.JsonIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.Create;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

// Pipeline for writing data to JSON files using the `JsonIO.write()` transform.
public class WriteJsonIO {

    // Represents an Example JSON record.
    @DefaultSchema(JavaFieldSchema.class)
    public static class SampleRecord implements Serializable {
        public int id;
        public String month;
        public String amount

        public SampleRecord() {
        }

        public SampleRecord(int id, String month, String amount) {
            this.id = id;
            this.month = month;
            this.amount = amount;
        }
    }

    // Pipeline options for writing to JSON files.
    public interface WriteJsonOptions extends PipelineOptions {

        @Description("A file path to write JSON files to")
        @Validation.Required
        String getFilePath();

        // Set the file path.
        void setFilePath(String filePath);
    }

    // Main entry point.
    public static void main(String[] args) {

        // Get the pipeline options.
        WriteJsonOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation().as(WriteJsonOptions.class);

        // Create the pipeline.
        Pipeline p = Pipeline.create(options);

        // Create a list of SampleRecord objects.
        List<SampleRecord> rows =
            Arrays.asList(
                new SampleRecord(1, "January", "$1000"),
                new SampleRecord(2, "February", "$2000"),
                new SampleRecord(3, "March", "$3000"));

        // Write the records to JSON files
        p.apply("Create Records", Create.of(rows))
            .apply(
                "Write Records to JSON File",
                    JsonIO.<SampleRecord>write(options.getFilePath())
                        .withNumShards(1));
        // Run the pipeline.
        p.run();
    }
}
```

This code snippet utilizes the pipeline options pattern to parse command-line arguments.
