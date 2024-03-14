Prompt:
Write the Java code to write data to JSON files using Apache Beam.
Response:

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

/**
 * Pipeline for writing data to JSON files using the {@code JsonIO.write()} transform.
 */
public class WriteJsonIO {

    /** Represents an Example JSON record. */
    @DefaultSchema(JavaFieldSchema.class)
    public static class ExampleRecord implements Serializable {
        public int id;
        public String name;

        public ExampleRecord() {
        }

        public ExampleRecord(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    /**
     * Pipeline options for write to JSON files.
     */
    public interface WriteJsonOptions extends PipelineOptions {

        @Description("A file path prefix to write JSON files to")
        @Validation.Required
        String getFilePathPrefix();

        void setFilePathPrefix(String filePathPrefix);
    }

    public static void main(String[] args) {
        WriteJsonOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation().as(WriteJsonOptions.class);

        Pipeline p = Pipeline.create(options);

        List<ExampleRecord> rows =
            Arrays.asList(
                new ExampleRecord(1, "Charles"),
                new ExampleRecord(2, "Alice"),
                new ExampleRecord(3, "Bob"),
                new ExampleRecord(4, "Amanda"),
                new ExampleRecord(5, "Alex"),
                new ExampleRecord(6, "Eliza"));

        p.apply("Create", Create.of(rows))
            .apply(
                "Write to JSON",
                    JsonIO.<ExampleRecord>write(options.getFilePathPrefix())
                        .withNumShards(1));
        p.run();
    }
}
```