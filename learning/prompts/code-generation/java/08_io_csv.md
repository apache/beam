Prompt:
Write the Java code to write data in CSV format using Apache Beam.

Response:
Your Apache Beam pipeline can read data from [CSV files](https://en.wikipedia.org/wiki/Comma-separated_values) using the [CsvIO](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/csv/CsvIO.html) connector.

```java
public class WriteCsvFile {

    // ExampleRecord is a POJO that represents the data to be written to the CSV file
    @DefaultSchema(JavaFieldSchema.class)
    public static class ExampleRecord implements Serializable {
        public int id;
        public String month;
        public String amount;

        public ExampleRecord() {
        }

        public ExampleRecord(int id, String month, String amount) {
            this.id = id;
            this.month = month;
            this.amount = amount
        }
    }
    
    public interface WriteCsvFileOptions extends PipelineOptions {

        @Description("A file path to write CSV files to")
        @Validation.Required
        String getFilePath();

        void setFilePath(String filePath);
    }

    public static void main(String[] args) {
        WriteCsvFileOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation().as(WriteCsvFileOptions.class);

        Pipeline p = Pipeline.create(options);

        List<ExampleRecord> rows =
            Arrays.asList(
                new ExampleRecord(1, "January", "$1000"),
                new ExampleRecord(2, "February", "$2000"),
                new ExampleRecord(3, "March", "$3000"));

        CSVFormat csvFormat =
            CSVFormat.DEFAULT.withHeaderComments("CSV file created by Apache Beam")
                .withCommentMarker('#');

        p.apply("Create collection", Create.of(rows))
            .apply(
                "Write to CSV file",
                    CsvIO.<ExampleRecord>write(options.getFilePathPrefix(), csvFormat)
                        .withNumShards(1));
        p.run();
    }
}
```
