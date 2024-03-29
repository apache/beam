Prompt:
Write a sample Java code snippet that writes data to a JDBC sink using Apache Beam.

Response:
Your Apache Beam pipeline can write data to an Oracle Java Database Connectivity (JDBC) enabled database using the Apache Beam JdbcIO connector.

Here is an example of how to use the JdbcIO connector to accomplish this:

```java
package jdbc;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

// Pipeline to write data to a JDBC sink using the Apache Beam JdbcIO connector
public class WriteJdbcSink {

    // Class representing the data to be written to the JDBC sink
    public static class ExampleRow implements Serializable {

        private int id;
        private String month;
        private String amount;

        public ExampleRow() {}

        public ExampleRow(int id, String month, String amount) {
            this.id = id;
            this.month = month;
            this.amount = amount;
        }

        public int getId() {
            return id;
        }

        public String getMonth() {
            return month;
        }

        public String getAmount() {
            return amount;
        }
    }

    // Pipeline options for writing data to the JDBC sink
    public interface WriteJdbcSinkOptions extends PipelineOptions {
        @Description("Table name to write to")
        @Validation.Required
        String getTableName();

        void setTableName(String tableName);

        @Description("JDBC sink URL")
        @Validation.Required
        String getJdbcSinkUrl();

        void setJdbcSinkUrl(String jdbcSinkUrl);

        @Description("JDBC driver class name")
        @Default.String("org.postgresql.Driver")
        String getDriverClassName();

        void setDriverClassName(String driverClassName);

        @Description("DB Username")
        @Validation.Required
        String getSinkUsername();

        void setSinkUsername(String username);

        @Description("DB password")
        @Validation.Required
        String getSinkPassword();

        void setSinkPassword(String password);
    }

    // Main method to run the pipeline
    public static void main(String[] args) {
        // Parse the pipeline options from the command line
        WriteJdbcSinkOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteJdbcSinkOptions.class);

        // Create the JDBC sink configuration using the provided options
        JdbcIO.DataSourceConfiguration config =
                JdbcIO.DataSourceConfiguration.create(options.getDriverClassName(), options.getJdbcSinkUrl())
                        .withUsername(options.getSinkUsername())
                        .withPassword(options.getSinkPassword());

        // Create the pipeline
        Pipeline p = Pipeline.create(options);

        // Create sample rows to write to the JDBC sink
        List<ExampleRow> rows = Arrays.asList(
                new ExampleRow(1, "January", "$1000"),
                new ExampleRow(2, "February", "$2000"),
                new ExampleRow(3, "March", "$3000")
        );

        // // Create PCollection from the list of rows
        p.apply("Create collection of records", Create.of(rows))
        // Write the rows to the JDBC sink
        .apply(
            "Write to JDBC Sink",
            JdbcIO.<ExampleRow>write()
                .withDataSourceConfiguration(config)
                .withStatement(String.format("insert into %s values(?, ?, ?)", options.getTableName()))
                .withBatchSize(10L)
                .withPreparedStatementSetter(
                    (element, statement) -> {
                      statement.setInt(1, element.getId());
                      statement.setString(2, element.getMonth());
                      statement.setString(3, element.getAmount());
                    }));
        // Run the pipeline
        p.run();
    }
}
This code snippet utilizes the pipeline options pattern to parse command-line arguments.