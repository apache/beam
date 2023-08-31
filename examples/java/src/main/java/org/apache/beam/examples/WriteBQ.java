package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Splitter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WriteBQ {

  public interface WriteBQOptions extends PipelineOptions, BigQueryOptions {
    @Description("Table to write to")
    String getTable();

    void setTable(String value);
  }

  public static void main(String[] args) {

    WriteBQOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WriteBQOptions.class);
    options.setTable("google.com:clouddfe:jjc_test.writebq2");
    options.setUseStorageWriteApi(true);
    options.setStorageApiAppendThresholdRecordCount(100);

    Pipeline p = Pipeline.create(options);

    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("name").setType("STRING"));
    fields.add(new TableFieldSchema().setName("year").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("country").setType("STRING"));
    TableSchema schema = new TableSchema().setFields(fields);

    p
        .apply(GenerateSequence.from(0).to(100_000))
        //Convert to TableRow
        .apply("to TableRow", ParDo.of(new DoFn<Long, TableRow>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            TableRow row = new TableRow();

            row.set("name", "name");
            row.set("year", c.element());
            row.set("country", "country");

            c.output(row);
          }
        }))
        // to BigQuery
        // Using `writeTableRows` is slightly less performant than using write with `WithFormatFunction`
        // due to the TableRow encoding. See `WriteWithFormatBQ` for an example.
        .apply(BigQueryIO.writeTableRows() // Input type from prev stage is Row
            .withSchema(schema)
            .to(options.getTable())
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    p.run();
  }
}
