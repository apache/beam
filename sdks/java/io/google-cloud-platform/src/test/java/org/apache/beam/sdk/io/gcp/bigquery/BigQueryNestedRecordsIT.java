package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** An example that exports nested BigQuery record to a file. */
@RunWith(JUnit4.class)
public class BigQueryNestedRecordsIT {
  private static final String RECORD_QUERY =
      "SELECT city.* FROM [apache-beam-testing:big_query_nested_test.source_table]";

  private static final String UNFLATTENABLE_QUERY =
      "SELECT * FROM [apache-beam-testing:big_query_nested_test.genomics_2]";

  private static Integer stringifyCount = 0;

  /** Options supported by this class. */
  public interface Options extends PipelineOptions {

    @Description("Query for the pipeline input.  Must return exactly one result")
    @Default.String(RECORD_QUERY)
    String getInput();

    void setInput(String value);

    @Description("Query for unflattenable input.  Must return exactly one result")
    @Default.String(UNFLATTENABLE_QUERY)
    String getUnflattenableInput();

    void setunflattenableInput(String value);
  }

  @Test
  public void testNestedRecords() throws Exception {
    PipelineOptionsFactory.register(Options.class);
    Options options = TestPipeline.testingPipelineOptions().as(Options.class);
    runPipeline(options);
  }

  private static void runPipeline(Options options) throws Exception {
    // Create flattened and unflattened collections via Dataflow, via normal and side input
    // paths.
    Pipeline p = Pipeline.create(options);
    BigQueryOptions bigQueryOptions = options.as(BigQueryOptions.class);

    PCollection<TableRow> flattenedCollection =
        p.apply("ReadFlattened", BigQueryIO.readTableRows().fromQuery(options.getInput()));
    PCollection<TableRow> nonFlattenedCollection =
        p.apply(
            "ReadNonFlattened",
            BigQueryIO.readTableRows().fromQuery(options.getInput()).withoutResultFlattening());
    PCollection<TableRow> unflattenableCollection =
        p.apply(
            "ReadUnflattenable",
            BigQueryIO.readTableRows()
                .fromQuery(options.getUnflattenableInput())
                .withoutResultFlattening());

    final PCollectionView<Iterable<TableRow>> flattenedView =
        flattenedCollection.apply("PCViewFlattened", View.asIterable());
    PCollection<TableRow> flattenedSideInput =
        p.apply("Create1", Create.of(1))
            .apply(
                "ReadFlattenedSI",
                ParDo.of(
                        new DoFn<Integer, TableRow>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            for (TableRow r : c.sideInput(flattenedView)) {
                              c.output(r);
                            }
                          }
                        })
                    .withSideInputs(flattenedView));

    final PCollectionView<Iterable<TableRow>> nonFlattenedView =
        nonFlattenedCollection.apply("PCViewNonFlattened", View.asIterable());
    PCollection<TableRow> nonFlattenedSideInput =
        p.apply("Create2", Create.of(1))
            .apply(
                "ReadNonFlattenedSI",
                ParDo.of(
                        new DoFn<Integer, TableRow>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            for (TableRow r : c.sideInput(nonFlattenedView)) {
                              c.output(r);
                            }
                          }
                        })
                    .withSideInputs(nonFlattenedView));

    // Also query BigQuery directly.
    BigqueryClient bigQueryClient = new BigqueryClient(bigQueryOptions.getAppName());

    TableRow queryFlattenedTyped =
        bigQueryClient
            .queryWithRetries(options.getInput(), bigQueryOptions.getProject(), true)
            .getRows()
            .get(0);

    TableRow queryUnflattened =
        bigQueryClient
            .queryUnflattened(options.getInput(), bigQueryOptions.getProject(), true)
            .get(0);

    TableRow queryUnflattenable =
        bigQueryClient
            .queryUnflattened(options.getUnflattenableInput(), bigQueryOptions.getProject(), true)
            .get(0);

    // Verify that the results are the same.
    PAssert.thatSingleton(flattenedCollection).isEqualTo(queryFlattenedTyped);
    PAssert.thatSingleton(flattenedSideInput).isEqualTo(queryFlattenedTyped);
    PAssert.thatSingleton(nonFlattenedCollection).isEqualTo(queryUnflattened);
    PAssert.thatSingleton(nonFlattenedSideInput).isEqualTo(queryUnflattened);
    PAssert.thatSingleton(unflattenableCollection).isEqualTo(queryUnflattenable);

    // And that flattened results are different from non-flattened results.
    PAssert.thatSingleton(flattenedCollection).notEqualTo(queryUnflattened);
    p.run().waitUntilFinish();
  }
}
