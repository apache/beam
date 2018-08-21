package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryToTableIT {

  private BigQueryToTableOptions options;
  private String project;
  private final String BIG_QUERY_DATASET_ID = "java_query_to_table_it";
  private final String OUTPUT_TABLE_NAME = "output_table";
  private Bigquery bqClient;
  private BigQueryOptions bqOption;
  private String outputTable;

  public interface BigQueryToTableOptions extends TestPipelineOptions {

    @Description("The BigQuery query to be used for creating the source")
    @Validation.Required
    String getQuery();

    void setQuery(String query);

    @Description("BigQuery table to write to, specified as "
        + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
    @Validation.Required
    String getOutput();

    void setOutput(String value);

    @Description("BigQuery output table schema.")
    @Validation.Required
    TableSchema getOutputSchema();

    void setOutputSchema(TableSchema value);

    @Description("Whether to force reshuffle.")
    @Default.Boolean(false)
    boolean getReshuffle();

    void setReshuffle(boolean reshuffle);

    @Description("Whether to use the Standard SQL dialect when querying BigQuery.")
    @Default.Boolean(false)
    boolean getUsingStandardSql();

    void setUsingStandardSql(boolean usingStandardSql);
  }

  private static HttpRequestInitializer chainHttpRequestInitializer(
      Credentials credential, HttpRequestInitializer httpRequestInitializer) {
    if (credential == null) {
      return new ChainingHttpRequestInitializer(
          new NullCredentialInitializer(), httpRequestInitializer);
    } else {
      return new ChainingHttpRequestInitializer(
          new HttpCredentialsAdapter(credential), httpRequestInitializer);
    }
  }

  @Before
  public void setupBqEnvironment() throws Exception {
    PipelineOptionsFactory.register(BigQueryToTableOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BigQueryToTableOptions.class);
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    bqOption = options.as(BigQueryOptions.class);
    bqClient = new Bigquery.Builder(
                Transport.getTransport(),
                Transport.getJsonFactory(),
                chainHttpRequestInitializer(
                    bqOption.getGcpCredential(),
                    new RetryHttpRequestInitializer(ImmutableList.of(404))))
                .setApplicationName(bqOption.getAppName())
                .setGoogleClientRequestInitializer(bqOption.getGoogleApiTrace()).build();
    bqClient.datasets()
        .insert(project, new Dataset()
            .setDatasetReference(new DatasetReference()
                .setDatasetId(BIG_QUERY_DATASET_ID)))
        .execute();

    outputTable = project + ":" + BIG_QUERY_DATASET_ID + "." + OUTPUT_TABLE_NAME;
  }

  @After
  public void cleanBqEnvironment() throws Exception {
    bqClient.tables().delete(project, BIG_QUERY_DATASET_ID, OUTPUT_TABLE_NAME).execute();
    bqClient.datasets().delete(project, BIG_QUERY_DATASET_ID).execute();
  }

  private void runBigQueryToTablePipeline() {
    System.out.print(options);
    Pipeline p = Pipeline.create(options);
    BigQueryIO.Read bigQueryRead = BigQueryIO.read().fromQuery(options.getQuery());
    if (options.getUsingStandardSql()) {
      bigQueryRead = bigQueryRead.usingStandardSql();
    }
    PCollection<TableRow> input = p.apply(bigQueryRead);
    if (options.getReshuffle()) {
      input = input
          .apply(WithKeys.<Void, TableRow>of((Void) null))
          .setCoder(KvCoder.of(VoidCoder.of(), TableRowJsonCoder.of()))
          .apply(Reshuffle.<Void, TableRow>of())
          .apply(Values.<TableRow>create());
    }
    input.apply(
        BigQueryIO.writeTableRows().to(options.getOutput())
            .withSchema(options.getOutputSchema())
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    p.run().waitUntilFinish();
  }

  @Test
  public void testLegacyQueryWithoutReshuffle() throws Exception {
    List<String> expectedList = Arrays.asList("apple", "orange");
    options.setQuery("SELECT * FROM (SELECT \"apple\" as fruit), (SELECT \"orange\" as fruit),");
    options.setOutput(outputTable);
    List<TableFieldSchema> fieldSchemas = new ArrayList<>();
    fieldSchemas.add(new TableFieldSchema().setName("fruit").setType("STRING"));
    options.setOutputSchema(new TableSchema().setFields(fieldSchemas));

    runBigQueryToTablePipeline();

    QueryRequest query = new QueryRequest()
        .setQuery(String.format("SELECT fruit from [%s];", outputTable));
    QueryResponse response = bqClient.jobs().query(project, query).execute();
    List<String> tableResult = response.getRows()
        .stream()
        .flatMap(row -> row.getF().stream()
            .map(cell -> cell.getV().toString()))
        .collect(Collectors.toList());

    assertEquals(expectedList, tableResult);
  }
}
