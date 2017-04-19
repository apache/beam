package com.disney.dtss.desa.tools;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerOptions;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.coders.SpannerMutationCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.util.Transport;

  
import java.io.Serializable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Generalized bulk loader for importing BigQuery tables as Datastore kinds.
 *
 * <p>To execute this pipeline locally, specify general pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 * }
 * </pre>
 * and the Datastore info for the output, with the form
 * <pre>{@code
 *   --dataset=DATASET_ID
 *   --key=Key specification for the kind.   
 * }</pre>
 *
 * <p>To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 * }
 * </pre>
 * and the BigQuery table reference for input
 * <pre>{@code
 *   --input=<project_id>:<dataset_id>.<table_id>
 * }</pre>
 *
 */
public class SpannerCSVLoader {

  private static final Logger mLog = LoggerFactory.getLogger(SpannerCSVLoader.class);
  private static Options options;


  /**
   * Command options specificiation.
   */
  private static interface Options extends PipelineOptions {
    @Description("File to read from ")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("Instance ID to write to in Spanner")
    @Validation.Required
    String getInstanceId();
    void setInstanceId(String value);

    @Description("Database ID to write to in Spanner")
    @Validation.Required
    String getDatabaseId();
    void setDatabaseId(String value);

    @Description("Schema YAML")
    @Validation.Required
    String getSchema();
    void setSchema(String value);
  }


  /**
   * Constructs and executes the processing pipeline based upon command options.
   */
  public static void main(String[] args) throws Exception {

    options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Yaml yaml = new Yaml();  
    InputStream in = Files.newInputStream(Paths.get(options.getSchema()));
    TableInfo tableInfo = yaml.loadAs(in, TableInfo.class);

    Pipeline p = Pipeline.create(options);
    p.apply(TextIO.Read.from(options.getInput()))
        .apply(ParDo.of(new ParseLineFn(tableInfo)))
        .apply(SpannerIO.writeTo(options.getInstanceId(), options.getDatabaseId()));
    p.run().waitUntilFinish();

  }


  /**
   * A DoFn that creates a Spanner Mutation for each BigQuery row.
   */
  static class ParseLineFn extends DoFn<String, Mutation> {
    private final TableInfo tableInfo;
    private final SpannerMutationBuilder rowBuilder;

    ParseLineFn(TableInfo tableInfo) {
      this.tableInfo = tableInfo;
      rowBuilder = new SpannerMutationBuilder(tableInfo);
    }


    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
          Mutation e = rowBuilder.build(c.element());
          if (e != null)
              c.output(e);
      }
      catch (Exception e) {
          mLog.error(e.getMessage(), e);
      }
    }
  }


  public static class TableInfo implements Serializable {

      private String tableName;
      private List<Schema> schema;

      public String getTableName() { return tableName; }
      public void setTableName(String tableName) { this.tableName = tableName; }

      public List<Schema> getSchema() { return this.schema; }
      public void setSchema(List<Schema> schema) { this.schema = schema; }
  }

  
  public static class Schema implements Serializable {

      private String name;
      private String type;
      private boolean nullable;
      private boolean exclude;
      private boolean pk;
     

      Schema() {
          exclude = false;
          nullable = true;
      }

      public String getName() { return name; }
      public void setName(String name) { this.name = name; }
      public String getType() { return type; }
      public void setType(String type) { this.type = type; }
      public boolean isNullable() { return nullable; }
      public void setNullable(boolean flag) { nullable = flag; }
      public boolean isExclude() { return this.exclude; }
      public void setExclude(boolean flag) { this.exclude = flag; }
      public boolean isPk() { return pk; }
      public void setPk(boolean flag) { pk = flag; }

      
      @Override
      public int hashCode() {
          return name.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
          if (this == obj)
              return true;
          if (obj == null)
              return false;
          if (getClass() != obj.getClass())
              return false;
          Schema other = (Schema) obj;
          return name.equals(other.getName());
    }
      
  }

}
