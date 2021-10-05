/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.example;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogPipelineOptions;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogTableProvider;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Example pipeline that uses Google Cloud Data Catalog to retrieve the table metadata. */
public class BeamSqlDataCatalogExample {

  private static final Logger LOG = LoggerFactory.getLogger(BeamSqlDataCatalogExample.class);

  /** Pipeline options to specify the query and the output for the example. */
  public interface DCExamplePipelineOptions extends PipelineOptions {

    /** SQL Query. */
    @Description("Required. SQL Query containing the pipeline logic.")
    @Validation.Required
    String getQueryString();

    void setQueryString(String queryString);

    /** Output file prefix. */
    @Description("Required. Output file prefix.")
    @Validation.Required
    String getOutputFilePrefix();

    void setOutputFilePrefix(String outputPathPrefix);
  }

  public static void main(String[] args) throws Exception {
    LOG.info("Args: {}", Arrays.asList(args));
    DCExamplePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DCExamplePipelineOptions.class);
    LOG.info("Query: {}\nOutput: {}", options.getQueryString(), options.getOutputFilePrefix());

    Pipeline pipeline = Pipeline.create(options);

    validateArgs(options);

    try (DataCatalogTableProvider tableProvider =
        DataCatalogTableProvider.create(options.as(DataCatalogPipelineOptions.class))) {
      pipeline
          .apply(
              "SQL Query",
              SqlTransform.query(options.getQueryString())
                  .withDefaultTableProvider("datacatalog", tableProvider))
          .apply("Convert to Strings", rowsToStrings())
          .apply("Write output", TextIO.write().to(options.getOutputFilePrefix()));

      pipeline.run().waitUntilFinish();
    }
  }

  private static MapElements<Row, String> rowsToStrings() {
    return MapElements.into(TypeDescriptor.of(String.class))
        .via(
            row -> row.getValues().stream().map(String::valueOf).collect(Collectors.joining(", ")));
  }

  private static void validateArgs(DCExamplePipelineOptions options) {
    if (Strings.isNullOrEmpty(options.getQueryString())
        || Strings.isNullOrEmpty(options.getOutputFilePrefix())) {
      String usage =
          "ERROR: SQL query or output file is not specified."
              + "To run this example:\n"
              + "./gradlew "
              + ":sdks:java:extensions:sql:datacatalog:runDataCatalogExample "
              + "-PgcpProject=<project> "
              + "-PgcsTempRoot=<GCS temp location> "
              + "-PqueryString=<query> "
              + "-PoutputFilePrefix=<output location> "
              + "-PtempLocation=<temp GCS location for BQ export>\n\n";
      throw new IllegalArgumentException(usage);
    }
  }
}
