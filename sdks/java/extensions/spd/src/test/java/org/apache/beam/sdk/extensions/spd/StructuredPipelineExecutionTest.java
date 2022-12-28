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
package org.apache.beam.sdk.extensions.spd;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map.Entry;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.python.PythonService;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructuredPipelineExecutionTest {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredPipelineExecutionTest.class);

  /*
  @Rule public final transient TestPipeline pipeline = TestPipeline.fromOptions(pipelineOptions());

  private static PipelineOptions pipelineOptions() {
    PipelineOptions p = TestPipeline.testingPipelineOptions();
    p.setStableUniqueNames(CheckEnabled.OFF);
    return p;
  }
  */

  @Test
  public void testSimplePipeline() throws Exception {


    URL pipelineURL = ClassLoader.getSystemClassLoader().getResource("simple_pipeline");
    URL profileURL = ClassLoader.getSystemClassLoader().getResource("test_profile.yml");
    Path pipelinePath = Paths.get(pipelineURL.toURI());

    int port = PythonService.findAvailablePort();
    ImmutableList.Builder<String> args = ImmutableList.builder();
    args.add("-p " + port);
    PythonService pyRunner = new PythonService("apache_beam.runners.portability.local_job_service_main", args.build());
    try (AutoCloseable p = pyRunner.start()) {
      PythonService.waitForPort("localhost", port, 15000);

      Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs("--runner=PortableRunner","--jobEndpoint=localhost:"+port).as(PipelineOptions.class));


      // Test Table Provider
      TestTableProvider testProvider = new TestTableProvider();
      testProvider.createTable(
          Table.builder()
              .type("test")
              .name("simple_pipeline_test_rows")
              .schema(
                  Schema.builder()
                      .addField("id", FieldType.STRING)
                      .addField("wins", FieldType.INT64)
                      .build())
              .build());
      Schema rowSchema = testProvider.getTable("simple_pipeline_test_rows").getSchema();
      LOG.info("Adding test rows to simple_pipeline_test_rows with schema " + rowSchema);
      testProvider.addRows(
          "simple_pipeline_test_rows",
          row(rowSchema, "1", 10L),
          row(rowSchema, "2", 9L),
          row(rowSchema, "3", 18L),
          row(rowSchema, "4", 33L),
          row(rowSchema, "1", 10L),
          row(rowSchema, "5", 0L));

      StructuredPipelineDescription spd = new StructuredPipelineDescription(pipeline, testProvider);
      spd.loadProject(Paths.get(profileURL.toURI()), pipelinePath);

      for (Entry<String, Table> e : testProvider.getTables().entrySet()) {
        LOG.info("Rows in table " + e.getKey());
        LOG.info("" + e.getValue());
        for (Row row : testProvider.tableRows(e.getKey())) {
          LOG.info("" + row);
        }
      }
      LOG.info("Running pipeline");
      pipeline.run();
      for (Entry<String, Table> e : testProvider.getTables().entrySet()) {
        LOG.info("Rows in table " + e.getKey());
        LOG.info("" + e.getValue());
        for (Row row : testProvider.tableRows(e.getKey())) {
          LOG.info("" + row);
        }
      }
    }
  }

  private static Row row(Schema schema, Object... objects) {
    return Row.withSchema(schema).addValues(objects).build();
  }
}
