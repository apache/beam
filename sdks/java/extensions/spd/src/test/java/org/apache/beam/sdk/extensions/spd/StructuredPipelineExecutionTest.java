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
import java.util.Map;
import org.apache.beam.runners.portability.PortableRunner;
import org.apache.beam.sdk.extensions.python.PythonService;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class StructuredPipelineExecutionTest {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredPipelineExecutionTest.class);

  @Test
  public void testSimplePipeline() throws Exception {

    // Pipeline pipeline = Pipeline.create();

    URL pipelineURL = ClassLoader.getSystemClassLoader().getResource("simple_pipeline");
    URL profileURL = ClassLoader.getSystemClassLoader().getResource("test_profile.yml");
    Path pipelinePath = Paths.get(pipelineURL.toURI());

    // Start up the Expansion Service
    int expansionPort = PythonService.findAvailablePort();
    ImmutableList.Builder<String> eargs = ImmutableList.builder();
    eargs.add(
        "--port=" + expansionPort,
        "--environment_type=beam:env:embedded_python:v1",
        "--environment_config={}",
        "--fully_qualified_name_glob=*",
        "--pickle_library=cloudpickle");
    PythonService expansionService =
        new PythonService("apache_beam.runners.portability.expansion_service_main", eargs.build());
    try (AutoCloseable x = expansionService.start()) {
      PythonService.waitForPort("localhost", expansionPort, 15000);
      // Test Table Provider
      TestTableProvider testProvider = new TestTableProvider();
      // This table needs to be built before using in a pipeline otherwise addRows doesn't seem to
      // work?
      testProvider.createTable(
          Table.builder()
              .name("simple_pipeline_test_rows")
              .type("test")
              .schema(
                  Schema.builder()
                      .addField("id", Schema.FieldType.STRING)
                      .addField("wins", Schema.FieldType.INT64)
                      .build())
              .build());
      Schema rowSchema = testProvider.getTable("simple_pipeline_test_rows").getSchema();
      if (rowSchema.getFieldCount() != 2) {
        throw new Exception("Expected simple_pipeline_test_rows to have a schema size of 2");
      }
      LOG.info("Adding test rows to simple_pipeline_test_rows with schema " + rowSchema);
      testProvider.addRows(
          "simple_pipeline_test_rows",
          row(rowSchema, "1", 10L),
          row(rowSchema, "2", 9L),
          row(rowSchema, "3", 18L),
          row(rowSchema, "4", 33L),
          row(rowSchema, "1", 10L),
          row(rowSchema, "5", 0L));

      StructuredPipelineDescription spd = new StructuredPipelineDescription(testProvider);
      spd.registerExpansionService("python", "localhost:" + expansionPort);
      spd.loadProject(Paths.get(profileURL.toURI()), pipelinePath);

      for (Map.Entry<String, Table> e : testProvider.getTables().entrySet()) {
        LOG.info("Rows in table " + e.getKey());
        LOG.info("" + e.getValue());
        for (Row row : testProvider.tableRows(e.getKey())) {
          LOG.info("" + row);
        }
      }
      LOG.info("----------------------- RUN PIPELINE -------------------------------");
      int port = PythonService.findAvailablePort();
      ImmutableList.Builder<String> args = ImmutableList.builder();
      args.add("--port=" + port);
      PythonService pyRunner =
          new PythonService("apache_beam.runners.portability.local_job_service_main", args.build());
      try (AutoCloseable p = pyRunner.start()) {
        PythonService.waitForPort("localhost", port, 15000);
        PortablePipelineOptions options = PipelineOptionsFactory.as(PortablePipelineOptions.class);
        options.setRunner(PortableRunner.class);
        options.setDefaultEnvironmentType("LOOPBACK");
        options.setJobEndpoint("localhost:" + port);
        PortableRunner runner = PortableRunner.fromOptions(options);
        runner.run(spd.getPipeline()).waitUntilFinish();
      }
      LOG.info("-------------------------DONE PIPELINE-------------------------------");
      for (Map.Entry<String, Table> e : testProvider.getTables().entrySet()) {
        LOG.info("Rows in table " + e.getKey());
        LOG.info("" + e.getValue());
        int count = 0;
        for (Row row : testProvider.tableRows(e.getKey())) {
          LOG.info("" + row);
          count++;
        }
        if ("another_win_by_id".equals(e.getKey()) && count == 0) {
          throw new Exception("Expected to output another_win_by_id");
        }
      }
    }
  }

  private static Row row(Schema schema, Object... objects) {
    return Row.withSchema(schema).addValues(objects).build();
  }
}
