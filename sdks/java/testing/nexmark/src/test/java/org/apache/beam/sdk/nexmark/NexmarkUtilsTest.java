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
package org.apache.beam.sdk.nexmark;

import static org.apache.beam.sdk.nexmark.NexmarkUtils.ResourceNameMode.QUERY;
import static org.apache.beam.sdk.nexmark.NexmarkUtils.ResourceNameMode.QUERY_AND_SALT;
import static org.apache.beam.sdk.nexmark.NexmarkUtils.ResourceNameMode.QUERY_RUNNER_AND_MODE;
import static org.apache.beam.sdk.nexmark.NexmarkUtils.ResourceNameMode.VERBATIM;
import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the {@link NexmarkUtils}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class NexmarkUtilsTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testPrepareCsvSideInput() throws Exception {
    NexmarkConfiguration config = NexmarkConfiguration.DEFAULT.copy();
    config.sideInputType = NexmarkUtils.SideInputType.CSV;
    ResourceId sideInputResourceId =
        FileSystems.matchNewResource(
            String.format(
                "%s/JoinToFiles-%s",
                pipeline.getOptions().getTempLocation(), new Random().nextInt()),
            false);
    config.sideInputUrl = sideInputResourceId.toString();
    config.sideInputRowCount = 10000;
    config.sideInputNumShards = 15;

    PCollection<KV<Long, String>> sideInput = NexmarkUtils.prepareSideInput(pipeline, config);
    try {
      PAssert.that(sideInput)
          .containsInAnyOrder(
              LongStream.range(0, config.sideInputRowCount)
                  .boxed()
                  .map(l -> KV.of(l, l.toString()))
                  .collect(Collectors.toList()));
      pipeline.run();
    } finally {
      NexmarkUtils.cleanUpSideInput(config);
    }
  }

  @Test
  public void testFullQueryNameAppendsLanguageIfNeeded() {
    String fullName = NexmarkUtils.fullQueryName("sql", "1");
    assertEquals("1_sql", fullName);
  }

  @Test
  public void testFullQueryNameDoesntContainNullLanguage() {
    String fullName = NexmarkUtils.fullQueryName(null, "1");
    assertEquals("1", fullName);
  }

  @Test
  public void testTableName() {
    String table = "nexmark";
    String query = "query";
    long salt = 1111;
    String version = "version";
    Class runner = Runner.class;
    boolean isStreaming = true;

    testTableName(VERBATIM, table, query, salt, version, runner, isStreaming, "nexmark_version");

    testTableName(QUERY, table, query, salt, version, runner, isStreaming, "nexmark_query_version");

    testTableName(
        QUERY_AND_SALT,
        table,
        query,
        salt,
        version,
        runner,
        isStreaming,
        "nexmark_query_version_1111");

    testTableName(
        QUERY_RUNNER_AND_MODE,
        table,
        query,
        salt,
        version,
        runner,
        isStreaming,
        "nexmark_query_Runner_streaming_version");

    testTableName(
        QUERY_RUNNER_AND_MODE,
        table,
        query,
        salt,
        version,
        runner,
        !isStreaming,
        "nexmark_query_Runner_batch_version");

    testTableName(
        QUERY_RUNNER_AND_MODE,
        table,
        query,
        salt,
        null,
        runner,
        isStreaming,
        "nexmark_query_Runner_streaming");
  }

  private void testTableName(
      NexmarkUtils.ResourceNameMode nameMode,
      String baseTableName,
      String queryName,
      Long salt,
      String version,
      Class runner,
      Boolean isStreaming,
      final String expected) {
    NexmarkOptions options = PipelineOptionsFactory.as(NexmarkOptions.class);
    options.setResourceNameMode(nameMode);
    options.setBigQueryTable(baseTableName);
    options.setRunner(runner);
    options.setStreaming(isStreaming);

    String tableName = NexmarkUtils.tableName(options, queryName, salt, version);

    assertEquals(expected, tableName);
  }

  private static class Runner extends PipelineRunner<PipelineResult> {

    private Runner() {}

    @Override
    public PipelineResult run(Pipeline pipeline) {
      return null;
    }
  }
}
