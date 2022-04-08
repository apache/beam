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
package org.apache.beam.sdk.io.kudu;

import static org.apache.beam.sdk.io.kudu.KuduTestUtils.COL_ID;
import static org.apache.beam.sdk.io.kudu.KuduTestUtils.COL_NAME;
import static org.apache.beam.sdk.io.kudu.KuduTestUtils.GenerateUpsert;
import static org.apache.beam.sdk.io.kudu.KuduTestUtils.SCHEMA;
import static org.apache.beam.sdk.io.kudu.KuduTestUtils.createTableOptions;
import static org.apache.beam.sdk.io.kudu.KuduTestUtils.rowCount;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A test of {@link org.apache.beam.sdk.io.kudu.KuduIO} on an independent Kudu instance.
 *
 * <p>This test requires a running instance of Kudu. Pass in connection information using
 * PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/kudu -DintegrationTestPipelineOptions='[
 *    "--kuduMasterAddresses=127.0.0.1",
 *    "--kuduTable=beam-integration-test",
 *    "--numberOfRecords=100000" ]'
 *    --tests org.apache.beam.sdk.io.kudu.KuduIOIT
 *    -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>To start a Kudu server in docker you can use the following:
 *
 * <pre>
 *   docker pull usuresearch/apache-kudu docker run -d --rm --name apache-kudu -p 7051:7051 \
 *     -p 7050:7050 -p 8051:8051 -p 8050:8050 usuresearch/apache-kudu ```
 * </pre>
 *
 * <p>See <a href="https://hub.docker.com/r/usuresearch/apache-kudu/">for information about this
 * image</a>.
 *
 * <p>Once running you may need to visit <a href="http://localhost:8051/masters">the masters
 * list</a> and copy the host (e.g. <code>host: "e94929167e2a"</code>) adding it to your <code>
 * etc/hosts</code> file pointing to localhost e.g.:
 *
 * <pre>
 *   127.0.0.1 localhost e94929167e2a
 * </pre>
 */
@RunWith(JUnit4.class)
public class KuduIOIT {

  /** KuduIOIT options. */
  public interface KuduPipelineOptions extends IOTestPipelineOptions {
    @Description("Kudu master addresses (comma separated address list)")
    @Default.String("127.0.0.1:7051")
    String getKuduMasterAddresses();

    void setKuduMasterAddresses(String masterAddresses);

    @Description("Kudu table")
    @Default.String("beam-integration-test")
    String getKuduTable();

    void setKuduTable(String name);
  }

  private static KuduPipelineOptions options;
  private static KuduClient client;
  private static KuduTable kuduTable;

  @Rule public final TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUp() throws KuduException {
    PipelineOptionsFactory.register(KuduPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(KuduPipelineOptions.class);

    // synchronous operations
    client =
        new AsyncKuduClient.AsyncKuduClientBuilder(options.getKuduMasterAddresses())
            .build()
            .syncClient();

    if (client.tableExists(options.getKuduTable())) {
      client.deleteTable(options.getKuduTable());
    }

    kuduTable =
        client.createTable(options.getKuduTable(), KuduTestUtils.SCHEMA, createTableOptions());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      if (client.tableExists(options.getKuduTable())) {
        client.deleteTable(options.getKuduTable());
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testWriteThenRead() throws Exception {
    runWrite();
    runReadAll();
    readPipeline = TestPipeline.create();
    runReadProjectedColumns();
    readPipeline = TestPipeline.create();
    runReadWithPredicates();
  }

  private void runReadAll() {
    // Lambdas erase too much type information so specify the coder
    PCollection<String> output =
        readPipeline.apply(
            KuduIO.<String>read()
                .withMasterAddresses(options.getKuduMasterAddresses())
                .withTable(options.getKuduTable())
                .withParseFn(
                    (SerializableFunction<RowResult, String>) input -> input.getString(COL_NAME))
                .withCoder(StringUtf8Coder.of()));
    PAssert.thatSingleton(output.apply("Count", Count.globally()))
        .isEqualTo((long) options.getNumberOfRecords());

    readPipeline.run().waitUntilFinish();
  }

  private void runReadWithPredicates() {
    PCollection<String> output =
        readPipeline.apply(
            "Read with predicates",
            KuduIO.<String>read()
                .withMasterAddresses(options.getKuduMasterAddresses())
                .withTable(options.getKuduTable())
                .withParseFn(
                    (SerializableFunction<RowResult, String>) input -> input.getString(COL_NAME))
                .withPredicates(
                    Arrays.asList(
                        KuduPredicate.newComparisonPredicate(
                            SCHEMA.getColumn(COL_ID), KuduPredicate.ComparisonOp.GREATER_EQUAL, 2),
                        KuduPredicate.newComparisonPredicate(
                            SCHEMA.getColumn(COL_ID), KuduPredicate.ComparisonOp.LESS, 7)))
                .withCoder(StringUtf8Coder.of()));

    output.apply(Count.globally());

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo((long) 5);

    readPipeline.run().waitUntilFinish();
  }

  /**
   * Tests that the projected columns are passed down to the Kudu scanner by attempting to read the
   * {@value KuduTestUtils#COL_NAME} in the parse function when it is omitted.
   */
  private void runReadProjectedColumns() {
    thrown.expect(IllegalArgumentException.class);
    readPipeline
        .apply(
            "Read with projected columns",
            KuduIO.<String>read()
                .withMasterAddresses(options.getKuduMasterAddresses())
                .withTable(options.getKuduTable())
                .withParseFn(
                    (SerializableFunction<RowResult, String>) input -> input.getString(COL_NAME))
                .withProjectedColumns(Collections.singletonList(COL_ID))) // COL_NAME excluded
        .setCoder(StringUtf8Coder.of());
    readPipeline.run().waitUntilFinish();
  }

  private void runWrite() throws Exception {
    writePipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(options.getNumberOfRecords()))
        .apply(
            "Write records to Kudu",
            KuduIO.write()
                .withMasterAddresses(options.getKuduMasterAddresses())
                .withTable(options.getKuduTable())
                .withFormatFn(new GenerateUpsert()));
    writePipeline.run().waitUntilFinish();

    assertThat(
        "Wrong number of records in table",
        rowCount(kuduTable),
        equalTo(options.getNumberOfRecords()));
  }
}
