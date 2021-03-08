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
package org.apache.beam.sdk.io.hcatalog;

import static org.apache.beam.sdk.io.common.IOITHelper.getHashForRecordCount;
import static org.apache.beam.sdk.io.hcatalog.test.HCatalogIOTestUtils.buildHCatRecords;

import java.util.Map;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A test of {@link org.apache.beam.sdk.io.hcatalog.HCatalogIO} on an independent Hive/HCatalog
 * instance.
 *
 * <p>This test requires a running instance of Hadoop, Hive and HCatalog. Pass in connection
 * information using PipelineOptions:
 *
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/hcatalog -DintegrationTestPipelineOptions='[
 *  "--HCatalogMetastoreHostName=hcatalog-metastore",
 *  "--HCatalogMetastorePort=9083",
 *  "--HCatalogHivePort=10000",
 *  "--HCatalogHiveDatabaseName=default",
 *  "--HCatalogHiveUsername=user",
 *  "--HCatalogHivePassword=password",
 *  "--numberOfRecords=1000" ]'
 * </pre>
 *
 * <p>If you want to run this with a runner besides directrunner, there are profiles for dataflow
 * and spark in the hcatalog pom. You'll want to activate those in addition to the normal test
 * runner invocation pipeline options.
 */
@RunWith(JUnit4.class)
public class HCatalogIOIT {

  /** PipelineOptions for testing {@link org.apache.beam.sdk.io.hcatalog.HCatalogIO}. */
  public interface HCatalogPipelineOptions extends IOTestPipelineOptions {
    @Description("HCatalog metastore host (hostname/ip address)")
    @Default.String("hcatalog-metastore")
    String getHCatalogMetastoreHostName();

    void setHCatalogMetastoreHostName(String host);

    @Description("HCatalog metastore port")
    @Default.Integer(9083)
    Integer getHCatalogMetastorePort();

    void setHCatalogMetastorePort(Integer port);

    @Description("HCatalog hive port")
    @Default.Integer(10000)
    Integer getHCatalogHivePort();

    void setHCatalogHivePort(Integer port);

    @Description("HCatalog hive database")
    @Default.String("default")
    String getHCatalogHiveDatabaseName();

    void setHCatalogHiveDatabaseName(String databaseName);

    @Description("HCatalog hive username")
    @Default.String("")
    String getHCatalogHiveUsername();

    void setHCatalogHiveUsername(String username);

    @Description("HCatalog hive password")
    @Default.String("")
    String getHCatalogHivePassword();

    void setHCatalogHivePassword(String password);
  }

  private static final ImmutableMap<Integer, String> EXPECTED_HASHES =
      ImmutableMap.of(
          100, "34c19971bd34cc1ed6218b84d0db3018",
          1000, "2db7f961724848ffcea299075c166ae8",
          10_000, "7885cdda3ed927e17f7db330adcbebcc");

  private static HiveDatabaseTestHelper helper;
  private static Map<String, String> configProperties;

  private static final String testIdentifier = "HCatalogIOIT";
  private static HCatalogPipelineOptions options;
  private static String tableName;

  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  @BeforeClass
  public static void setup() throws Exception {
    PipelineOptionsFactory.register(HCatalogPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HCatalogPipelineOptions.class);

    final String metastoreUri =
        String.format(
            "thrift://%s:%s",
            options.getHCatalogMetastoreHostName(), options.getHCatalogMetastorePort());
    configProperties = ImmutableMap.of("hive.metastore.uris", metastoreUri);

    helper =
        new HiveDatabaseTestHelper(
            options.getHCatalogMetastoreHostName(),
            options.getHCatalogHivePort(),
            options.getHCatalogHiveDatabaseName(),
            options.getHCatalogHiveUsername(),
            options.getHCatalogHivePassword());

    try {
      tableName = helper.createHiveTable(testIdentifier);
    } catch (Exception e) {
      helper.closeConnection();
      throw new Exception("Problem with creating table for " + testIdentifier + ": " + e, e);
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      helper.dropHiveTable(tableName);
    } catch (Exception e) {
      helper.closeConnection();
      throw new Exception("Problem with deleting table " + tableName + ": " + e, e);
    } finally {
      helper.closeConnection();
    }
  }

  @Test
  public void writeAndReadAll() {
    pipelineWrite
        .apply("Generate sequence", Create.of(buildHCatRecords(options.getNumberOfRecords())))
        .apply(
            HCatalogIO.write()
                .withConfigProperties(configProperties)
                .withDatabase(options.getHCatalogHiveDatabaseName())
                .withTable(tableName));
    pipelineWrite.run().waitUntilFinish();

    PCollection<String> testRecords =
        pipelineRead
            .apply(
                HCatalogIO.read()
                    .withConfigProperties(configProperties)
                    .withDatabase(options.getHCatalogHiveDatabaseName())
                    .withTable(tableName))
            .apply(ParDo.of(new CreateHCatFn()));

    PCollection<String> consolidatedHashcode =
        testRecords.apply("Calculate hashcode", Combine.globally(new HashingFn()));

    String expectedHash = getHashForRecordCount(options.getNumberOfRecords(), EXPECTED_HASHES);
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    pipelineRead.run().waitUntilFinish();
  }

  /** Outputs value stored in the HCatRecord. */
  private static class CreateHCatFn extends DoFn<HCatRecord, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().get(0).toString());
    }
  }
}
