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
package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;


import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A test of {@link org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO} on an
 * independent Hive instance using HCatalog.
 *
 * <p>This test requires a running instance of Hive, and the test dataset must exist in
 * the database.
 *
 * <p>You can run this test by doing the following:
 * <pre>
 *  mvn -e -Pio-it verify -pl sdks/java/io/hadoop/jdk1.8-tests/HIFIOHCatalogIT
 *  -DintegrationTestPipelineOptions='[
 *  "--hiveMetastoreUri=thrift://metastoreIp:port"]'
 * </pre>
 *
 * <p>If you want to run this with a runner besides directrunner, there are profiles for dataflow
 * and spark in the jdk1.8-tests pom. You'll want to activate those in addition to the normal test
 * runner invocation pipeline options.
 */

@RunWith(JUnit4.class)
public class HIFIOHCatalogIT implements Serializable {

  private static final String HIVE_DATABASE = "default";
  private static final String HIVE_TABLE = "employee";
  private static final String HIVE_FILTER = "salary=\"1000\"";
  private static HIFTestOptions options;
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFTestOptions.class);
  }

  /**
   * This test reads data from the Hive instance using HCatalog
   * and verifies if data is read successfully.
   * @throws IOException
   */
  @Test
  public void testHIFReadForHCatalog() throws IOException {
    Long expectedRecordsCount = 200000L;
    Configuration conf = getConfiguration(options);
    PCollection<KV<Long, String>> hiveData = pipeline.apply(HadoopInputFormatIO
        .<Long, String>read().withConfiguration(conf).withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(hiveData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(expectedRecordsCount);
    pipeline.run().waitUntilFinish();
  }

  static SimpleFunction<DefaultHCatRecord, String> myValueTranslate =
      new SimpleFunction<DefaultHCatRecord, String>() {
    @Override
    public String apply(DefaultHCatRecord input) {
      return input.getAll().toString();
    }
  };

  /**
   * Returns Hadoop configuration for reading data from Hive using HCatalog.
   * To read data from Hive using
   * HadoopInputFormatIO, following properties must be set: InputFormat class, InputFormat key
   * class, InputFormat value class, Metastore URI, database(optional,
   * assumes 'default' if none specified), table, filter(optional)
   * @throws IOException
   */
  private static Configuration getConfiguration(HIFTestOptions options) throws IOException {
    Configuration hcatConf = new Configuration();
    hcatConf.setClass("mapreduce.job.inputformat.class",
        HCatInputFormat.class, InputFormat.class);
    hcatConf.setClass("key.class", LongWritable.class, WritableComparable.class);
    hcatConf.setClass("value.class", DefaultHCatRecord.class, Writable.class);
    hcatConf.set("hive.metastore.uris", options.getHiveMetastoreUri());

    //explicitly specifying database, table & filter
    HCatInputFormat.setInput(hcatConf, HIVE_DATABASE, HIVE_TABLE, HIVE_FILTER);

    //specifying table only, assumes 'default' database in this case and no filter
    //HCatInputFormat.setInput(hiveConf, null, HIVE_TABLE, null);

    return hcatConf;
  }
}
