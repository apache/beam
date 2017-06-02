/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.io.hcatalog;

import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.TEST_TABLE_NAME;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.commitRecords;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.getReaderContext;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.getWriterContext;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.toHCatRecord;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.writeRecords;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.isA;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.beam.sdk.io.hcatalog.HCatalogIO.BoundedHCatalogSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


/**
 *Test for HCatalogIO.
 */
public class HCatalogIOTest implements Serializable {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  /**
   * Perform end-to-end test of Read operation.
   */
  public void testReadE2ESuccess() throws CommandNeedRetryException,
  IOException, ClassNotFoundException {

    Map<String, String> map = insertTestData();

    PCollection<String> output = pipeline.apply(HCatalogIO.read()
        .withConfigProperties(map)
        .withTable(HCatalogIOTestUtils.TEST_TABLE_NAME))
        .apply(ParDo.<DefaultHCatRecord, String>of(new DoFn<DefaultHCatRecord, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(c.element().get(0).toString());
          }
        }));
    PAssert.thatSingleton(output.apply("Count All", Count.<String>globally()))
    .isEqualTo(100L);
    pipeline.run();
  }

  @Test
  /**
   * Perform end-to-end test of Write operation.
   */
  public void testWriteE2ESuccess() throws CommandNeedRetryException,
  IOException, ClassNotFoundException {
    createTestTable();
    pipeline.apply(
        Create.of(
            toHCatRecord(0), toHCatRecord(1), toHCatRecord(2), toHCatRecord(3),
            toHCatRecord(4), toHCatRecord(5), toHCatRecord(6), toHCatRecord(7),
            toHCatRecord(8), toHCatRecord(9), toHCatRecord(10), toHCatRecord(11),
            toHCatRecord(12), toHCatRecord(13), toHCatRecord(14), toHCatRecord(15),
            toHCatRecord(16), toHCatRecord(17), toHCatRecord(18), toHCatRecord(19),
            toHCatRecord(20), toHCatRecord(21), toHCatRecord(22), toHCatRecord(23),
            toHCatRecord(24), toHCatRecord(25), toHCatRecord(26), toHCatRecord(27),
            toHCatRecord(28), toHCatRecord(29), toHCatRecord(30), toHCatRecord(31),
            toHCatRecord(32), toHCatRecord(33), toHCatRecord(34), toHCatRecord(35),
            toHCatRecord(36), toHCatRecord(37), toHCatRecord(38), toHCatRecord(39))
        )
        .apply(HCatalogIO.write()
            .withConfigProperties(getConfigProperties())
            .withTable(TEST_TABLE_NAME));
    pipeline.run();
    Assert.assertTrue(HCatalogIOTestUtils.getRecordsCount(getConfigProperties()) == 40);
  }

  /**
   * Test of Write to a non-existent table.
   */
  @Test
  public void testWriteFailureTableDoesNotExist() throws CommandNeedRetryException,
  IOException, ClassNotFoundException {
    thrown.expectCause(isA(UserCodeException.class));
    thrown.expectMessage(containsString("org.apache.hive.hcatalog.common.HCatException"));
    thrown.expectMessage(containsString("NoSuchObjectException"));
    pipeline.apply(
        Create.of(
            toHCatRecord(0), toHCatRecord(1), toHCatRecord(2), toHCatRecord(3),
            toHCatRecord(4), toHCatRecord(5), toHCatRecord(6), toHCatRecord(7),
            toHCatRecord(8), toHCatRecord(9), toHCatRecord(10), toHCatRecord(11),
            toHCatRecord(12), toHCatRecord(13), toHCatRecord(14), toHCatRecord(15),
            toHCatRecord(16), toHCatRecord(17), toHCatRecord(18), toHCatRecord(19),
            toHCatRecord(20), toHCatRecord(21), toHCatRecord(22), toHCatRecord(23),
            toHCatRecord(24), toHCatRecord(25), toHCatRecord(26), toHCatRecord(27),
            toHCatRecord(28), toHCatRecord(29), toHCatRecord(30), toHCatRecord(31),
            toHCatRecord(32), toHCatRecord(33), toHCatRecord(34), toHCatRecord(35),
            toHCatRecord(36), toHCatRecord(37), toHCatRecord(38), toHCatRecord(39))
        )
        .apply(HCatalogIO.write()
            .withConfigProperties(getConfigProperties())
            .withTable("myowntable"));
    pipeline.run();
  }

  /**
   * Test of Write without specifying a table.
   */
  @Test
  public void testWriteFailureValidationTable() throws CommandNeedRetryException,
  IOException, ClassNotFoundException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(containsString("table"));
    pipeline.apply(
        Create.of(
            toHCatRecord(0), toHCatRecord(1), toHCatRecord(2), toHCatRecord(3),
            toHCatRecord(4), toHCatRecord(5), toHCatRecord(6), toHCatRecord(7),
            toHCatRecord(8), toHCatRecord(9), toHCatRecord(10), toHCatRecord(11),
            toHCatRecord(12), toHCatRecord(13), toHCatRecord(14), toHCatRecord(15),
            toHCatRecord(16), toHCatRecord(17), toHCatRecord(18), toHCatRecord(19),
            toHCatRecord(20), toHCatRecord(21), toHCatRecord(22), toHCatRecord(23),
            toHCatRecord(24), toHCatRecord(25), toHCatRecord(26), toHCatRecord(27),
            toHCatRecord(28), toHCatRecord(29), toHCatRecord(30), toHCatRecord(31),
            toHCatRecord(32), toHCatRecord(33), toHCatRecord(34), toHCatRecord(35),
            toHCatRecord(36), toHCatRecord(37), toHCatRecord(38), toHCatRecord(39))
        )
        .apply(HCatalogIO.write()
            .withConfigProperties(getConfigProperties()));
    pipeline.run();
  }

  /**
   * Test of Write without specifying configuration properties.
   */
  @Test
  public void testWriteFailureValidationConfigProp() throws CommandNeedRetryException,
  IOException, ClassNotFoundException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(containsString("configProperties"));
    pipeline.apply(
        Create.of(
            toHCatRecord(0), toHCatRecord(1), toHCatRecord(2), toHCatRecord(3),
            toHCatRecord(4), toHCatRecord(5), toHCatRecord(6), toHCatRecord(7),
            toHCatRecord(8), toHCatRecord(9), toHCatRecord(10), toHCatRecord(11),
            toHCatRecord(12), toHCatRecord(13), toHCatRecord(14), toHCatRecord(15),
            toHCatRecord(16), toHCatRecord(17), toHCatRecord(18), toHCatRecord(19),
            toHCatRecord(20), toHCatRecord(21), toHCatRecord(22), toHCatRecord(23),
            toHCatRecord(24), toHCatRecord(25), toHCatRecord(26), toHCatRecord(27),
            toHCatRecord(28), toHCatRecord(29), toHCatRecord(30), toHCatRecord(31),
            toHCatRecord(32), toHCatRecord(33), toHCatRecord(34), toHCatRecord(35),
            toHCatRecord(36), toHCatRecord(37), toHCatRecord(38), toHCatRecord(39))
        )
        .apply(HCatalogIO.write()
            .withTable("myowntable"));
    pipeline.run();
  }


  /**
   * Test of Read from a non-existent table.
   */
  @Test
  public void testReadFailureTableDoesNotExist() throws CommandNeedRetryException,
  IOException, ClassNotFoundException {

    Map<String, String> map = insertTestData();

    pipeline.apply(HCatalogIO.read()
        .withConfigProperties(map)
        .withTable("myowntable"))
        .apply(ParDo.<DefaultHCatRecord, String>of(new DoFn<DefaultHCatRecord, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(c.element().get(0).toString());
          }
        }));
    thrown.expectCause(isA(NoSuchObjectException.class));
    //thrown.expectMessage(containsString("NoSuchObjectException"));
    pipeline.run();
  }

  /**
   * Test of Read without specifying configuration properties.
   */
  @Test
  public void testReadFailureValidationConfig() throws CommandNeedRetryException,
  IOException, ClassNotFoundException {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(containsString("configProperties"));
    pipeline.apply(HCatalogIO.read()
        .withTable(TEST_TABLE_NAME))
        .apply(ParDo.<DefaultHCatRecord, String>of(new DoFn<DefaultHCatRecord, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(c.element().get(0).toString());
          }
        }));
    pipeline.run();
  }

  /**
   * Test of Read without specifying a table.
   */
  @Test
  public void testReadFailureValidationTable() throws CommandNeedRetryException,
  IOException, ClassNotFoundException {
    Map<String, String> map = new HashMap<String, String>();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(containsString("table"));
    pipeline.apply(HCatalogIO.read()
        .withConfigProperties(map))
        .apply(ParDo.<DefaultHCatRecord, String>of(new DoFn<DefaultHCatRecord, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(c.element().get(0).toString());
          }
        }));
    pipeline.run();
  }

  /**
   * Test of Read using SourceTestUtils.readFromSource(..).
   */
  @Test
  public void testReadFromSource() throws CommandNeedRetryException,
  IOException, ClassNotFoundException {
    List<BoundedHCatalogSource> sourceList = getSourceList();
    List<DefaultHCatRecord> recordsList = new ArrayList<DefaultHCatRecord>();
    for (int i = 0; i < sourceList.size(); i++) {
      recordsList.addAll(SourceTestUtils.readFromSource(sourceList.get(i),
        PipelineOptionsFactory.create()));
    }
    Assert.assertTrue(recordsList.size() == 100);
  }

  /**
   * Test of Read using SourceTestUtils.assertSourcesEqualReferenceSource(..).
   */
  @Test
  public void testSourcesEqualReferenceSource() throws Exception {
    List<BoundedHCatalogSource> sourceList = getSourceList();
    for (int i = 0; i < sourceList.size(); i++) {
      SourceTestUtils.assertSourcesEqualReferenceSource(sourceList.get(i),
          sourceList.get(i).split(-1, PipelineOptionsFactory.create()),
          PipelineOptionsFactory.create());
    }
  }

  private List<BoundedHCatalogSource> getSourceList() throws CommandNeedRetryException,
  HCatException, IOException,
      FileNotFoundException, ClassNotFoundException {
    List<BoundedHCatalogSource> sourceList = new ArrayList<BoundedHCatalogSource>();
    Map<String, String> configProperties = insertTestData();
    ReaderContext context = getReaderContext(configProperties);
    HCatalogIO.Read spec = HCatalogIO.read()
        .withConfigProperties(configProperties)
        .withContext(context)
        .withTable(TEST_TABLE_NAME);

    for (int i = 0; i < context.numSplits(); i++) {
      BoundedHCatalogSource source = new BoundedHCatalogSource(spec.withSplitId(i));
      sourceList.add(source);
    }
    return sourceList;
  }

  private Map<String, String> insertTestData() throws CommandNeedRetryException, HCatException,
      IOException, FileNotFoundException, ClassNotFoundException {
    createTestTable();
    Map<String, String> map = getConfigProperties();

    WriterContext cntxt = getWriterContext(map);

    writeRecords(cntxt);
    commitRecords(map, cntxt);
    return map;
  }

  private Map<String, String> getConfigProperties() {
    Iterator<Entry<String, String>> itr = EmbeddedMetastoreService.getHiveConf().iterator();
    Map<String, String> map = new HashMap<String, String>();
    while (itr.hasNext()) {
      Entry<String, String> kv = itr.next();
      map.put(kv.getKey(), kv.getValue());
    }
    return map;
  }

  private void createTestTable() throws CommandNeedRetryException {
    EmbeddedMetastoreService.executeQuery("drop table " + TEST_TABLE_NAME);
    EmbeddedMetastoreService.executeQuery("create table " + TEST_TABLE_NAME + "(mycol1 string,"
        + "mycol2 int)");
  }
}
