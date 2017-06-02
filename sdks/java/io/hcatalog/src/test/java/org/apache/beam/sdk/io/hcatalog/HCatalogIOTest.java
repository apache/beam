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

import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.TEST_RECORDS_COUNT;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.TEST_TABLE_NAME;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.getConfigProperties;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.getDefaultHCatRecords;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.getExpectedRecords;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.getReaderContext;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.prepareTestData;
import static org.apache.beam.sdk.io.hcatalog.HCatalogIOTestUtils.reCreateTestTable;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.isA;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.hcatalog.HCatalogIO.BoundedHCatalogSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;


/**
 *Test for HCatalogIO.
 */
public class HCatalogIOTest implements Serializable {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Rule
  public final transient TestRule testDataSetupRule = new TestWatcher() {
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          if (description.getAnnotation(TestDataSetupReqd.class) != null) {
            prepareTestData();
          } else if (description.getAnnotation(TestDataInitializationReqd.class) != null) {
            reCreateTestTable();
          }
          base.evaluate();
        }
      };
    }
  };

  /**
   * Use this annotation to setup complete test data(table populated with records).
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  @interface TestDataSetupReqd {
  }

  /**
   * Use this annotation to setup test tables alone(empty tables, no records are populated).
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  @interface TestDataInitializationReqd {
  }


  /**
   * Perform end-to-end test of Write-then-Read operation.
   */
  @Test
  @TestDataInitializationReqd
  public void testWriteWriteThenReadSuccess() throws Exception {
    pipeline.apply(Create.of(getDefaultHCatRecords(TEST_RECORDS_COUNT)))
    .apply(HCatalogIO.write()
        .withConfigProperties(getConfigProperties())
        .withTable(TEST_TABLE_NAME));

    pipeline.run();

    PCollection<String> output = pipeline.apply(HCatalogIO.read()
        .withConfigProperties(getConfigProperties())
        .withTable(HCatalogIOTestUtils.TEST_TABLE_NAME))
        .apply(ParDo.<DefaultHCatRecord, String>of(new DoFn<DefaultHCatRecord, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(c.element().get(0).toString());
          }
        }));
    PAssert.that(output).containsInAnyOrder(getExpectedRecords(TEST_RECORDS_COUNT));
    pipeline.run();
  }

  /**
   * Test of Write to a non-existent table.
   */
  @Test
  public void testWriteFailureTableDoesNotExist() throws Exception {
    thrown.expectCause(isA(UserCodeException.class));
    thrown.expectMessage(containsString("org.apache.hive.hcatalog.common.HCatException"));
    thrown.expectMessage(containsString("NoSuchObjectException"));
    pipeline.apply(Create.of(getDefaultHCatRecords(TEST_RECORDS_COUNT)))
        .apply(HCatalogIO.write()
            .withConfigProperties(getConfigProperties())
            .withTable("myowntable"));
    pipeline.run();
  }

  /**
   * Test of Write without specifying a table.
   */
  @Test
  public void testWriteFailureValidationTable() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(containsString("table"));
    HCatalogIO.write().withConfigProperties(getConfigProperties()).validate(null);
  }

  /**
   * Test of Write without specifying configuration properties.
   */
  @Test
  public void testWriteFailureValidationConfigProp() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(containsString("configProperties"));
    HCatalogIO.write().withTable("myowntable").validate(null);
  }

  /**
   * Test of Read from a non-existent table.
   */
  @Test
  public void testReadFailureTableDoesNotExist() throws Exception {
    pipeline.apply(HCatalogIO.read()
        .withConfigProperties(getConfigProperties())
        .withTable("myowntable"));
    thrown.expectCause(isA(NoSuchObjectException.class));
    pipeline.run();
  }

  /**
   * Test of Read without specifying configuration properties.
   */
  @Test
  public void testReadFailureValidationConfig() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(containsString("configProperties"));
    HCatalogIO.read().withTable("myowntable").validate(null);
  }

  /**
   * Test of Read without specifying a table.
   */
  @Test
  public void testReadFailureValidationTable() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(containsString("table"));
    HCatalogIO.read().withConfigProperties(getConfigProperties()).validate(null);
  }

  /**
   * Test of Read using SourceTestUtils.readFromSource(..).
   */
  @Test
  @TestDataSetupReqd
  public void testReadFromSource() throws Exception {
    List<BoundedHCatalogSource> sourceList = getSourceList();
    List<String> recordsList = new ArrayList<String>();
    for (int i = 0; i < sourceList.size(); i++) {
      List<DefaultHCatRecord> hcatRecordsList = SourceTestUtils.readFromSource(sourceList.get(i),
        PipelineOptionsFactory.create());
      for (HCatRecord record : hcatRecordsList) {
        recordsList.add(record.get(0).toString());
      }
    }
    Assert.assertThat(getExpectedRecords(TEST_RECORDS_COUNT),
        Matchers.containsInAnyOrder(recordsList.toArray()));
  }

  /**
   * Test of Read using SourceTestUtils.assertSourcesEqualReferenceSource(..).
   */
  @Test
  @TestDataSetupReqd
  public void testSourcesEqualReferenceSource() throws Exception {
    List<BoundedHCatalogSource> sourceList = getSourceList();
    for (int i = 0; i < sourceList.size(); i++) {
      SourceTestUtils.assertSourcesEqualReferenceSource(sourceList.get(i),
          sourceList.get(i).split(-1, PipelineOptionsFactory.create()),
          PipelineOptionsFactory.create());
    }
  }

  private List<BoundedHCatalogSource> getSourceList() throws Exception {
    List<BoundedHCatalogSource> sourceList = new ArrayList<BoundedHCatalogSource>();
    ReaderContext context = getReaderContext(getConfigProperties());
    HCatalogIO.Read spec = HCatalogIO.read()
        .withConfigProperties(getConfigProperties())
        .withContext(context)
        .withTable(TEST_TABLE_NAME);

    for (int i = 0; i < context.numSplits(); i++) {
      BoundedHCatalogSource source = new BoundedHCatalogSource(spec.withSplitId(i));
      sourceList.add(source);
    }
    return sourceList;
  }
}
