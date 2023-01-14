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
package org.apache.beam.sdk.io.hcatalog.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

/** Utility class for HCatalogIOTest and HCatalogIOIT. */
@Internal
public class HCatalogIOTestUtils {
  public static final String TEST_DATABASE = "default";
  public static final String TEST_TABLE = "mytable";
  public static final String TEST_FILTER = "myfilter";
  public static final int TEST_RECORDS_COUNT = 1000;

  private static final ReadEntity READ_ENTITY =
      new ReadEntity.Builder().withTable(TEST_TABLE).build();
  private static final WriteEntity WRITE_ENTITY =
      new WriteEntity.Builder().withTable(TEST_TABLE).build();

  /** Returns a ReaderContext instance for the passed datastore config params. */
  public static ReaderContext getReaderContext(Map<String, String> config) throws HCatException {
    return DataTransferFactory.getHCatReader(READ_ENTITY, config).prepareRead();
  }

  /** Returns a WriterContext instance for the passed datastore config params. */
  private static WriterContext getWriterContext(Map<String, String> config) throws HCatException {
    return DataTransferFactory.getHCatWriter(WRITE_ENTITY, config).prepareWrite();
  }

  /** Writes records to the table using the passed WriterContext. */
  private static void writeRecords(WriterContext context) throws HCatException {
    DataTransferFactory.getHCatWriter(context)
        .write(buildHCatRecords(TEST_RECORDS_COUNT).iterator());
  }

  /** Commits the pending writes to the database. */
  private static void commitRecords(Map<String, String> config, WriterContext context)
      throws IOException {
    DataTransferFactory.getHCatWriter(WRITE_ENTITY, config).commit(context);
  }

  /** Returns a list of strings containing 'expected' test data for verification. */
  public static List<String> getExpectedRecords(int count) {
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      expected.add("record " + i);
    }
    return expected;
  }

  /**
   * Returns a list of KV&lt;String, Integer&gt; containing 'expected' test data for verification.
   */
  public static List<KV<String, Integer>> getExpectedRecordsAsKV(int count) {
    List<KV<String, Integer>> expected = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      expected.add(KV.of("record " + i, i));
    }
    return expected;
  }

  /** Returns a list of HCatRecords of passed size. */
  public static List<HCatRecord> buildHCatRecords(int size) {
    List<HCatRecord> expected = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      expected.add(toHCatRecord(i));
    }
    return expected;
  }

  /** Inserts data into test datastore. */
  public static void insertTestData(Map<String, String> configMap) throws Exception {
    WriterContext cntxt = getWriterContext(configMap);
    writeRecords(cntxt);
    commitRecords(configMap, cntxt);
  }

  /** Returns config params for the test datastore as a Map. */
  public static Map<String, String> getConfigPropertiesAsMap(HiveConf hiveConf) {
    Map<String, String> map = new HashMap<>();
    for (Entry<String, String> kv : hiveConf) {
      map.put(kv.getKey(), kv.getValue());
    }
    return map;
  }

  /** returns a DefaultHCatRecord instance for passed value. */
  private static DefaultHCatRecord toHCatRecord(int value) {
    return new DefaultHCatRecord(Arrays.asList("record " + value, value));
  }
}
