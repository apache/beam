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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;

/**
 * Utility class for HCatalogIOTest.
 */
public class HCatalogIOTestUtils {

  static final String TEST_TABLE_NAME = "mytable";
  static final int TEST_RECORDS_COUNT = 50;

  /**
   * Returns values from a column of the table as a list of strings.
   * @throws HCatException
   */
  static List<String> getRecords(Map<String, String> map) throws HCatException {
    List<String> records = new ArrayList<String>();
    ReaderContext readCntxt = getReaderContext(map);
    for (int split = 0; split < readCntxt.numSplits(); split++) {
      records.addAll(readRecords(readCntxt, split));
    }
    return records;
  }

  /**
   * Returns a ReaderContext instance for the passed datastore config params.
   * @throws HCatException
   */
   static ReaderContext getReaderContext(Map<String, String> config)
    throws HCatException {
    ReadEntity entity = new ReadEntity.Builder().withTable(TEST_TABLE_NAME).build();
    HCatReader reader = DataTransferFactory.getHCatReader(entity, config);
    ReaderContext context = reader.prepareRead();
    return context;
  }

   /**
    * Returns a WriterContext instance for the passed datastore config params.
    * @throws HCatException
    */
   static WriterContext getWriterContext(Map<String, String> config) throws HCatException {
      WriteEntity.Builder builder = new WriteEntity.Builder();
      WriteEntity entity = builder.withTable(TEST_TABLE_NAME).build();
      HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
      WriterContext context = writer.prepareWrite();
      return context;
    }

   private static List<String> readRecords(ReaderContext cntxt, int splitId) throws
   HCatException {
     List<String> records = new ArrayList<String>();
    HCatReader reader = DataTransferFactory.getHCatReader(cntxt, splitId);
    Iterator<HCatRecord> itr = reader.read();
    while (itr.hasNext()) {
      //returning the value of the first column i.e. get(0) as
      //this should be enough for the testcase
      records.add(itr.next().get(0).toString());
    }
    return records;
  }

   /**
    * Writes records to the table using the passed WriterContext.
    * @throws HCatException
    */
   static void writeRecords(WriterContext context) throws HCatException {
    HCatWriter writer = DataTransferFactory.getHCatWriter(context);
    writer.write(getHCatRecords(TEST_RECORDS_COUNT).iterator());
  }

   /**
    * Commits the pending writes to the database.
    * @throws IOException
    */
   static void commitRecords(Map<String, String> config, WriterContext context)
       throws IOException {
    WriteEntity.Builder builder = new WriteEntity.Builder();
    WriteEntity entity = builder.withTable(TEST_TABLE_NAME).build();
    HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
    writer.commit(context);
  }

   /**
    * Returns a list of strings containing 'expected' test data for verification.
    */
   static List<String> getExpectedRecords(int count) {
     List<String> expected = new ArrayList<String>();
     for (int i = 0; i < count; i++) {
       expected.add("record " + i);
     }
     return expected;
   }

   /**
    * Returns a list of HCatRecords of passed size.
    */
   private static List<HCatRecord> getHCatRecords(int size) {
     List<HCatRecord> expected = new ArrayList<HCatRecord>();
     for (int i = 0; i < size; i++) {
       expected.add(toHCatRecord(i));
     }
     return expected;
   }

   /**
    * Returns a list of DefaultHCatRecords of passed size.
    */
   static List<DefaultHCatRecord> getDefaultHCatRecords(int size) {
     List<DefaultHCatRecord> expected = new ArrayList<DefaultHCatRecord>();
     for (int i = 0; i < size; i++) {
       expected.add(toHCatRecord(i));
     }
     return expected;
   }
   /**
    * Inserts data into test datastore.
    * @throws Exception
    */
   static void prepareTestData() throws Exception {
     reCreateTestTable();
     Map<String, String> map = getConfigProperties();

     WriterContext cntxt = getWriterContext(map);

     writeRecords(cntxt);
     commitRecords(map, cntxt);
   }

   /**
    * Returns config params for the test datastore.
    */
   static Map<String, String> getConfigProperties() {
     Iterator<Entry<String, String>> itr = EmbeddedMetastoreService.getHiveConf().iterator();
     Map<String, String> map = new HashMap<String, String>();
     while (itr.hasNext()) {
       Entry<String, String> kv = itr.next();
       map.put(kv.getKey(), kv.getValue());
     }
     return map;
   }

   /**
    * Drops and re-creates a table by the name TEST_TABLE_NAME.
    * @throws CommandNeedRetryException
    */
  static void reCreateTestTable() throws CommandNeedRetryException {
   EmbeddedMetastoreService.executeQuery("drop table " + TEST_TABLE_NAME);
   EmbeddedMetastoreService.executeQuery("create table " + TEST_TABLE_NAME + "(mycol1 string,"
       + "mycol2 int)");
  }
   /**
    * returns a DefaultHCatRecord instance for passed value.
    */
   static DefaultHCatRecord toHCatRecord(int value) {
    return new DefaultHCatRecord(Arrays.<Object>asList("record " + value, value));
  }
}
