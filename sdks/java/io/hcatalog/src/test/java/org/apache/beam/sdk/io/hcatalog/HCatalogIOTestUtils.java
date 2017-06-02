package org.apache.beam.sdk.io.hcatalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

  /**
   * Returns the count of records read from the table.
   * @throws HCatException
   */
  static int getRecordsCount(Map<String, String> map) throws HCatException {
    ReaderContext readCntxt = getReaderContext(map);
    int recordsCount = 0;
    for (int i = 0; i < readCntxt.numSplits(); i++) {
      recordsCount += readRecords(readCntxt, i);
    }
    return recordsCount;
  }

  /**
   * Returns ReaderContext object for the passed configuration parameters.
   * @throws HCatException
   */
   static ReaderContext getReaderContext(Map<String, String> config)
    throws HCatException {
    ReadEntity entity = new ReadEntity.Builder().withTable(TEST_TABLE_NAME).build();
    HCatReader reader = DataTransferFactory.getHCatReader(entity, config);
    ReaderContext cntxt = reader.prepareRead();
    return cntxt;
  }

   /**
    * Returns WriterContext object for the passed configuration parameters.
    * @throws HCatException
    */
   static WriterContext getWriterContext(Map<String, String> config) throws HCatException {
      WriteEntity.Builder builder = new WriteEntity.Builder();
      WriteEntity entity = builder.withTable(TEST_TABLE_NAME).build();
      HCatWriter writer = DataTransferFactory.getHCatWriter(entity, config);
      WriterContext info = writer.prepareWrite();
      return info;
    }

   private static int readRecords(ReaderContext cntxt, int slaveNum) throws  HCatException {
    HCatReader reader = DataTransferFactory.getHCatReader(cntxt, slaveNum);
    Iterator<HCatRecord> itr = reader.read();
    int noOfRecords = 0;
    while (itr.hasNext()) {
      ++noOfRecords;
    }
    return noOfRecords;
  }

   /**
    * Writes records to the table using the passed WriterContext.
    * @throws HCatException
    */
   static void writeRecords(WriterContext context) throws HCatException {
    HCatWriter writer = DataTransferFactory.getHCatWriter(context);
    writer.write(new HCatRecordItr());
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
    * Constructs and returns a DefaultHCatRecord for passed value.
    */
   static DefaultHCatRecord toHCatRecord(int value) {
    List<Object> dataList = new ArrayList<Object>(2);
    dataList.add("Record " + value);
    dataList.add(value);
    return new DefaultHCatRecord(dataList);
  }

  static class HCatRecordItr implements Iterator<HCatRecord> {

    int counter = 0;

    @Override
    public boolean hasNext() {
      return counter++ < 100 ? true : false;
    }

    @Override
    public HCatRecord next() {
      return toHCatRecord(counter);
    }

    @Override
    public void remove() {
      throw new RuntimeException();
    }
  }
}
