package org.apache.beam.sdk.io.kudu;

import com.google.common.collect.ImmutableList;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;

/** Utilities for Kudu tests. */
class KuduTestUtils {
  static final String COL_ID = "id";
  static final String COL_NAME = "name";

  static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              new ColumnSchema.ColumnSchemaBuilder(COL_ID, Type.INT64).key(true).build(),
              new ColumnSchema.ColumnSchemaBuilder(COL_NAME, Type.STRING)
                  .nullable(false)
                  .desiredBlockSize(4096)
                  .encoding(ColumnSchema.Encoding.PLAIN_ENCODING)
                  .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.NO_COMPRESSION)
                  .build()));

  static CreateTableOptions createTableOptions() {
    return new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of(COL_ID))
        .setNumReplicas(1);
  }

  /** Creates an Upsert Operation that matches the schema for each input. */
  static class GenerateUpsert implements KuduIO.FormatFunction<Long> {
    @Override
    public Operation apply(TableAndRecord<Long> input) {
      Upsert upsert = input.getTable().newUpsert();
      PartialRow row = upsert.getRow();
      row.addLong(COL_ID, input.getRecord());
      row.addString(COL_NAME, input.getRecord() + ": name");
      return upsert;
    }
  }

  /** Returns the count of rows for the given table. */
  static int rowCount(KuduTable table) throws KuduException {
    KuduScanner scanner = table.getAsyncClient().syncClient().newScannerBuilder(table).build();
    try {
      int rowCount = 0;
      while (scanner.hasMoreRows()) {
        rowCount += scanner.nextRows().getNumRows();
      }
      return rowCount;
    } finally {
      scanner.close();
    }
  }
}
