package org.apache.beam.sdk.io.kudu;

import org.apache.kudu.client.KuduTable;

/**
 * A wrapper for a {@link KuduTable} and the {@link T} representing a typed record.
 *
 * @param <T> The type of the record
 */
public class TableAndRecord<T> {
  private final KuduTable table;
  private final T record;

  public TableAndRecord(KuduTable table, T record) {
    this.table = table;
    this.record = record;
  }

  public KuduTable getTable() {
    return table;
  }

  public T getRecord() {
    return record;
  }
}
