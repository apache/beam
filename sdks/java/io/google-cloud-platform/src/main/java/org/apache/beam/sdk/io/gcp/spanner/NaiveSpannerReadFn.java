package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.common.annotations.VisibleForTesting;

/** A simplest read function implementation. Parallelism support is coming. */
@VisibleForTesting
class NaiveSpannerReadFn extends AbstractSpannerFn<Object, Struct> {
  private static final long serialVersionUID = 7645917508410554173L;

  private final SpannerIO.Read config;

  NaiveSpannerReadFn(SpannerIO.Read config) {
    this.config = config;
  }

  SpannerConfig getSpannerConfig() {
    return config.getSpannerConfig();
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    TimestampBound timestampBound = TimestampBound.strong();
    if (config.getTransaction() != null) {
      Transaction transaction = c.sideInput(config.getTransaction());
      timestampBound = TimestampBound.ofReadTimestamp(transaction.timestamp());
    }
    try (ReadOnlyTransaction readOnlyTransaction =
        databaseClient().readOnlyTransaction(timestampBound)) {
      ResultSet resultSet = execute(readOnlyTransaction);
      while (resultSet.next()) {
        c.output(resultSet.getCurrentRowAsStruct());
      }
    }
  }

  private ResultSet execute(ReadOnlyTransaction readOnlyTransaction) {
    if (config.getQuery() != null) {
      return readOnlyTransaction.executeQuery(config.getQuery());
    }
    if (config.getIndex() != null) {
      return readOnlyTransaction.readUsingIndex(
          config.getTable(), config.getIndex(), config.getKeySet(), config.getColumns());
    }
    return readOnlyTransaction.read(config.getTable(), config.getKeySet(), config.getColumns());
  }
}
