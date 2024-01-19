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
package org.apache.beam.sdk.io.kudu;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AbstractKuduScannerBuilder;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of the {@link KuduService} that uses a Kudu instance. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class KuduServiceImpl<T> implements KuduService<T> {
  private static final Logger LOG = LoggerFactory.getLogger(KuduServiceImpl.class);

  @Override
  public Writer createWriter(KuduIO.Write<T> spec) throws KuduException {
    return new WriterImpl(spec);
  }

  @Override
  public BoundedSource.BoundedReader createReader(KuduIO.KuduSource source) {
    return new ReaderImpl(source);
  }

  @Override
  public List<byte[]> createTabletScanners(KuduIO.Read spec) throws KuduException {
    try (KuduClient client = getKuduClient(spec.getMasterAddresses())) {
      KuduTable table = client.openTable(spec.getTable());
      KuduScanToken.KuduScanTokenBuilder builder = client.newScanTokenBuilder(table);
      configureBuilder(spec, table.getSchema(), builder);
      List<KuduScanToken> tokens = builder.build();
      return tokens.stream().map(t -> uncheckCall(t::serialize)).collect(Collectors.toList());
    }
  }

  /** Writer storing an entity into Apache Kudu table. */
  class WriterImpl implements Writer<T> {
    private final KuduIO.FormatFunction<T> formatFunction;
    private KuduClient client;
    private KuduSession session;
    private KuduTable table;

    WriterImpl(KuduIO.Write<T> spec) throws KuduException {
      checkNotNull(spec.masterAddresses(), "masterAddresses cannot be null");
      checkNotNull(spec.table(), "table cannot be null");
      this.formatFunction = checkNotNull(spec.formatFn(), "formatFn cannot be null");
      client =
          new AsyncKuduClient.AsyncKuduClientBuilder(spec.masterAddresses()).build().syncClient();
      table = client.openTable(spec.table());
    }

    @Override
    public void openSession() throws KuduException {
      // errors are collected per session so we align session with the bundle
      session = client.newSession();
      // async flushing as per the official kudu-spark approach
      session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    }

    @Override
    public void write(T entity) throws KuduException {
      checkState(session != null, "must call openSession() before writing");
      session.apply(formatFunction.apply(new TableAndRecord(table, entity)));
    }

    @Override
    public void closeSession() throws Exception {
      try {
        session.close();
        if (session.countPendingErrors() > 0) {
          LOG.error("At least {} errors occurred writing to Kudu", session.countPendingErrors());
          RowError[] errors = session.getPendingErrors().getRowErrors();
          for (int i = 0; errors != null && i < 3 && i < errors.length; i++) {
            LOG.error("Sample error: {}", errors[i]);
          }
          throw new Exception(
              "At least " + session.countPendingErrors() + " error(s) occurred writing to Kudu");
        }
      } finally {
        session = null;
      }
    }

    @Override
    public void close() throws Exception {
      client.close();
      client = null;
    }
  }

  /** Bounded reader of an Apache Kudu table. */
  class ReaderImpl extends BoundedSource.BoundedReader<T> {
    private final KuduIO.KuduSource<T> source;
    private KuduClient client;
    private KuduScanner scanner;
    private RowResultIterator iter;
    private RowResult current;
    private long recordsReturned;

    ReaderImpl(KuduIO.KuduSource<T> source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      LOG.debug("Starting Kudu reader");
      client =
          new AsyncKuduClient.AsyncKuduClientBuilder(source.spec.getMasterAddresses())
              .build()
              .syncClient();

      if (source.serializedToken != null) {
        // tokens available if the source is already split
        scanner = KuduScanToken.deserializeIntoScanner(source.serializedToken, client);
      } else {
        KuduTable table = client.openTable(source.spec.getTable());
        KuduScanner.KuduScannerBuilder builder =
            table.getAsyncClient().syncClient().newScannerBuilder(table);

        configureBuilder(source.spec, table.getSchema(), builder);
        scanner = builder.build();
      }

      return advance();
    }

    /**
     * Returns the current record transformed into the desired type.
     *
     * @return the current record
     * @throws NoSuchElementException If the current does not exist
     */
    @Override
    public T getCurrent() throws NoSuchElementException {
      if (current != null) {
        return source.spec.getParseFn().apply(current);

      } else {
        throw new NoSuchElementException(
            "No current record (Indicates misuse. Perhaps advance() was not called?)");
      }
    }

    @Override
    public boolean advance() throws KuduException {
      // scanner pages over results, with each page holding an iterator of records
      if (iter == null || (!iter.hasNext() && scanner.hasMoreRows())) {
        iter = scanner.nextRows();
      }

      if (iter != null && iter.hasNext()) {
        current = iter.next();
        ++recordsReturned;
        return true;
      }

      return false;
    }

    @Override
    public void close() throws IOException {
      LOG.debug("Closing reader after reading {} records.", recordsReturned);
      if (scanner != null) {
        scanner.close();
        scanner = null;
      }
      if (client != null) {
        client.close();
        client = null;
      }
    }

    @Override
    public synchronized KuduIO.KuduSource getCurrentSource() {
      return source;
    }
  }

  /** Creates a new synchronous client. */
  private synchronized KuduClient getKuduClient(List<String> masterAddresses) {
    return new AsyncKuduClient.AsyncKuduClientBuilder(masterAddresses).build().syncClient();
  }

  /** Configures the scanner builder to conform to the spec. */
  private static <T2> void configureBuilder(
      KuduIO.Read<T2> spec, Schema schema, AbstractKuduScannerBuilder builder) {
    builder.cacheBlocks(true); // as per kudu-spark
    if (spec.getBatchSize() != null) {
      builder.batchSizeBytes(spec.getBatchSize());
    }
    if (spec.getProjectedColumns() != null) {
      builder.setProjectedColumnNames(spec.getProjectedColumns());
    }
    if (spec.getFaultTolerent() != null) {
      builder.setFaultTolerant(spec.getFaultTolerent());
    }
    if (spec.getSerializablePredicates() != null) {
      for (Common.ColumnPredicatePB predicate : spec.getSerializablePredicates()) {
        builder.addPredicate(KuduPredicate.fromPB(schema, predicate));
      }
    }
  }

  /** Wraps the callable converting checked to RuntimeExceptions. */
  private static <T> T uncheckCall(Callable<T> callable) {
    try {
      return callable.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
