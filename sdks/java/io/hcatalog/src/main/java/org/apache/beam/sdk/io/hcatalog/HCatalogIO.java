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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write data using HCatalog.
 *
 *
 * <h3>Reading using HCatalog</h3>
 *
 * <p>HCatalog source supports reading of DefaultHCatRecord from a HCatalog managed source,
 * for eg. Hive.</p>
 *
 * <p>To configure a HCatalog source, you must specify a metastore URI and a table name.
 * Other optional parameters are database &amp; filter
 * For instance:</p>
 *
 * <pre>{@code
 *
 * Map<String, String> configProperties = new HashMap<String, String>();
 * configProperties.put("hive.metastore.uris","thrift://metastore-host:port");
 *
 * pipeline
 *
 *   .apply(HCatalogIO.read()
 *       .withConfigProperties(configProperties) //mandatory
 *       .withTable("employee") //mandatory
 *       .withDatabase("default") //optional, assumes default if none specified
 *       .withFilter(filterString) //optional,
 *       should be specified if the table is partitioned
 * }</pre>
 *
 *
 *
 * <h3>Writing using HCatalog</h3>
 *
 * <p>HCatalog sink supports writing of DefaultHCatRecord to a HCatalog managed source,
 * for eg. Hive.</p>
 *
 * <p>To configure a HCatalog sink, you must specify a metastore URI and a table name.
 * Other optional parameters are database, partition &amp; batchsize
 * The destination table should exist beforehand, the transform does not create a
 * new table if it does not exist
 * For instance:</p>
 *
 * <pre>{@code
 *
 * Map<String, String> configProperties = new HashMap<String, String>();
 * configProperties.put("hive.metastore.uris","thrift://metastore-host:port");
 *
 * pipeline

 *   .apply(...)
 *   .apply(HiveIO.write()
 *       .withConfigProperties(configProperties) //mandatory
 *       .withTable("employee") //mandatory
 *       .withDatabase("default") //optional, assumes default if none specified
 *       .withFilter(partitionValues) //optional,
 *       should be specified if the table is partitioned
 *       .withBatchSize(1024L)) //optional,
 *       assumes a default batch size of 1024 if none specified
 * }</pre>
 *

 */
@Experimental
public class HCatalogIO {

  private static final Logger LOG = LoggerFactory.getLogger(HCatalogIO.class);

  /** Write data to Hive. */
  public static Write write() {
    return new AutoValue_HCatalogIO_Write.Builder().setBatchSize(1024L).build();
  }

  /** Read data from Hive. */
  public static Read read() {
    return new AutoValue_HCatalogIO_Read.Builder().build();
  }

  private HCatalogIO() {
  }


  /**
   * A {@link PTransform} to read data using HCatalog.
   */
  @VisibleForTesting
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<DefaultHCatRecord>> {
    @Nullable abstract Map<String, String> getConfigProperties();
    @Nullable abstract String getDatabase();
    @Nullable abstract String getTable();
    @Nullable abstract String getFilter();
    @Nullable abstract Coder<DefaultHCatRecord> getCoder();
    @Nullable abstract ReaderContext getContext();
    @Nullable abstract Integer getSplitId();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConfigProperties(Map<String, String> configProperties);
      abstract Builder setDatabase(String database);
      abstract Builder setTable(String table);
      abstract Builder setFilter(String filter);
      abstract Builder setCoder(Coder<DefaultHCatRecord> coder);
      abstract Builder setSplitId(Integer splitId);
      abstract Builder setContext(ReaderContext context);
      abstract Read build();
    }

    /**
     * Sets the configuration properties like metastore URI.
     * This is mandatory
     * @param configProperties
     * @return
     */
    public Read withConfigProperties(Map<String, String> configProperties) {
      return toBuilder().setConfigProperties(Collections.unmodifiableMap(configProperties))
          .build();
    }

    /**
     * Sets the database name.
     * This is optional, assumes 'default' database if none specified
     * @param database
     * @return
     */
    public Read withDatabase(String database) {
      return toBuilder().setDatabase(database).build();
    }

    /**
     * Sets the table name to read from.
     * This is mandatory
     * @param table
     * @return
     */
    public Read withTable(String table) {
      return toBuilder().setTable(table).build();
    }

    /**
     * Sets the filter (partition) details.
     * This is optional, assumes none if not specified
     * @param filter
     * @return
     */
    public Read withFilter(String filter) {
      return toBuilder().setFilter(filter).build();
    }

    Read withSplitId(int splitId) {
      checkArgument(splitId >= 0, "Invalid split id-" + splitId);
      return toBuilder().setSplitId(splitId).build();
    }

    Read withContext(ReaderContext context) {
      return toBuilder().setContext(context).build();
    }

    @Override
    public PCollection<DefaultHCatRecord> expand(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(new BoundedHCatalogSource(this)));
    }

    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(getTable(), "table");
      checkNotNull(getConfigProperties(), "configProperties");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("configProperties", getConfigProperties().toString()));
      builder.add(DisplayData.item("table", getTable()));
      builder.addIfNotNull(DisplayData.item("database", getDatabase()));
      builder.addIfNotNull(DisplayData.item("filter", getFilter()));
    }
  }

  /**
   * A HCatalog {@link BoundedSource} reading {@link DefaultHCatRecord} from  a given instance.
   */
  @VisibleForTesting
  static class BoundedHCatalogSource extends BoundedSource<DefaultHCatRecord> {
    private Read spec;

    BoundedHCatalogSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public Coder<DefaultHCatRecord> getDefaultOutputCoder() {
      return WritableCoder.of(DefaultHCatRecord.class);
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
    }

    @Override
    public BoundedReader<DefaultHCatRecord> createReader(PipelineOptions options) {
      return new BoundedHCatalogReader(this);
    }

    /**
     * This implementation simply returns 0L as this is not used in calculating the splits.
     */
    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) {
      return 0L;
    }

    /**
     * This implementation does not take into consideration desiredBundleSizeBytes
     * or getEstimatedSizeBytes().
     * Number of splits returned by the native API is used instead.
     * @throws HCatException
     */
    @Override
    public List<BoundedSource<DefaultHCatRecord>> split(long desiredBundleSizeBytes,
                                                PipelineOptions options) throws HCatException {
      List<BoundedSource<DefaultHCatRecord>> sources = new ArrayList<>();
      ReaderContext readerContext = getReaderContext();
      for (int i = 0; i < readerContext.numSplits(); i++) {
        sources.add(new BoundedHCatalogSource(spec.withSplitId(i).withContext(readerContext)));
      }
      return sources;
    }

    private ReaderContext getReaderContext() throws HCatException {
      ReadEntity entity = new ReadEntity.Builder().withDatabase(spec.getDatabase())
          .withTable(spec.getTable()).withFilter(spec.getFilter()).build();
      HCatReader masterReader = DataTransferFactory.getHCatReader(entity,
          spec.getConfigProperties());
      ReaderContext readerContext = null;
      readerContext = masterReader.prepareRead();
      return readerContext;
    }

    static class BoundedHCatalogReader extends BoundedSource.BoundedReader<DefaultHCatRecord> {
      private final BoundedHCatalogSource source;
      private DefaultHCatRecord current;
      Iterator<HCatRecord> hcatIterator;

      public BoundedHCatalogReader(BoundedHCatalogSource boundedHiveSource) {
        this.source = boundedHiveSource;
      }

      @Override
      public boolean start() throws HCatException {
        HCatReader reader = DataTransferFactory.getHCatReader(source.spec.getContext(),
            source.spec.getSplitId());
        hcatIterator = reader.read();
        return advance();
      }

      @Override
      public boolean advance() {
        if (hcatIterator.hasNext()) {
          current = (DefaultHCatRecord) hcatIterator.next();
          return true;
        } else {
          current = null;
          return false;
        }
      }

      @Override
      public BoundedHCatalogSource getCurrentSource() {
        return source;
      }

      @Override
      public DefaultHCatRecord getCurrent() {
        if (current == null) {
          throw new NoSuchElementException ("Current element is null");
        }
        return current;
      }

      @Override
      public void close() {
        //nothing to close/release
      }
    }
  }

  /**
   * A {@link PTransform} to write to a HCatalog managed source.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<DefaultHCatRecord>, PDone> {
    @Nullable abstract Map<String, String> getConfigProperties();
    @Nullable abstract String getDatabase();
    @Nullable abstract String getTable();
    @Nullable abstract Map getFilter();
    abstract long getBatchSize();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConfigProperties(Map<String, String> configProperties);
      abstract Builder setDatabase(String database);
      abstract Builder setTable(String table);
      abstract Builder setFilter(Map partition);
      abstract Builder setBatchSize(long batchSize);
      abstract Write build();
    }

    /**
     * Sets the configuration properties like metastore URI.
     * This is mandatory
     * @param metastoreUri
     * @return
     */
    public Write withConfigProperties(Map<String, String> configProperties) {
      return toBuilder().setConfigProperties(Collections.unmodifiableMap(configProperties))
          .build();
    }

    /**
     * Sets the database name.
     * This is optional, assumes 'default' database if none specified
     * @param database
     * @return
     */
    public Write withDatabase(String database) {
      return toBuilder().setDatabase(database).build();
    }

    /**
     * Sets the table name to write to, the table should exist beforehand.
     * This is mandatory
     * @param table
     * @return
     */
    public Write withTable(String table) {
      return toBuilder().setTable(table).build();
    }

    /**
     * Sets the filter (partition) details.
     * This is required if the table is partitioned
     * @param filter
     * @return
     */
    public Write withFilter(Map filter) {
      return toBuilder().setFilter(filter).build();
    }

    /**
     * Sets batch size for the write operation.
     * This is optional, assumes a default batch size of 1024 if not set
     * @param batchSize
     * @return
     */
    public Write withBatchSize(long batchSize) {
      return toBuilder().setBatchSize(batchSize).build();
    }

    @Override
    public PDone expand(PCollection<DefaultHCatRecord> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(getConfigProperties(), "configProperties");
      checkNotNull(getTable(), "table");
    }

    private static class WriteFn extends DoFn<DefaultHCatRecord, Void> {
      private final Write spec;
      private WriterContext writerContext;
      private HCatWriter slaveWriter;
      private HCatWriter masterWriter;
      private List<HCatRecord> hCatRecordsBatch;

      public WriteFn(Write spec) {
        this.spec = spec;
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.addIfNotNull(DisplayData.item("database", spec.getDatabase()));
        builder.add(DisplayData.item("table", spec.getTable()));
        builder.addIfNotNull(DisplayData.item("filter", String.valueOf(spec.getFilter())));
        builder.add(DisplayData.item("configProperties", spec.getConfigProperties().toString()));
        builder.add(DisplayData.item("batchSize", spec.getBatchSize()));
      }

      @Setup
      public void initiateWrite() throws HCatException {
        writerContext = getWriterContext();
        slaveWriter = DataTransferFactory.getHCatWriter(writerContext);
      }

      @StartBundle
      public void startBundle() {
        hCatRecordsBatch = new ArrayList<HCatRecord>();
      }

      @ProcessElement
      public void processElement(ProcessContext ctx) throws HCatException {
        hCatRecordsBatch.add(ctx.element());
        if (hCatRecordsBatch.size() >= spec.getBatchSize()) {
          flush();
        }
      }

      @FinishBundle
      public void finishBundle() throws HCatException {
        flush();
      }

      private void flush() throws HCatException {
        try {
          slaveWriter.write(hCatRecordsBatch.iterator());
          masterWriter.commit(writerContext);
        } catch (HCatException e) {
          LOG.error("Exception in flush - write/commit data to Hive", e);
          //abort on exception
          masterWriter.abort(writerContext);
          throw e;
        } finally {
          hCatRecordsBatch.clear();
        }
      }

      private WriterContext getWriterContext() throws HCatException {
        WriteEntity.Builder builder = new WriteEntity.Builder();
        WriteEntity entity = builder.withDatabase(spec.getDatabase()).withTable(spec.getTable())
            .withPartition(spec.getFilter()).build();
        masterWriter = DataTransferFactory.getHCatWriter(entity, spec.getConfigProperties());
        WriterContext writerContext = masterWriter.prepareWrite();
        return writerContext;
      }
    }
  }
}
