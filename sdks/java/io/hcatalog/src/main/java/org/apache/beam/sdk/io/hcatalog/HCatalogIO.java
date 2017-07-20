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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
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
 * <h3>Reading using HCatalog</h3>
 *
 * <p>HCatalog source supports reading of HCatRecord from a HCatalog managed source, for eg. Hive.
 *
 * <p>To configure a HCatalog source, you must specify a metastore URI and a table name. Other
 * optional parameters are database &amp; filter For instance:
 *
 * <pre>{@code
 * Map<String, String> configProperties = new HashMap<String, String>();
 * configProperties.put("hive.metastore.uris","thrift://metastore-host:port");
 *
 * pipeline
 *   .apply(HCatalogIO.read()
 *       .withConfigProperties(configProperties)
 *       .withDatabase("default") //optional, assumes default if none specified
 *       .withTable("employee")
 *       .withFilter(filterString) //optional, may be specified if the table is partitioned
 * }</pre>
 *
 * <h3>Writing using HCatalog</h3>
 *
 * <p>HCatalog sink supports writing of HCatRecord to a HCatalog managed source, for eg. Hive.
 *
 * <p>To configure a HCatalog sink, you must specify a metastore URI and a table name. Other
 * optional parameters are database, partition &amp; batchsize The destination table should exist
 * beforehand, the transform does not create a new table if it does not exist For instance:
 *
 * <pre>{@code
 * Map<String, String> configProperties = new HashMap<String, String>();
 * configProperties.put("hive.metastore.uris","thrift://metastore-host:port");
 *
 * pipeline
 *   .apply(...)
 *   .apply(HiveIO.write()
 *       .withConfigProperties(configProperties)
 *       .withDatabase("default") //optional, assumes default if none specified
 *       .withTable("employee")
 *       .withPartition(partitionValues) //optional, may be specified if the table is partitioned
 *       .withBatchSize(1024L)) //optional, assumes a default batch size of 1024 if none specified
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class HCatalogIO {

  private static final Logger LOG = LoggerFactory.getLogger(HCatalogIO.class);

  private static final long BATCH_SIZE = 1024L;
  private static final String DEFAULT_DATABASE = "default";

  /** Write data to Hive. */
  public static Write write() {
    return new AutoValue_HCatalogIO_Write.Builder().setBatchSize(BATCH_SIZE).build();
  }

  /** Read data from Hive. */
  public static Read read() {
    return new AutoValue_HCatalogIO_Read.Builder().setDatabase(DEFAULT_DATABASE).build();
  }

  private HCatalogIO() {}

  /** A {@link PTransform} to read data using HCatalog. */
  @VisibleForTesting
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<HCatRecord>> {
    @Nullable abstract Map<String, String> getConfigProperties();
    @Nullable abstract String getDatabase();
    @Nullable abstract String getTable();
    @Nullable abstract String getFilter();
    @Nullable abstract ReaderContext getContext();
    @Nullable abstract Integer getSplitId();
    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConfigProperties(Map<String, String> configProperties);
      abstract Builder setDatabase(String database);
      abstract Builder setTable(String table);
      abstract Builder setFilter(String filter);
      abstract Builder setSplitId(Integer splitId);
      abstract Builder setContext(ReaderContext context);
      abstract Read build();
    }

    /** Sets the configuration properties like metastore URI. */
    public Read withConfigProperties(Map<String, String> configProperties) {
      return toBuilder().setConfigProperties(new HashMap<>(configProperties)).build();
    }

    /** Sets the database name. This is optional, assumes 'default' database if none specified */
    public Read withDatabase(String database) {
      return toBuilder().setDatabase(database).build();
    }

    /** Sets the table name to read from. */
    public Read withTable(String table) {
      return toBuilder().setTable(table).build();
    }

    /** Sets the filter details. This is optional, assumes none if not specified */
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
    public PCollection<HCatRecord> expand(PBegin input) {
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

  /** A HCatalog {@link BoundedSource} reading {@link HCatRecord} from a given instance. */
  @VisibleForTesting
  static class BoundedHCatalogSource extends BoundedSource<HCatRecord> {
    private final Read spec;

    BoundedHCatalogSource(Read spec) {
      this.spec = spec;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Coder<HCatRecord> getDefaultOutputCoder() {
      return (Coder) WritableCoder.of(DefaultHCatRecord.class);
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
    public BoundedReader<HCatRecord> createReader(PipelineOptions options) {
      return new BoundedHCatalogReader(this);
    }

    /**
     * Returns the size of the table in bytes, does not take into consideration filter/partition
     * details passed, if any.
     */
    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
      Configuration conf = new Configuration();
      for (Entry<String, String> entry : spec.getConfigProperties().entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
      IMetaStoreClient client = null;
      try {
        HiveConf hiveConf = HCatUtil.getHiveConf(conf);
        client = HCatUtil.getHiveMetastoreClient(hiveConf);
        Table table = HCatUtil.getTable(client, spec.getDatabase(), spec.getTable());
        return StatsUtils.getFileSizeForTable(hiveConf, table);
      } finally {
        // IMetaStoreClient is not AutoCloseable, closing it manually
        if (client != null) {
          client.close();
        }
      }
    }

    /**
     * Calculates the 'desired' number of splits based on desiredBundleSizeBytes which is passed as
     * a hint to native API. Retrieves the actual splits generated by native API, which could be
     * different from the 'desired' split count calculated using desiredBundleSizeBytes
     */
    @Override
    public List<BoundedSource<HCatRecord>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      int desiredSplitCount = 1;
      long estimatedSizeBytes = getEstimatedSizeBytes(options);
      if (desiredBundleSizeBytes > 0 && estimatedSizeBytes > 0) {
        desiredSplitCount = (int) Math.ceil((double) estimatedSizeBytes / desiredBundleSizeBytes);
      }
      ReaderContext readerContext = getReaderContext(desiredSplitCount);
      //process the splits returned by native API
      //this could be different from 'desiredSplitCount' calculated above
      LOG.info(
          "Splitting into bundles of {} bytes: "
              + "estimated size {}, desired split count {}, actual split count {}",
          desiredBundleSizeBytes,
          estimatedSizeBytes,
          desiredSplitCount,
          readerContext.numSplits());

      List<BoundedSource<HCatRecord>> res = new ArrayList<>();
      for (int split = 0; split < readerContext.numSplits(); split++) {
        res.add(new BoundedHCatalogSource(spec.withContext(readerContext).withSplitId(split)));
      }
      return res;
    }

    private ReaderContext getReaderContext(long desiredSplitCount) throws HCatException {
      ReadEntity entity =
          new ReadEntity.Builder()
              .withDatabase(spec.getDatabase())
              .withTable(spec.getTable())
              .withFilter(spec.getFilter())
              .build();
      // pass the 'desired' split count as an hint to the API
      Map<String, String> configProps = new HashMap<>(spec.getConfigProperties());
      configProps.put(
          HCatConstants.HCAT_DESIRED_PARTITION_NUM_SPLITS, String.valueOf(desiredSplitCount));
      return DataTransferFactory.getHCatReader(entity, configProps).prepareRead();
    }

    static class BoundedHCatalogReader extends BoundedSource.BoundedReader<HCatRecord> {
      private final BoundedHCatalogSource source;
      private HCatRecord current;
      private Iterator<HCatRecord> hcatIterator;

      public BoundedHCatalogReader(BoundedHCatalogSource source) {
        this.source = source;
      }

      @Override
      public boolean start() throws HCatException {
        HCatReader reader =
            DataTransferFactory.getHCatReader(source.spec.getContext(), source.spec.getSplitId());
        hcatIterator = reader.read();
        return advance();
      }

      @Override
      public boolean advance() {
        if (hcatIterator.hasNext()) {
          current = hcatIterator.next();
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
      public HCatRecord getCurrent() {
        if (current == null) {
          throw new NoSuchElementException("Current element is null");
        }
        return current;
      }

      @Override
      public void close() {
        // nothing to close/release
      }
    }
  }

  /** A {@link PTransform} to write to a HCatalog managed source. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<HCatRecord>, PDone> {
    @Nullable abstract Map<String, String> getConfigProperties();
    @Nullable abstract String getDatabase();
    @Nullable abstract String getTable();
    @Nullable abstract Map<String, String> getPartition();
    abstract long getBatchSize();
    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConfigProperties(Map<String, String> configProperties);
      abstract Builder setDatabase(String database);
      abstract Builder setTable(String table);
      abstract Builder setPartition(Map<String, String> partition);
      abstract Builder setBatchSize(long batchSize);
      abstract Write build();
    }

    /** Sets the configuration properties like metastore URI. */
    public Write withConfigProperties(Map<String, String> configProperties) {
      return toBuilder().setConfigProperties(new HashMap<>(configProperties)).build();
    }

    /** Sets the database name. This is optional, assumes 'default' database if none specified */
    public Write withDatabase(String database) {
      return toBuilder().setDatabase(database).build();
    }

    /** Sets the table name to write to, the table should exist beforehand. */
    public Write withTable(String table) {
      return toBuilder().setTable(table).build();
    }

    /** Sets the partition details. */
    public Write withPartition(Map<String, String> partition) {
      return toBuilder().setPartition(partition).build();
    }

    /**
     * Sets batch size for the write operation. This is optional, assumes a default batch size of
     * 1024 if not set
     */
    public Write withBatchSize(long batchSize) {
      return toBuilder().setBatchSize(batchSize).build();
    }

    @Override
    public PDone expand(PCollection<HCatRecord> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(getConfigProperties(), "configProperties");
      checkNotNull(getTable(), "table");
    }

    private static class WriteFn extends DoFn<HCatRecord, Void> {
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
        builder.addIfNotNull(DisplayData.item("partition", String.valueOf(spec.getPartition())));
        builder.add(DisplayData.item("configProperties", spec.getConfigProperties().toString()));
        builder.add(DisplayData.item("batchSize", spec.getBatchSize()));
      }

      @Setup
      public void initiateWrite() throws HCatException {
        WriteEntity entity =
            new WriteEntity.Builder()
                .withDatabase(spec.getDatabase())
                .withTable(spec.getTable())
                .withPartition(spec.getPartition())
                .build();
        masterWriter = DataTransferFactory.getHCatWriter(entity, spec.getConfigProperties());
        writerContext = masterWriter.prepareWrite();
        slaveWriter = DataTransferFactory.getHCatWriter(writerContext);
      }

      @StartBundle
      public void startBundle() {
        hCatRecordsBatch = new ArrayList<>();
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
        if (hCatRecordsBatch.isEmpty()) {
          return;
        }
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

      @Teardown
      public void tearDown() throws Exception {
        if (slaveWriter != null) {
          slaveWriter = null;
        }
        if (masterWriter != null) {
          masterWriter = null;
        }
        if (writerContext != null) {
          writerContext = null;
        }
      }
    }
  }
}
