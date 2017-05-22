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
package org.apache.beam.sdk.io.hive;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
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
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write data on Hive.
 *
 *
 * <h3>Reading from Hive</h3>
 *
 * <p>Hive source supports reading of HCatRecord from a Hive instance.</p>
 *
 * <p>To configure a Hive source, you must specify a metastoreUri and a table name.
 * Other optional parameters are database & filter
 * For instance:</p>
 *
 * <pre>{@code
 *
 * pipeline
 *
 *   .apply(HiveIO.write()
 *       .withMetastoreUri("thrift://hadoop-clust-0118-m:9083") //mandatory
 *       .withTable("employee") //mandatory
 *       .withDatabase("default") //optional, assumes default if none specified
 *       .withFilter(filterString) //optional,
 *       should be specified if the table is partitioned
 * }</pre>
 *
 *
 *
 * <h3>Writing to Hive</h3>
 *
 * <p>Hive sink supports writing of HCatRecord to a Hive instance.</p>
 *
 * <p>To configure a Hive sink, you must specify a metastoreUri and a table name.
 * Other optional parameters are database, partition & batchsize
 * For instance:</p>
 *
 * <pre>{@code
 *
 * pipeline

 *   .apply(...)
 *   .apply(HiveIO.write()
 *       .withMetastoreUri("thrift://hadoop-clust-0118-m:9083") //mandatory
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
public class HiveIO {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIO.class);

  /** Write data to Hive. */
  public static Write write() {
    return new AutoValue_HiveIO_Write.Builder().setBatchSize(1024L).build();
  }

  /** Read data from Hive. */
  public static Read read() {
    return new AutoValue_HiveIO_Read.Builder().build();
  }

  private HiveIO() {
  }


  /**
   * A {@link PTransform} to read data from Hive.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<HCatRecord>> {
    @Nullable abstract String getMetastoreUri();
    @Nullable abstract String getDatabase();
    @Nullable abstract String getTable();
    @Nullable abstract String getFilter();
    @Nullable abstract Coder<HCatRecord> getCoder();
    @Nullable abstract ReaderContext getContext();
    @Nullable abstract Integer getSplitId();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMetastoreUri(String metastoreUri);
      abstract Builder setDatabase(String database);
      abstract Builder setTable(String table);
      abstract Builder setFilter(String filter);
      abstract Builder setCoder(Coder<HCatRecord> coder);
      abstract Builder setSplitId(Integer splitId);
      abstract Builder setContext(ReaderContext context);
      abstract Read build();
    }

    public Read withMetastoreUri(String metastoreUri) {
      return toBuilder().setMetastoreUri(metastoreUri).build();
    }

    public Read withDatabase(String database) {
      return toBuilder().setDatabase(database).build();
    }

    public Read withTable(String table) {
      return toBuilder().setTable(table).build();
    }

    public Read withFilter(String filter) {
      return toBuilder().setFilter(filter).build();
    }

    protected Read withSplitId(Integer splitId) {
      checkArgument(splitId >= 0);
      return toBuilder().setSplitId(splitId).build();
    }

    protected Read withContext(ReaderContext context) {
      return toBuilder().setContext(context).build();
    }

    @Override
    public PCollection<HCatRecord> expand(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(new BoundedHiveSource(this)));
    }

    @Override
    public void validate(PipelineOptions options) {
      checkNotNull(getTable(), "table");
      checkNotNull(getMetastoreUri(), "metastoreUri");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("metastoreUri", getMetastoreUri()));
      builder.add(DisplayData.item("table", getTable()));
      builder.addIfNotNull(DisplayData.item("database", getDatabase()));
      builder.addIfNotNull(DisplayData.item("filter", getFilter()));
    }
  }

  /**
   * A Hive {@link BoundedSource} reading {@link HCatRecord} from  a given instance.
   */
  @VisibleForTesting
  static class BoundedHiveSource extends BoundedSource<HCatRecord> {
    private Read spec;

    private BoundedHiveSource(Read spec) {
      this.spec = spec;
    }

    @Override
    public Coder<HCatRecord> getDefaultOutputCoder() {
      return HCatRecordSimpleCoder.of(spec.getMetastoreUri());
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
      return new BoundedHiveReader(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws
    IOException, NoSuchObjectException, TException {
      //TODO: This is not really used, should be removed
      Configuration conf = new Configuration();
      conf.set(HCatConstants.HCAT_METASTORE_URI, spec.getMetastoreUri());
      HiveConf hiveConf = HCatUtil.getHiveConf(conf);
      IMetaStoreClient client = HCatUtil.getHiveMetastoreClient(hiveConf);
      Table table = HCatUtil.getTable(client, spec.getDatabase(),
        spec.getTable());
      client.close();
      return StatsUtils.getFileSizeForTable(hiveConf, table);
    }

    @Override
    public List<BoundedSource<HCatRecord>> split(long desiredBundleSizeBytes,
                                                PipelineOptions options) {
      //TODO: We are really not using the bundle size at run time
      //to make the split
      //just going with the default splits returned by Hive
      //unable to explicitly control the splits returned by Hive
      List<BoundedSource<HCatRecord>> sources = new ArrayList<>();
      ReaderContext cntxt = getReaderContext();
      for (int i = 0; i < cntxt.numSplits(); i++) {
        //create an instance of BoundedHiveSource for each of the split
        //to be read in parallel
        sources.add(new BoundedHiveSource(spec.withSplitId(i).withContext(cntxt)));
      }
      return sources;
    }

    private ReaderContext getReaderContext() {
      ReadEntity entity = new ReadEntity.Builder().withDatabase(spec.getDatabase())
          .withTable(spec.getTable()).withFilter(spec.getFilter()).build();
      Map<String, String> hiveConfig = new HashMap<String, String>();
      hiveConfig.put(HCatConstants.HCAT_METASTORE_URI, spec.getMetastoreUri());
      HCatReader masterReader = DataTransferFactory.getHCatReader(entity, hiveConfig);
      ReaderContext cntxt = null;
      try {
        cntxt = masterReader.prepareRead();
        } catch (HCatException e) {
        LOG.error("Exception while preparing for read", e);
      }
      return cntxt;
    }

    private static class BoundedHiveReader extends BoundedSource.BoundedReader<HCatRecord> {
      private final BoundedHiveSource source;
      private HCatRecord current;
      Iterator<HCatRecord> hcatIterator;

      public BoundedHiveReader(BoundedHiveSource boundedHiveSource) {
        this.source = boundedHiveSource;
      }

      @Override
      public boolean start() {
        try {
          HCatReader reader = DataTransferFactory.getHCatReader(source.spec.getContext(),
              source.spec.getSplitId());
          hcatIterator = reader.read();
        } catch (HCatException e) {
          LOG.error("Exception while reading", e);
          return false;
        }
        return advance();
      }

      @Override
      public boolean advance() {
        if (hcatIterator.hasNext()) {
          current = hcatIterator.next();
          return true;
        } else {
          return false;
        }
      }

      @Override
      public BoundedHiveSource getCurrentSource() {
        return source;
      }

      @Override
      public HCatRecord getCurrent() {
        return current;
      }

      @Override
      public void close() {
        //nothing to close/release
      }
    }
  }

  /**
   * A {@link PTransform} to write to a Hive database.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<HCatRecord>, PDone> {
    @Nullable abstract String getMetastoreUri();
    @Nullable abstract String getDatabase();
    @Nullable abstract String getTable();
    @Nullable abstract Map getFilter();
    abstract long getBatchSize();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMetastoreUri(String metastoreUri);
      abstract Builder setDatabase(String database);
      abstract Builder setTable(String table);
      abstract Builder setFilter(Map partition);
      abstract Builder setBatchSize(long batchSize);
      abstract Write build();
    }

    public Write withMetastoreUri(String metastoreUri) {
      return toBuilder().setMetastoreUri(metastoreUri).build();
    }

    public Write withDatabase(String database) {
      return toBuilder().setDatabase(database).build();
    }

    public Write withTable(String table) {
      return toBuilder().setTable(table).build();
    }

    public Write withFilter(Map filter) {
      return toBuilder().setFilter(filter).build();
    }

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
      checkNotNull(getMetastoreUri(), "metastoreUri");
      checkNotNull(getTable(), "database");
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
      public void populateDisplayData(
          org.apache.beam.sdk.transforms.display.DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.add(DisplayData.item("database", spec.getDatabase()));
        builder.add(DisplayData.item("table", spec.getTable()));
        builder.add(DisplayData.item("filter", (spec.getFilter() != null
            ? spec.getFilter().toString() : "NULL")));
        builder.add(DisplayData.item("metastoreUri", spec.getMetastoreUri()));
        builder.add(DisplayData.item("batchSize", spec.getBatchSize()));
      }

      @Setup
      public void initiateWrite() throws HCatException {
        try {
          //initiate writerContext, masterWriter & slaveWriter
          //to be used for writing into Hive
          writerContext = getWriterContext();
          slaveWriter = DataTransferFactory.getHCatWriter(writerContext);
        } catch (HCatException e) {
          LOG.error("Exception in initiateWriterContext", e);
          throw e;
        }
      }

      @StartBundle
      public void startBundle() {
        hCatRecordsBatch = new ArrayList<HCatRecord>();
      }

      @ProcessElement
      public void processElement(ProcessContext ctx) throws HCatException {
        hCatRecordsBatch.add(ctx.element());
        //apply batch
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
          //write data using slaveWriter
          slaveWriter.write(hCatRecordsBatch.iterator());
          //commit using masterWriter
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
        Map<String, String> configMap = new HashMap<String, String>();
        //this is the thrift metastore URI
        configMap.put(HCatConstants.HCAT_METASTORE_URI, spec.getMetastoreUri());
        WriteEntity.Builder builder = new WriteEntity.Builder();
        WriteEntity entity = builder.withDatabase(spec.getDatabase()).withTable(spec.getTable())
            .withPartition(spec.getFilter()).build();
        masterWriter = DataTransferFactory.getHCatWriter(entity, configMap);
        WriterContext context = masterWriter.prepareWrite();
        return context;
      }
    }
  }
}
