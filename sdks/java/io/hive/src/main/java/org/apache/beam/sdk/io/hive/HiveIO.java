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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatWriter;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.data.transfer.WriterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write data on Hive.
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
 *       .withPartition(partitionValues) //optional, 
 *       should be specified if the table is partitioned
 *       .withBatchSize(1024L)) //optional, 
 *       assumes a default batch size of 1024 if none specified
 * }</pre>
 */
@Experimental
public class HiveIO {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIO.class);

  /** Write data to Hive. */
  public static Write write() {
    return new AutoValue_HiveIO_Write.Builder().setBatchSize(1024L).build();
  }

  private HiveIO() {
  }

  /**
   * A {@link PTransform} to write to a Hive database.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<HCatRecord>, PDone> {
    @Nullable abstract String getMetastoreUri();
    @Nullable abstract String getDatabase();
    @Nullable abstract String getTable();
    @Nullable abstract Map getPartition();
    abstract long getBatchSize();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMetastoreUri(String metastoreUri);
      abstract Builder setDatabase(String database);
      abstract Builder setTable(String table);
      abstract Builder setPartition(Map partition);
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

    public Write withPartition(Map partition) {
      return toBuilder().setPartition(partition).build();
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
        builder.add(DisplayData.item("partition", (spec.getPartition() != null
            ? spec.getPartition().toString() : "NULL")));
        builder.add(DisplayData.item("metastoreUri", spec.getMetastoreUri()));
        builder.add(DisplayData.item("batchSize", spec.getBatchSize()));
      }

      @Setup
      public void initiateWrite() throws HCatException {
        try {
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
          masterWriter.abort(writerContext);
          throw e;
        }
        hCatRecordsBatch.clear();
      }

      private WriterContext getWriterContext() throws HCatException {
        Map<String, String> configMap = new HashMap<String, String>();
        configMap.put("hive.metastore.uris", spec.getMetastoreUri());
        WriteEntity.Builder builder = new WriteEntity.Builder();
        WriteEntity entity = builder.withDatabase(spec.getDatabase()).withTable(spec.getTable())
            .withPartition(spec.getPartition()).build();
        masterWriter = DataTransferFactory.getHCatWriter(entity, configMap);
        WriterContext context = masterWriter.prepareWrite();
        return context;
      }
    }
  }
}
