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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import static com.google.common.base.Preconditions.checkArgument;
import static scala.collection.JavaConversions.asScalaBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.catalog.Database;
import org.apache.spark.sql.catalog.Function;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.collection.immutable.Map;

/**
 * This is a spark structured streaming {@link DataSourceV2} implementation. As Continuous streaming
 * is tagged experimental in spark, this class does no implement {@link ContinuousReadSupport}. This
 * class is just a mix-in.
 */
public class DatasetStreamingSource<T> implements DataSourceV2, MicroBatchReadSupport{

  private int numPartitions;
  private Long bundleSize;
  private TranslationContext context;
  private BoundedSource<T> source;


  @Override
  public MicroBatchReader createMicroBatchReader(
      Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
    this.numPartitions = context.getSparkSession().sparkContext().defaultParallelism();
    checkArgument(this.numPartitions > 0, "Number of partitions must be greater than zero.");
    this.bundleSize = context.getOptions().getBundleSize();
    return new DatasetMicroBatchReader(schema, checkpointLocation, options);
  }

  /** This class can be mapped to Beam {@link BoundedSource}. */
  private class DatasetMicroBatchReader implements MicroBatchReader {

    private Optional<StructType> schema;
    private String checkpointLocation;
    private DataSourceOptions options;

    private DatasetMicroBatchReader(
        Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
      //TODO deal with schema and options
    }

    @Override
    public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
      //TODO extension point for SDF
    }

    @Override
    public Offset getStartOffset() {
      //TODO extension point for SDF
      return null;
    }

    @Override
    public Offset getEndOffset() {
      //TODO extension point for SDF
      return null;
    }

    @Override
    public Offset deserializeOffset(String json) {
      //TODO extension point for SDF
      return null;
    }

    @Override
    public void commit(Offset end) {
      //TODO no more to read after end Offset
    }

    @Override
    public void stop() {}

    @Override
    public StructType readSchema() {
      return null;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
      List<InputPartition<InternalRow>> result = new ArrayList<>();
      long desiredSizeBytes;
      SparkPipelineOptions options = context.getOptions();
      try {
        desiredSizeBytes =
            (bundleSize == null)
                ? source.getEstimatedSizeBytes(options) / numPartitions
                : bundleSize;
        List<? extends BoundedSource<T>> sources = source.split(desiredSizeBytes, options);
        for (BoundedSource<T> source : sources) {
          result.add(
              new InputPartition<InternalRow>() {

                @Override
                public InputPartitionReader<InternalRow> createPartitionReader() {
                  BoundedReader<T> reader = null;
                  try {
                    reader = source.createReader(options);
                  } catch (IOException e) {
                    throw new RuntimeException(
                        "Error creating BoundedReader " + reader.getClass().getCanonicalName(), e);
                  }
                  return new DatasetMicroBatchPartitionReader(reader);
                }
              });
        }
        return result;

      } catch (Exception e) {
        throw new RuntimeException(
            "Error in splitting BoundedSource " + source.getClass().getCanonicalName(), e);
      }
    }
  }

  /** This class can be mapped to Beam {@link BoundedReader} */
  private class DatasetMicroBatchPartitionReader implements InputPartitionReader<InternalRow> {

    BoundedReader<T> reader;
    private boolean started;
    private boolean closed;

    DatasetMicroBatchPartitionReader(BoundedReader<T> reader) {
      this.reader = reader;
      this.started = false;
      this.closed = false;
    }

    @Override
    public boolean next() throws IOException {
      if (!started) {
        started = true;
        return reader.start();
      } else {
        return !closed && reader.advance();
      }
    }

    @Override
    public InternalRow get() {
      List<Object> list = new ArrayList<>();
      list.add(
          WindowedValue.timestampedValueInGlobalWindow(
              reader.getCurrent(), reader.getCurrentTimestamp()));
      return InternalRow.apply(asScalaBuffer(list).toList());
    }

    @Override
    public void close() throws IOException {
      closed = true;
      reader.close();
    }
  }

  private static class DatasetCatalog<T> extends Catalog {

    TranslationContext context;
    Source<T> source;

    private DatasetCatalog(TranslationContext context, Source<T> source) {
      this.context = context;
      this.source = source;
    }

    @Override public String currentDatabase() {
      return null;
    }

    @Override public void setCurrentDatabase(String dbName) {

    }

    @Override public Dataset<Database> listDatabases() {
      return null;
    }

    @Override public Dataset<Table> listTables() {
      return null;
    }

    @Override public Dataset<Table> listTables(String dbName) throws AnalysisException {
      return null;
    }

    @Override public Dataset<Function> listFunctions() {
      return null;
    }

    @Override public Dataset<Function> listFunctions(String dbName) throws AnalysisException {
      return null;
    }

    @Override public Dataset<Column> listColumns(String tableName) throws AnalysisException {
      return null;
    }

    @Override public Dataset<Column> listColumns(String dbName, String tableName)
        throws AnalysisException {
      return null;
    }

    @Override public Database getDatabase(String dbName) throws AnalysisException {
      return null;
    }

    @Override public Table getTable(String tableName) throws AnalysisException {
      return new DatasetTable<>("beam", "beaam", "beam fake table to wire up with Beam sources",
          null, true, source, context);
    }

    @Override public Table getTable(String dbName, String tableName) throws AnalysisException {
      return null;
    }

    @Override public Function getFunction(String functionName) throws AnalysisException {
      return null;
    }

    @Override public Function getFunction(String dbName, String functionName)
        throws AnalysisException {
      return null;
    }

    @Override public boolean databaseExists(String dbName) {
      return false;
    }

    @Override public boolean tableExists(String tableName) {
      return false;
    }

    @Override public boolean tableExists(String dbName, String tableName) {
      return false;
    }

    @Override public boolean functionExists(String functionName) {
      return false;
    }

    @Override public boolean functionExists(String dbName, String functionName) {
      return false;
    }

    @Override public Dataset<Row> createTable(String tableName, String path) {
      return null;
    }

    @Override public Dataset<Row> createTable(String tableName, String path, String source) {
      return null;
    }

    @Override public Dataset<Row> createTable(String tableName, String source,
        Map<String, String> options) {
      return null;
    }

    @Override public Dataset<Row> createTable(String tableName, String source, StructType schema,
        Map<String, String> options) {
      return null;
    }

    @Override public boolean dropTempView(String viewName) {
      return false;
    }

    @Override public boolean dropGlobalTempView(String viewName) {
      return false;
    }

    @Override public void recoverPartitions(String tableName) {

    }

    @Override public boolean isCached(String tableName) {
      return false;
    }

    @Override public void cacheTable(String tableName) {

    }

    @Override public void cacheTable(String tableName, StorageLevel storageLevel) {

    }

    @Override public void uncacheTable(String tableName) {

    }

    @Override public void clearCache() {

    }

    @Override public void refreshTable(String tableName) {

    }

    @Override public void refreshByPath(String path) {

    }

    private static class DatasetTable<T> extends Table {

      private Source<T> source;
      private TranslationContext context;

      public DatasetTable(String name, String database, String description, String tableType,
          boolean isTemporary, Source<T> source, TranslationContext context) {
        super(name, database, description, tableType, isTemporary);
        this.source = source;
        this.context = context;
      }

      private Source<T> getSource() {
        return source;
      }

      private TranslationContext getContext() {
        return context;
      }
    }
  }
}
