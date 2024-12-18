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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A writer that manages multiple {@link RecordWriter}s to write to multiple tables and partitions.
 * Assigns one {@link DestinationState} per windowed destination. A {@link DestinationState} assigns
 * one writer per partition in table destination. If the Iceberg {@link Table} is un-partitioned,
 * the data is written normally using one {@link RecordWriter} (i.e. the {@link DestinationState}
 * has one writer). At any given moment, the number of open data writers should be less than or
 * equal to the number of total partitions (across all windowed destinations).
 *
 * <p>A {@link DestinationState} maintains its writers in a {@link Cache}. If a {@link RecordWriter}
 * is inactive for 1 minute, the {@link DestinationState} will automatically close it to free up
 * resources. When a data writer is closed, its resulting {@link DataFile} gets written. Calling
 * {@link #close()} on this {@link RecordWriterManager} will do the following for each {@link
 * DestinationState}:
 *
 * <ol>
 *   <li>Close all underlying {@link RecordWriter}s
 *   <li>Collect all {@link DataFile}s as {@link SerializableDataFile}s (a more Beam-friendly type)
 * </ol>
 *
 * <p>After closing, the resulting {@link SerializableDataFile}s can be retrieved using {@link
 * #getSerializableDataFiles()}.
 */
class RecordWriterManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RecordWriterManager.class);
  /**
   * Represents the state of one Iceberg table destination. Creates one {@link RecordWriter} per
   * partition and manages them in a {@link Cache}.
   *
   * <p>On closing, each writer's output {@link DataFile} is collected.
   */
  class DestinationState {
    private final IcebergDestination icebergDestination;
    private final PartitionSpec spec;
    private final org.apache.iceberg.Schema schema;
    private final PartitionKey partitionKey;
    private final Table table;
    private final String stateToken = UUID.randomUUID().toString();
    final Cache<PartitionKey, RecordWriter> writers;
    private final List<SerializableDataFile> dataFiles = Lists.newArrayList();
    @VisibleForTesting final Map<PartitionKey, Integer> writerCounts = Maps.newHashMap();
    private final Map<String, PartitionField> partitionFieldMap = Maps.newHashMap();
    private final List<Exception> exceptions = Lists.newArrayList();

    DestinationState(IcebergDestination icebergDestination, Table table) {
      this.icebergDestination = icebergDestination;
      this.schema = table.schema();
      this.spec = table.spec();
      this.partitionKey = new PartitionKey(spec, schema);
      this.table = table;
      for (PartitionField partitionField : spec.fields()) {
        partitionFieldMap.put(partitionField.name(), partitionField);
      }

      // build a cache of RecordWriters.
      // writers will expire after 1 min of idle time.
      // when a writer expires, its data file is collected.
      this.writers =
          CacheBuilder.newBuilder()
              .expireAfterAccess(1, TimeUnit.MINUTES)
              .removalListener(
                  (RemovalNotification<PartitionKey, RecordWriter> removal) -> {
                    final PartitionKey pk = Preconditions.checkStateNotNull(removal.getKey());
                    final RecordWriter recordWriter =
                        Preconditions.checkStateNotNull(removal.getValue());
                    try {
                      recordWriter.close();
                    } catch (IOException e) {
                      RuntimeException rethrow =
                          new RuntimeException(
                              String.format(
                                  "Encountered an error when closing data writer for table '%s', path: %s",
                                  icebergDestination.getTableIdentifier(), recordWriter.path()),
                              e);
                      exceptions.add(rethrow);
                      throw rethrow;
                    }
                    openWriters--;
                    String partitionPath = getPartitionDataPath(pk.toPath(), partitionFieldMap);
                    dataFiles.add(
                        SerializableDataFile.from(recordWriter.getDataFile(), partitionPath));
                  })
              .build();
    }

    /**
     * Computes the partition key for this Iceberg {@link Record} and writes it using the
     * appropriate {@link RecordWriter}, creating new writers as needed.
     *
     * <p>However, if this {@link RecordWriterManager} is already saturated with writers, and we
     * can't create a new writer, the {@link Record} is rejected and {@code false} is returned.
     */
    boolean write(Record record) {
      partitionKey.partition(getPartitionableRecord(record));

      if (!writers.asMap().containsKey(partitionKey) && openWriters >= maxNumWriters) {
        return false;
      }
      RecordWriter writer = fetchWriterForPartition(partitionKey);
      writer.write(record);
      return true;
    }

    /**
     * Checks if a viable {@link RecordWriter} already exists for this partition and returns it. If
     * no {@link RecordWriter} exists or if it has reached the maximum limit of bytes written, a new
     * one is created and returned.
     */
    private RecordWriter fetchWriterForPartition(PartitionKey partitionKey) {
      RecordWriter recordWriter = writers.getIfPresent(partitionKey);

      if (recordWriter == null || recordWriter.bytesWritten() > maxFileSize) {
        // calling invalidate for a non-existent key is a safe operation
        writers.invalidate(partitionKey);
        recordWriter = createWriter(partitionKey);
        writers.put(partitionKey, recordWriter);
      }
      return recordWriter;
    }

    private RecordWriter createWriter(PartitionKey partitionKey) {
      // keep track of how many writers we opened for each destination-partition path
      // use this as a prefix to differentiate the new path.
      // this avoids overwriting a data file written by a previous writer in this destination state.
      int recordIndex = writerCounts.merge(partitionKey, 1, Integer::sum);
      try {
        RecordWriter writer =
            new RecordWriter(
                table,
                icebergDestination.getFileFormat(),
                filePrefix + "_" + stateToken + "_" + recordIndex,
                partitionKey);
        openWriters++;
        return writer;
      } catch (IOException e) {
        throw new RuntimeException(
            String.format(
                "Encountered an error when creating a RecordWriter for table '%s', partition %s.",
                icebergDestination.getTableIdentifier(), partitionKey),
            e);
      }
    }

    /**
     * Resolves an input {@link Record}'s partition values and returns another {@link Record} that
     * can be applied to the destination's {@link PartitionSpec}.
     */
    private Record getPartitionableRecord(Record record) {
      if (spec.isUnpartitioned()) {
        return record;
      }
      Record output = GenericRecord.create(schema);
      for (PartitionField partitionField : spec.fields()) {
        Transform<?, ?> transform = partitionField.transform();
        Types.NestedField field = schema.findField(partitionField.sourceId());
        String name = field.name();
        Object value = record.getField(name);
        @Nullable Literal<Object> literal = Literal.of(value.toString()).to(field.type());
        if (literal == null || transform.isVoid() || transform.isIdentity()) {
          output.setField(name, value);
        } else {
          output.setField(name, literal.value());
        }
      }
      return output;
    }
  }

  /**
   * Returns an equivalent partition path that is made up of partition data. Needed to reconstruct a
   * {@link DataFile}.
   */
  @VisibleForTesting
  static String getPartitionDataPath(
      String partitionPath, Map<String, PartitionField> partitionFieldMap) {
    if (partitionPath.isEmpty() || partitionFieldMap.isEmpty()) {
      return partitionPath;
    }
    List<String> resolved = new ArrayList<>();
    for (String partition : Splitter.on('/').splitToList(partitionPath)) {
      List<String> nameAndValue = Splitter.on('=').splitToList(partition);
      String name = nameAndValue.get(0);
      String value = nameAndValue.get(1);
      String transformName =
          Preconditions.checkArgumentNotNull(partitionFieldMap.get(name)).transform().toString();
      if (Transforms.month().toString().equals(transformName)) {
        int month = YearMonth.parse(value).getMonthValue();
        value = String.valueOf(month);
      } else if (Transforms.hour().toString().equals(transformName)) {
        long hour = ChronoUnit.HOURS.between(EPOCH, LocalDateTime.parse(value, HOUR_FORMATTER));
        value = String.valueOf(hour);
      }
      resolved.add(name + "=" + value);
    }
    return String.join("/", resolved);
  }

  private static final DateTimeFormatter HOUR_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
  private static final LocalDateTime EPOCH = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);

  private final Catalog catalog;
  private final String filePrefix;
  private final long maxFileSize;
  private final int maxNumWriters;
  @VisibleForTesting int openWriters = 0;

  @VisibleForTesting
  final Map<WindowedValue<IcebergDestination>, DestinationState> destinations = Maps.newHashMap();

  private final Map<WindowedValue<IcebergDestination>, List<SerializableDataFile>>
      totalSerializableDataFiles = Maps.newHashMap();

  @VisibleForTesting
  static final Cache<TableIdentifier, Table> TABLE_CACHE =
      CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();

  private boolean isClosed = false;

  RecordWriterManager(Catalog catalog, String filePrefix, long maxFileSize, int maxNumWriters) {
    this.catalog = catalog;
    this.filePrefix = filePrefix;
    this.maxFileSize = maxFileSize;
    this.maxNumWriters = maxNumWriters;
  }

  /**
   * Returns an Iceberg {@link Table}.
   *
   * <p>First attempts to fetch the table from the {@link #TABLE_CACHE}. If it's not there, we
   * attempt to load it using the Iceberg API. If the table doesn't exist at all, we attempt to
   * create it, inferring the table schema from the record schema.
   *
   * <p>Note that this is a best-effort operation that depends on the {@link Catalog}
   * implementation. Although it is expected, some implementations may not support creating a table
   * using the Iceberg API.
   */
  private Table getOrCreateTable(TableIdentifier identifier, Schema dataSchema) {
    @Nullable Table table = TABLE_CACHE.getIfPresent(identifier);
    if (table == null) {
      synchronized (TABLE_CACHE) {
        try {
          table = catalog.loadTable(identifier);
        } catch (NoSuchTableException e) {
          try {
            org.apache.iceberg.Schema tableSchema =
                IcebergUtils.beamSchemaToIcebergSchema(dataSchema);
            // TODO(ahmedabu98): support creating a table with a specified partition spec
            table = catalog.createTable(identifier, tableSchema);
            LOG.info("Created Iceberg table '{}' with schema: {}", identifier, tableSchema);
          } catch (AlreadyExistsException alreadyExistsException) {
            // handle race condition where workers are concurrently creating the same table.
            // if running into already exists exception, we perform one last load
            table = catalog.loadTable(identifier);
          }
        }
        TABLE_CACHE.put(identifier, table);
      }
    } else {
      // If fetching from cache, refresh the table to avoid working with stale metadata
      // (e.g. partition spec)
      table.refresh();
    }
    return table;
  }

  /**
   * Fetches the appropriate {@link RecordWriter} for this destination and partition and writes the
   * record.
   *
   * <p>If the {@link RecordWriterManager} is saturated (i.e. has hit the maximum limit of open
   * writers), the record is rejected and {@code false} is returned.
   */
  public boolean write(WindowedValue<IcebergDestination> icebergDestination, Row row) {
    DestinationState destinationState =
        destinations.computeIfAbsent(
            icebergDestination,
            destination -> {
              TableIdentifier identifier = destination.getValue().getTableIdentifier();
              Table table = getOrCreateTable(identifier, row.getSchema());
              return new DestinationState(destination.getValue(), table);
            });

    Record icebergRecord = IcebergUtils.beamRowToIcebergRecord(destinationState.schema, row);
    return destinationState.write(icebergRecord);
  }

  /**
   * Closes all remaining writers and collects all their {@link DataFile}s. Writes one {@link
   * ManifestFile} per windowed table destination.
   */
  @Override
  public void close() throws IOException {
    for (Map.Entry<WindowedValue<IcebergDestination>, DestinationState>
        windowedDestinationAndState : destinations.entrySet()) {
      DestinationState state = windowedDestinationAndState.getValue();

      // removing writers from the state's cache will trigger the logic to collect each writer's
      // data file.
      state.writers.invalidateAll();
      // first check for any exceptions swallowed by the cache
      if (!state.exceptions.isEmpty()) {
        IllegalStateException exception =
            new IllegalStateException(
                String.format("Encountered %s failed writer(s).", state.exceptions.size()));
        for (Exception e : state.exceptions) {
          exception.addSuppressed(e);
        }
        throw exception;
      }

      if (state.dataFiles.isEmpty()) {
        continue;
      }

      totalSerializableDataFiles.put(
          windowedDestinationAndState.getKey(), new ArrayList<>(state.dataFiles));
      state.dataFiles.clear();
    }
    destinations.clear();
    checkArgument(
        openWriters == 0,
        "Expected all data writers to be closed, but found %s data writer(s) still open",
        openWriters);
    isClosed = true;
  }

  /**
   * Returns a list of accumulated serializable {@link DataFile}s for each windowed {@link
   * IcebergDestination}. The {@link RecordWriterManager} must first be closed before this is
   * called.
   */
  public Map<WindowedValue<IcebergDestination>, List<SerializableDataFile>>
      getSerializableDataFiles() {
    checkState(
        isClosed,
        "Please close this %s before retrieving its data files.",
        getClass().getSimpleName());
    return totalSerializableDataFiles;
  }
}
