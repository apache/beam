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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.iceberg.DynamicDestinations;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.values.ValueKind;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Assigns a sort key to input {@link Row}s and groups by destination and shard keys, outputting
 * {@code KV<KV<destination, shard>, KV<sortKey, CdcRecord>>}.
 *
 * <p>For each element this:
 *
 * <ol>
 *   <li>resolves the destination string from the raw element;
 *   <li>resolves the element's {@link ValueKind};
 *   <li>in upsert mode, drops {@code UPDATE_BEFORE} records;
 *   <li>reads the sequence number from {@link CdcWriteConfig#getSequenceNumberColumn()};
 *   <li>takes the row to write from {@link DynamicDestinations#getData}, which excludes the control
 *       columns read above;
 *   <li>resolves and validates the destination table through {@link TableSetup};
 *   <li>encodes the primary key to bytes, which feed both the shard hash and the sort key;
 *   <li>computes the deterministic shard, according to {@code numShards} and {@code
 *       shardsPerPartition}
 * </ol>
 *
 * <p>When {@link CdcWriteConfig#getErrorHandling()} is enabled, a record-level failure (unknown
 * change type, missing/null sequence number, null equality value, an unresolvable destination) is
 * diverted to the {@link #FAILED} output as an {@link ErrorHandling#errorSchema} row ({@code
 * failed_row}, {@code error_message}). When error handling is disabled, the transform fails
 * instead.
 */
final class AssignCdcKeys extends PTransform<PCollection<Row>, PCollectionTuple> {

  static final TupleTag<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> KEYED = new TupleTag<>() {};
  static final TupleTag<Row> FAILED = new TupleTag<Row>() {};

  private final IcebergCatalogConfig catalogConfig;
  private final CdcWriteConfig config;
  private final DynamicDestinations destinations;
  private final String runId;

  AssignCdcKeys(
      IcebergCatalogConfig catalogConfig,
      CdcWriteConfig config,
      DynamicDestinations destinations,
      String runId) {
    this.catalogConfig = catalogConfig;
    this.config = config;
    this.destinations = destinations;
    this.runId = runId;
  }

  @Override
  public PCollectionTuple expand(PCollection<Row> input) {
    Schema inputSchema = input.getSchema();
    Schema errorSchema = ErrorHandling.errorSchema(inputSchema);
    Schema cdcDataSchema = destinations.getDataSchema();
    PCollectionTuple outputs =
        input.apply(
            "AssignKeys",
            ParDo.of(
                    new AssignFn(
                        new TableSetup(catalogConfig, config, destinations, runId),
                        config,
                        destinations,
                        errorSchema))
                .withOutputTags(KEYED, TupleTagList.of(FAILED)));
    outputs
        .get(KEYED)
        .setCoder(
            KvCoder.of(
                KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
                KvCoder.of(ByteArrayCoder.of(), CdcRecordCoder.of(cdcDataSchema))));
    outputs.get(FAILED).setCoder(RowCoder.of(errorSchema));
    return outputs;
  }

  /** Per-record entry point, running the eight steps listed in the main javadoc above. */
  private static final class AssignFn
      extends DoFn<Row, KV<KV<String, Integer>, KV<byte[], CdcRecord>>> {

    private final TableSetup tableSetup;
    private final CdcWriteConfig config;
    private final DynamicDestinations destinations;
    private final Schema errorSchema;
    private final int numShards;
    private final int shardsPerPartition;
    private final Counter failedRecords = Metrics.counter(AssignCdcKeys.class, "failedRecords");
    private final Counter upsertUpdateBeforeDropped =
        Metrics.counter(AssignCdcKeys.class, "upsertUpdateBeforeDropped");

    /** The control columns' positions in the current source schema. */
    private @Nullable ControlColumns controls;

    AssignFn(
        TableSetup tableSetup,
        CdcWriteConfig config,
        DynamicDestinations destinations,
        Schema errorSchema) {
      this.tableSetup = tableSetup;
      this.config = config;
      this.destinations = destinations;
      this.errorSchema = errorSchema;
      this.numShards = config.getNumShards();
      this.shardsPerPartition = config.getShardsPerPartition();
    }

    @ProcessElement
    public void processElement(
        @Element Row element,
        ValueKind elementKind,
        @Timestamp Instant timestamp,
        BoundedWindow window,
        PaneInfo pane,
        MultiOutputReceiver out) {
      try {
        Schema schema = element.getSchema();
        String destString =
            destinations.getTableStringIdentifier(
                ValueInSingleWindow.of(element, timestamp, window, pane));

        // Resolve the control columns' positions once per source schema. (The local lets the
        // nullness checker prove non-nullness, which it cannot for the field.)
        ControlColumns cols = controls;
        if (cols == null || !cols.matches(schema)) {
          cols = ControlColumns.of(schema, config);
          controls = cols;
        }

        ValueKind kind = resolveKind(element, cols, elementKind);
        if (config.getUpsert() && kind == ValueKind.UPDATE_BEFORE) {
          upsertUpdateBeforeDropped.inc();
          return;
        }
        long seq = readSeq(element, cols, kind);

        Row data = destinations.getData(element);
        TableSetup.Dest dest = tableSetup.get(destString, data.getSchema());
        requireNonNullEqualityValues(dest, data);
        byte[] pkBytes = encodePk(dest, data);

        out.get(KEYED)
            .output(
                KV.of(
                    KV.of(destString, shardFor(dest, data, pkBytes)),
                    KV.of(CdcSortKey.encode(pkBytes, seq, kind), CdcRecord.of(data, kind, seq))));
      } catch (TableSetup.TableConfigException e) {
        throw e;
      } catch (RuntimeException e) {
        if (!config.getErrorHandling()) {
          throw e;
        }
        failedRecords.inc();
        out.get(FAILED).output(ErrorHandling.errorRecord(errorSchema, element, e));
      }
    }

    /**
     * Resolves this element's {@link ValueKind}. When configured, uses the {@code
     * change_type_column} value (mapped via {@code change_type_map} when configured). Otherwise,
     * uses the element's native kind.
     */
    private ValueKind resolveKind(Row element, ControlColumns cols, ValueKind elementKind) {
      @Nullable String changeTypeColumn = config.getChangeTypeColumn();
      if (changeTypeColumn == null) {
        return elementKind;
      }
      if (cols.changeTypeIndex < 0) {
        throw new IllegalArgumentException(
            "change_type_column '"
                + changeTypeColumn
                + "' not found in element schema "
                + element.getSchema());
      }
      @Nullable String raw = element.getString(cols.changeTypeIndex);
      if (raw == null) {
        throw new IllegalArgumentException(
            "change_type_column '" + changeTypeColumn + "' is null for element " + element);
      }
      @Nullable Map<String, String> changeTypeMap = config.getChangeTypeMap();
      String name = changeTypeMap != null ? changeTypeMap.getOrDefault(raw, raw) : raw;
      try {
        return ValueKind.valueOf(name);
      } catch (IllegalArgumentException e) {
        String mappedClause = name.equals(raw) ? "" : " (mapped to '" + name + "')";
        throw new IllegalArgumentException(
            "change_type '"
                + raw
                + "'"
                + mappedClause
                + " is not a valid ValueKind name; must be one of "
                + Arrays.toString(ValueKind.values())
                + ", or add a change_type_map entry for it.",
            e);
      }
    }

    /** Reads the required non-null sequence number ({@code INT64}) from the full input row. */
    private long readSeq(Row element, ControlColumns cols, ValueKind kind) {
      String seqColumn = config.getSequenceNumberColumn();
      Schema schema = element.getSchema();
      @Nullable Long value;
      try {
        value = cols.seqIndex < 0 ? null : element.getInt64(cols.seqIndex);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            "sequence_number_column '"
                + seqColumn
                + "' must be INT64 (was: "
                + schema.getField(seqColumn).getType()
                + ")",
            e);
      }
      if (value == null) {
        throw new IllegalArgumentException(
            "sequence_number_column '"
                + seqColumn
                + "' is missing or null for a "
                + kind
                + " record; every CDC record requires a non-null sequence number.");
      }
      return value;
    }

    /**
     * Computes the record's write shard.
     *
     * <p>If the table is unpartitioned or if {@code shards_per_partition == num_shards}, the plain
     * primary-key shard is returned.
     *
     * <p>Otherwise computes the shard using {@link PartitionShardPlan}: each partition owns a block
     * of {@code shards_per_partition} consecutive shards. A record's primary key maps to an offset
     * within that block.
     *
     * <p>Must remain a pure function of the primary key: a key whose same-window records split
     * across shards breaks same-commit dedup.
     */
    private int shardFor(TableSetup.Dest dest, Row data, byte[] pkBytes) {
      @Nullable PartitionShardPlan partitionShardPlan = dest.partitionShardPlan();
      if (partitionShardPlan == null) {
        return TableSetup.shardFor(pkBytes, numShards);
      }
      int offset = Math.floorMod(TableSetup.pkHash(pkBytes), shardsPerPartition);
      return partitionShardPlan.shardFor(data, offset, numShards);
    }

    /**
     * Rejects a projected row with a null equality value: it cannot define row identity, so it
     * fails with a clear per-column error rather than an opaque coder failure (or, under
     * partition-block sharding, a silently null partition value).
     */
    private void requireNonNullEqualityValues(TableSetup.Dest dest, Row data) {
      int[] positions = dest.pkFieldPositions();
      for (int i = 0; i < positions.length; i++) {
        if (data.getValue(positions[i]) == null) {
          throw new IllegalArgumentException(
              "null value in equality column '"
                  + dest.pkSchema().getField(i).getName()
                  + "'; equality columns must be non-null to define row identity. Row: "
                  + data);
        }
      }
    }

    /** Extracts the primary key from the projected data row and encodes it to bytes. */
    private byte[] encodePk(TableSetup.Dest dest, Row data) {
      int[] pkPositions = dest.pkFieldPositions();
      List<@Nullable Object> pkValues = new ArrayList<>(pkPositions.length);
      for (int position : pkPositions) {
        pkValues.add(data.getValue(position));
      }
      Row pk = Row.withSchema(dest.pkSchema()).attachValues(pkValues);
      try {
        return CoderUtils.encodeToByteArray(dest.pkCoder(), pk);
      } catch (CoderException e) {
        throw new RuntimeException("Failed to encode primary key " + pk, e);
      }
    }
  }

  /** The control columns' positions in a source row schema. */
  private static final class ControlColumns {
    /** The source schema these positions were resolved against. */
    private final Schema schema;

    /** Position of the sequence-number column, or {@code -1} if the schema has none. */
    private final int seqIndex;

    /** Position of the change-type column, or {@code -1} if unconfigured or absent. */
    private final int changeTypeIndex;

    private ControlColumns(Schema schema, int seqIndex, int changeTypeIndex) {
      this.schema = schema;
      this.seqIndex = seqIndex;
      this.changeTypeIndex = changeTypeIndex;
    }

    static ControlColumns of(Schema schema, CdcWriteConfig config) {
      @Nullable String changeTypeColumn = config.getChangeTypeColumn();
      return new ControlColumns(
          schema,
          indexOrAbsent(schema, config.getSequenceNumberColumn()),
          changeTypeColumn == null ? -1 : indexOrAbsent(schema, changeTypeColumn));
    }

    private static int indexOrAbsent(Schema schema, String name) {
      return schema.hasField(name) ? schema.indexOf(name) : -1;
    }

    /** Whether these positions were resolved for {@code other}. */
    @SuppressWarnings("ReferenceEquality")
    boolean matches(Schema other) {
      return schema == other || schema.equals(other);
    }
  }
}
