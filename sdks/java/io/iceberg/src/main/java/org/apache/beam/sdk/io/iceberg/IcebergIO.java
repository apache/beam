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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * The underlying Iceberg connector used by {@link org.apache.beam.sdk.managed.Managed#ICEBERG}. Not
 * intended to be used directly.
 *
 * <p>For internal use only; no backwards compatibility guarantees
 */
@Internal
public class IcebergIO {

  public static WriteRows writeRows(IcebergCatalogConfig catalog) {
    return new AutoValue_IcebergIO_WriteRows.Builder().setCatalogConfig(catalog).build();
  }

  @AutoValue
  public abstract static class WriteRows extends PTransform<PCollection<Row>, IcebergWriteResult> {
    private static final int TRIGGERING_RECORD_COUNT = 50_000;

    abstract IcebergCatalogConfig getCatalogConfig();

    abstract @Nullable TableIdentifier getTableIdentifier();

    abstract @Nullable DynamicDestinations getDynamicDestinations();

    abstract @Nullable Duration getTriggeringFrequency();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCatalogConfig(IcebergCatalogConfig config);

      abstract Builder setTableIdentifier(TableIdentifier identifier);

      abstract Builder setDynamicDestinations(DynamicDestinations destinations);

      abstract Builder setTriggeringFrequency(Duration triggeringFrequency);

      abstract WriteRows build();
    }

    public WriteRows to(TableIdentifier identifier) {
      return toBuilder().setTableIdentifier(identifier).build();
    }

    public WriteRows to(DynamicDestinations destinations) {
      return toBuilder().setDynamicDestinations(destinations).build();
    }

    /**
     * Sets the frequency at which data is committed and a new {@link org.apache.iceberg.Snapshot}
     * is produced.
     *
     * <p>Roughly every triggeringFrequency duration, this connector will try to accumulate all
     * {@link org.apache.iceberg.ManifestFile}s and commit them to the table as appended files. Each
     * commit results in a new table {@link org.apache.iceberg.Snapshot}.
     *
     * <p>This is only applicable when writing an unbounded {@link PCollection} (i.e. a streaming
     * pipeline).
     */
    public WriteRows withTriggeringFrequency(Duration triggeringFrequency) {
      return toBuilder().setTriggeringFrequency(triggeringFrequency).build();
    }

    @Override
    public IcebergWriteResult expand(PCollection<Row> input) {
      List<?> allToArgs = Arrays.asList(getTableIdentifier(), getDynamicDestinations());
      Preconditions.checkArgument(
          1 == allToArgs.stream().filter(Predicates.notNull()).count(),
          "Must set exactly one of table identifier or dynamic destinations object.");

      DynamicDestinations destinations = getDynamicDestinations();
      if (destinations == null) {
        destinations =
            DynamicDestinations.singleTable(
                Preconditions.checkNotNull(getTableIdentifier()), input.getSchema());
      }

      // Assign destinations before re-windowing to global because
      // user's dynamic destination may depend on windowing properties
      PCollection<Row> assignedRows =
          input.apply("Set Destination Metadata", new AssignDestinations(destinations));

      if (assignedRows.isBounded().equals(PCollection.IsBounded.UNBOUNDED)) {
        Duration triggeringFrequency = getTriggeringFrequency();
        checkArgumentNotNull(
            triggeringFrequency, "Streaming pipelines must set a triggering frequency.");
        assignedRows =
            assignedRows.apply(
                "WindowIntoGlobal",
                Window.<Row>into(new GlobalWindows())
                    .triggering(
                        Repeatedly.forever(
                            AfterFirst.of(
                                AfterProcessingTime.pastFirstElementInPane()
                                    .plusDelayOf(triggeringFrequency),
                                AfterPane.elementCountAtLeast(TRIGGERING_RECORD_COUNT))))
                    .discardingFiredPanes());
      } else {
        Preconditions.checkArgument(
            getTriggeringFrequency() == null,
            "Triggering frequency is only applicable for streaming pipelines.");
      }
      return assignedRows.apply(
          "Write Rows to Destinations",
          new WriteToDestinations(getCatalogConfig(), destinations, getTriggeringFrequency()));
    }
  }

  public static ReadRows readRows(IcebergCatalogConfig catalogConfig) {
    return new AutoValue_IcebergIO_ReadRows.Builder().setCatalogConfig(catalogConfig).build();
  }

  @AutoValue
  public abstract static class ReadRows extends PTransform<PBegin, PCollection<Row>> {

    abstract IcebergCatalogConfig getCatalogConfig();

    abstract @Nullable TableIdentifier getTableIdentifier();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCatalogConfig(IcebergCatalogConfig config);

      abstract Builder setTableIdentifier(TableIdentifier identifier);

      abstract ReadRows build();
    }

    public ReadRows from(TableIdentifier tableIdentifier) {
      return toBuilder().setTableIdentifier(tableIdentifier).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      TableIdentifier tableId =
          checkStateNotNull(getTableIdentifier(), "Must set a table to read from.");

      Table table = getCatalogConfig().catalog().loadTable(tableId);

      return input.apply(
          Read.from(
              new ScanSource(
                  IcebergScanConfig.builder()
                      .setCatalogConfig(getCatalogConfig())
                      .setScanType(IcebergScanConfig.ScanType.TABLE)
                      .setTableIdentifier(tableId)
                      .setSchema(IcebergUtils.icebergSchemaToBeamSchema(table.schema()))
                      .build())));
    }
  }
}
