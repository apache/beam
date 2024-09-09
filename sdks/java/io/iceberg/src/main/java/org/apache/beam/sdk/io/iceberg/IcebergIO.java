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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;

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

    abstract IcebergCatalogConfig getCatalogConfig();

    abstract @Nullable TableIdentifier getTableIdentifier();

    abstract @Nullable DynamicDestinations getDynamicDestinations();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCatalogConfig(IcebergCatalogConfig config);

      abstract Builder setTableIdentifier(TableIdentifier identifier);

      abstract Builder setDynamicDestinations(DynamicDestinations destinations);

      abstract WriteRows build();
    }

    public WriteRows to(TableIdentifier identifier) {
      return toBuilder().setTableIdentifier(identifier).build();
    }

    public WriteRows to(DynamicDestinations destinations) {
      return toBuilder().setDynamicDestinations(destinations).build();
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
            DynamicDestinations.singleTable(Preconditions.checkNotNull(getTableIdentifier()));
      }
      return input
          .apply("Set Destination Metadata", new AssignDestinations(destinations))
          .apply(
              "Write Rows to Destinations",
              new WriteToDestinations(getCatalogConfig(), destinations));
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
