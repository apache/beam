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

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.managed.ManagedTransformConstants;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * SchemaTransform implementation for {@link IcebergIO#writeRows}. Writes Beam Rows to Iceberg and
 * outputs a {@code PCollection<Row>} representing snapshots created in the process.
 */
@AutoService(SchemaTransformProvider.class)
public class IcebergWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<SchemaTransformConfiguration> {

  static final String INPUT_TAG = "input";
  static final String OUTPUT_TAG = "output";

  static final Schema OUTPUT_SCHEMA =
      Schema.builder().addStringField("table").addFields(SnapshotInfo.SCHEMA.getFields()).build();

  @Override
  public String description() {
    return "Writes Beam Rows to Iceberg.\n"
        + "Returns a PCollection representing the snapshots produced in the process, with the following schema:\n"
        + "{\"table\" (str), \"operation\" (str), \"summary\" (map[str, str]), \"manifestListLocation\" (str)}";
  }

  @Override
  protected SchemaTransform from(SchemaTransformConfiguration configuration) {
    return new IcebergWriteSchemaTransform(configuration);
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  @Override
  public String identifier() {
    return ManagedTransformConstants.ICEBERG_WRITE;
  }

  static class IcebergWriteSchemaTransform extends SchemaTransform {
    private final SchemaTransformConfiguration configuration;

    IcebergWriteSchemaTransform(SchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    Row getConfigurationRow() {
      try {
        // To stay consistent with our SchemaTransform configuration naming conventions,
        // we sort lexicographically and convert field names to snake_case
        return SchemaRegistry.createDefault()
            .getToRowFunction(SchemaTransformConfiguration.class)
            .apply(configuration)
            .sorted()
            .toSnakeCase();
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> rows = input.get(INPUT_TAG);

      // TODO: support dynamic destinations
      IcebergWriteResult result =
          rows.apply(
              IcebergIO.writeRows(configuration.getIcebergCatalog())
                  .to(TableIdentifier.parse(configuration.getTable())));

      PCollection<Row> snapshots =
          result
              .getSnapshots()
              .apply(MapElements.via(new SnapshotToRow()))
              .setRowSchema(OUTPUT_SCHEMA);

      return PCollectionRowTuple.of(OUTPUT_TAG, snapshots);
    }

    @VisibleForTesting
    static class SnapshotToRow extends SimpleFunction<KV<String, SnapshotInfo>, Row> {
      @Override
      public Row apply(KV<String, SnapshotInfo> input) {
        SnapshotInfo snapshot = input.getValue();

        return Row.withSchema(OUTPUT_SCHEMA)
            .addValue(input.getKey())
            .addValues(snapshot.toRow().getValues())
            .build();
      }
    }
  }
}
