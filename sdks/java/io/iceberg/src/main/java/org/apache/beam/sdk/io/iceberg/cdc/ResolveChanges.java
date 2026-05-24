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
package org.apache.beam.sdk.io.iceberg.cdc;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * Receives a {@link CoGbkResult} containing inserts and deletes sharing the same snapshot ID and
 * Primary Key, and uses {@link CdcResolver} to identify logical updates.
 *
 * <p>Input elements are pre-prepared with reified timestamps. This is because CoGroupByKey assigns
 * all elements in a window with the same timestamp, erasing individual record timestamps. This DoFn
 * enriches the record with the reified value to preserve its timestamp. TODO(ahmedabu98): is this ^
 * necessary anymore, now that we window each group by its snapshot commit timestamp? All rows
 * coming from a group should all have the same timestamp at this point. And we only potentially
 * assign watermark column timestamp downstream from this DoFn.
 */
public class ResolveChanges extends DoFn<KV<KV<Long, Row>, CoGbkResult>, Row> {
  public static final TupleTag<TimestampedValue<Row>> DELETES = new TupleTag<>() {};
  public static final TupleTag<TimestampedValue<Row>> INSERTS = new TupleTag<>() {};
  private final RowFilter rowFilter;

  ResolveChanges(IcebergScanConfig scanConfig) {
    this.rowFilter =
        new RowFilter(scanConfig.getSchema())
            .keep(
                scanConfig.getProjectedSchema().columns().stream()
                    .map(Types.NestedField::name)
                    .collect(Collectors.toList()));
  }

  @ProcessElement
  public void processElement(
      @Element KV<KV<Long, Row>, CoGbkResult> element, OutputReceiver<Row> out) {
    Row primaryKey = element.getKey().getValue();
    Set<String> pkFields = new HashSet<>(primaryKey.getSchema().getFieldNames());
    CoGbkResult result = element.getValue();

    // should be okay to materialize these lists. a PK collision will likely be a handful of records
    // at most
    List<TimestampedValue<Row>> deletes = Lists.newArrayList(result.getAll(DELETES));
    List<TimestampedValue<Row>> inserts = Lists.newArrayList(result.getAll(INSERTS));
    // TODO(ahmedabu98): do we need to sort anymore? all records should have the same timestamp now.
    deletes.sort(Comparator.comparing(TimestampedValue::getTimestamp));
    inserts.sort(Comparator.comparing(TimestampedValue::getTimestamp));

    new RowResolver(pkFields)
        .resolve(
            deletes,
            inserts,
            (kind, tv) -> {
              Row projectedRow = rowFilter.filter(tv.getValue());
              out.builder(projectedRow).setValueKind(kind).setTimestamp(tv.getTimestamp()).output();
              logEmit(kind, tv);
            });
  }

  private static final class RowResolver extends CdcResolver<TimestampedValue<Row>> {
    private final Set<String> pkFields;

    RowResolver(Set<String> pkFields) {
      this.pkFields = pkFields;
    }

    @Override
    protected int nonPkHash(TimestampedValue<Row> element) {
      int hash = 1;
      for (String field : element.getValue().getSchema().getFieldNames()) {
        if (pkFields.contains(field)) {
          continue;
        }
        hash = 31 * hash + Objects.hashCode(element.getValue().getValue(field));
      }
      return hash;
    }

    @Override
    protected boolean nonPkEquals(TimestampedValue<Row> delete, TimestampedValue<Row> insert) {
      Schema schema = insert.getValue().getSchema();
      for (String field : schema.getFieldNames()) {
        // we already know PK values are equal
        if (pkFields.contains(field)) {
          continue;
        }
        // return early if two values are not equal
        if (!Row.Equals.deepEquals(
            insert.getValue().getValue(field),
            delete.getValue().getValue(field),
            schema.getField(field).getType())) {
          return false;
        }
      }
      return true;
    }
  }

  /** Debug-only logging hook so the existing CoW / update / extra prints survive the refactor. */
  private static void logEmit(ValueKind kind, TimestampedValue<Row> tv) {
    switch (kind) {
      case UPDATE_BEFORE:
        System.out.printf("[BIDIRECTIONAL] -- UpdateBefore:%n\t%s%n", tv);
        break;
      case UPDATE_AFTER:
        System.out.printf("[BIDIRECTIONAL] -- UpdateAfter%n\t%s%n", tv);
        break;
      case DELETE:
        System.out.printf("[BIDIRECTIONAL] -- Deleted%n%s%n", tv);
        break;
      case INSERT:
        System.out.printf("[BIDIRECTIONAL] -- Inserted%n%s%n", tv);
        break;
    }
  }
}
