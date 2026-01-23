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
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

/**
 * Receives inserts and deletes, keyed by snapshot ID and Primary Key, and determines if any updates
 * have occurred.
 *
 * <p>If the element has a mix of inserts and deletes, it is considered an update. INSERT becomes
 * UPDATE_BEFORE and DELETE becomes UPDATE_AFTER.
 *
 * <p>Otherwise, records are output as-is: INSERT as INSERT, and DELETE as DELETE.
 *
 * <p>Note: snapshots written using the Copy-on-Write method will produce tasks where records appear
 * to be deleted then re-inserted. We perform an initial de-duplication and drop these records to
 * avoid mistaking them as no-op updates.
 *
 * <p>Input elements are prepared by reifying their timestamps. This is because CoGroupByKey assigns
 * all elements in a window with the same timestamp, erasing individual record timestamps. This DoFn
 * preserves it by outputting records with their reified timestamps.
 */
public class ResolveChanges extends DoFn<KV<Row, CoGbkResult>, Row> {
  public static final TupleTag<TimestampedValue<Row>> DELETES = new TupleTag<>() {};
  public static final TupleTag<TimestampedValue<Row>> INSERTS = new TupleTag<>() {};

  @DoFn.ProcessElement
  public void processElement(@Element KV<Row, CoGbkResult> element, OutputReceiver<Row> out) {
    Set<String> pkFields = new HashSet<>(element.getKey().getSchema().getFieldNames());
    CoGbkResult result = element.getValue();

    List<TimestampedValue<Row>> deletes = Lists.newArrayList(result.getAll(DELETES));
    List<TimestampedValue<Row>> inserts = Lists.newArrayList(result.getAll(INSERTS));
    deletes.sort(Comparator.comparing(TimestampedValue::getTimestamp));
    inserts.sort(Comparator.comparing(TimestampedValue::getTimestamp));

    boolean[] duplicateDeletes = new boolean[deletes.size()];
    boolean[] duplicateInserts = new boolean[inserts.size()];

    boolean hasDeletes = !deletes.isEmpty();
    boolean hasInserts = !inserts.isEmpty();

    if (hasInserts && hasDeletes) {
      // UPDATE: row ID exists in both streams
      // - emit all deletes as 'UPDATE_BEFORE', and all inserts as 'UPDATE_AFTER'
      // - emit extra deletes as 'DELETE'
      // - emit extra inserts as 'INSERT'

      // First, loop through both deletes and inserts and deduplicate records.
      // Duplicates can occur when an Iceberg writer uses CoW method:
      //  Deletes records by rewriting an entire DataFile except for the few records intended for
      // deletion.
      //  From our perspective, all the records were deleted, and some were inserted back in.
      //  We must ignore these records and not mistake them for "updates".
      for (int d = 0; d < deletes.size(); d++) {
        TimestampedValue<Row> delete = deletes.get(d);

        for (int i = 0; i < inserts.size(); i++) {
          if (duplicateInserts[i]) {
            continue;
          }

          TimestampedValue<Row> insert = inserts.get(i);
          if (isDuplicate(pkFields, delete.getValue(), insert.getValue())) {
            duplicateDeletes[d] = true;
            duplicateInserts[i] = true;
            System.out.printf("[DEDUPE] -- Ignored CoW record: %s%n", delete);
            break;
          }
        }
      }

      // Second, loop through and output UPDATE pairs
      int d = 0;
      int i = 0;
      while (d < deletes.size() && i < inserts.size()) {
        // find next unique delete
        while (d < deletes.size() && duplicateDeletes[d]) {
          d++;
        }
        // find next unique insert
        while (i < inserts.size() && duplicateInserts[i]) {
          i++;
        }

        if (d < deletes.size() && i < inserts.size()) {
          // UPDATE pair found. output as UpdateBefore/After
          TimestampedValue<Row> updateBefore = deletes.get(d);
          TimestampedValue<Row> updateAfter = inserts.get(i);

          // TODO: output as UPDATE_BEFORE and UPDATE_AFTER kind
          out.outputWithTimestamp(updateBefore.getValue(), updateBefore.getTimestamp());
          out.outputWithTimestamp(updateAfter.getValue(), updateAfter.getTimestamp());
          System.out.printf(
              "[BIDIRECTIONAL] -- UpdateBefore:%n\t%s%n\tUpdateAfter%n\t%s%n",
              updateBefore, updateAfter);

          d++;
          i++;
        }
      }

      // Finally, output extra deletes or inserts
      while (d < deletes.size()) {
        // TODO: output as DELETE kind
        if (!duplicateDeletes[d]) {
          TimestampedValue<Row> delete = deletes.get(d);
          out.outputWithTimestamp(delete.getValue(), delete.getTimestamp());
          System.out.printf("[BIDIRECTIONAL] -- Deleted(extra)%n%s%n", delete);
        }
        d++;
      }
      while (i < inserts.size()) {
        // TODO: output as INSERT kind
        if (!duplicateInserts[i]) {
          TimestampedValue<Row> insert = inserts.get(i);
          out.outputWithTimestamp(insert.getValue(), insert.getTimestamp());
          System.out.printf("[BIDIRECTIONAL] -- Inserted(extra)%n%s%n", insert);
        }
        i++;
      }
    } else if (hasInserts) {
      // INSERT only
      for (TimestampedValue<Row> rec : inserts) {
        System.out.printf("[BIDIRECTIONAL (only inserts)] -- Added%n%s%n", rec);
        out.outputWithTimestamp(rec.getValue(), rec.getTimestamp());
      }
    } else if (hasDeletes) {
      // DELETE only
      for (TimestampedValue<Row> rec : deletes) {
        // TODO: output as DELETE kind
        System.out.printf("[BIDIRECTIONAL (only deletes)] -- Deleted%n%s%n", rec);
        out.outputWithTimestamp(rec.getValue(), rec.getTimestamp());
      }
    }
  }

  /** Compares both records and checks whether they are duplicates or not. */
  private static boolean isDuplicate(Set<String> pkFields, Row delete, Row insert) {
    Schema schema = insert.getSchema();
    for (String field : insert.getSchema().getFieldNames()) {
      if (pkFields.contains(field)) {
        // these records are grouped by Primary Key, so we already know PK values are equal
        continue;
      }
      // return early if two values are not equal
      if (!Row.Equals.deepEquals(
          insert.getValue(field), delete.getValue(field), schema.getField(field).getType())) {
        return false;
      }
    }
    return true;
  }
}
