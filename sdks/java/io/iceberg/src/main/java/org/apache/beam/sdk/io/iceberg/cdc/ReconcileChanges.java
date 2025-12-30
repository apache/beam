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

import java.util.Iterator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Receives inserts and deletes, keyed by snapshot ID and Primary Key, and determines if any updates
 * have occurred.
 *
 * <p>If the element has a mix of inserts and deletes, it is considered an update. INSERT becomes
 * UPDATE_BEFORE and DELETE becomes UPDATE_AFTER.
 *
 * <p>Otherwise, records are output as-is: INSERT as INSERT, and DELETE as DELETE.
 *
 * <p>Input elements have their timestamp reified. This is because CoGroupByKey assigns all elements
 * in a window with the same timestamp, erasing individual record timestamps. This DoFn preserves it
 * by outputting records with their reified timestamps.
 */
public class ReconcileChanges extends DoFn<KV<Row, CoGbkResult>, Row> {
  public static final TupleTag<TimestampedValue<Row>> DELETES = new TupleTag<>() {};
  public static final TupleTag<TimestampedValue<Row>> INSERTS = new TupleTag<>() {};

  @DoFn.ProcessElement
  public void processElement(@Element KV<Row, CoGbkResult> element, OutputReceiver<Row> out) {
    CoGbkResult result = element.getValue();

    // iterables are lazy-loaded from the shuffle service
    Iterable<TimestampedValue<Row>> deletes = result.getAll(DELETES);
    Iterable<TimestampedValue<Row>> inserts = result.getAll(INSERTS);

    boolean hasDeletes = deletes.iterator().hasNext();
    boolean hasInserts = inserts.iterator().hasNext();

    if (hasInserts && hasDeletes) {
      // UPDATE: row ID exists in both streams
      // - emit all deletes as 'UPDATE_BEFORE', and all inserts as 'UPDATE_AFTER'
      // - emit extra inserts as 'UPDATE_AFTER'
      // - ignore extra deletes (TODO: double check if this is a good decision)
      Iterator<TimestampedValue<Row>> deletesIterator = deletes.iterator();
      Iterator<TimestampedValue<Row>> insertsIterator = inserts.iterator();
      while (deletesIterator.hasNext() && insertsIterator.hasNext()) {
        // TODO: output as UPDATE_BEFORE kind
        TimestampedValue<Row> updateBefore = deletesIterator.next();
        out.outputWithTimestamp(updateBefore.getValue(), updateBefore.getTimestamp());
        System.out.printf("[BIDIRECTIONAL] -- UpdateBefore\n%s\n", updateBefore);

        // TODO: output as UPDATE_AFTER kind
        TimestampedValue<Row> updateAfter = insertsIterator.next();
        out.outputWithTimestamp(updateAfter.getValue(), updateAfter.getTimestamp());
        System.out.printf("[BIDIRECTIONAL] -- UpdateAfter\n%s\n", updateAfter);
      }
      while (insertsIterator.hasNext()) {
        // TODO: output as UPDATE_AFTER kind
        TimestampedValue<Row> insert = insertsIterator.next();
        out.outputWithTimestamp(insert.getValue(), insert.getTimestamp());
        System.out.printf("[BIDIRECTIONAL] -- Added(extra)\n%s\n", insert);
      }
    } else if (hasInserts) {
      // INSERT only
      for (TimestampedValue<Row> rec : inserts) {
        System.out.printf("[BIDIRECTIONAL] -- Added\n%s\n", rec);
        out.outputWithTimestamp(rec.getValue(), rec.getTimestamp());
      }
    } else if (hasDeletes) {
      // DELETE only
      for (TimestampedValue<Row> rec : deletes) {
        // TODO: output as DELETE kind
        System.out.printf("[BIDIRECTIONAL] -- Deleted\n%s\n", rec);
        out.outputWithTimestamp(rec.getValue(), rec.getTimestamp());
      }
    }
  }
}
