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

import static org.apache.beam.sdk.metrics.Metrics.counter;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Holds every file back until the pre-pass has a verdict for every table, then lets through only
 * the files of tables it did not refuse.
 *
 * <p>Two jobs in one step. The barrier is the side input: reading it cannot complete until the
 * whole verdict collection exists, which is the ordering {@code Wait.on} manufactures for a stage
 * that reads nothing. The filter is the per-key lookup, which {@code Wait.on} cannot express at
 * all, having no key dimension and discarding element values.
 */
final class TableGate extends DoFn<KV<String, String>, KV<String, String>> {

  static final TupleTag<KV<String, String>> RELEASED = new TupleTag<>();
  static final TupleTag<Row> ERRORS = new TupleTag<>();

  static final String HELD_COUNTER = "numFilesHeldBackByRefusal";
  private static final Counter numFilesHeldBack = counter(TableGate.class, HELD_COUNTER);

  private final PCollectionView<Map<String, SchemaCommitVerdict>> verdicts;

  TableGate(PCollectionView<Map<String, SchemaCommitVerdict>> verdicts) {
    this.verdicts = verdicts;
  }

  @ProcessElement
  public void process(ProcessContext context) {
    KV<String, String> element = context.element();
    Map<String, SchemaCommitVerdict> byTable = context.sideInput(verdicts);
    @Nullable SchemaCommitVerdict verdict = byTable.get(element.getKey());
    if (verdict == null) {
      // No verdict means no file of this table contributed a readable schema, so the pre-pass
      // never saw it. Released: registration reports each file with its own reason.
      context.output(element);
      return;
    }
    if (verdict.refused()) {
      numFilesHeldBack.inc();
      // the reason alone; the table it was headed for is the row's third field
      context.output(
          ERRORS,
          Row.withSchema(AddFiles.DYNAMIC_ERROR_SCHEMA)
              .addValues(
                  element.getValue(), checkStateNotNull(verdict.getRefusal()), element.getKey())
              .build());
      return;
    }
    context.output(element);
  }
}
