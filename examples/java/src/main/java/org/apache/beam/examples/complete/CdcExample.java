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
package org.apache.beam.examples.complete;

// beam-playground:
//   name: TfIdf
//   description: An example that computes a basic TF-IDF search table for a directory or
//     GCS prefix.
//   multifile: true
//   pipeline_options: --output output.txt
//   context_line: 447
//   categories:
//     - Combiners
//     - Options
//     - Joins
//   complexity: ADVANCED
//   tags:
//     - tfidf
//     - count
//     - strings

import java.util.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcExample {

  private static final Logger LOG = LoggerFactory.getLogger(CdcExample.class);

  /**
   * Options supported by {@link CdcExample}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions {
    @Default.Integer(10)
    Integer getNumIds();

    void setNumIds(Integer numIds);
  }

  private static final Schema SCHEMA =
      Schema.builder().addStringField("id").addInt64Field("counter").build();

  @DoFn.UnboundedPerElement
  static class FakeCdcStream extends DoFn<String, Row> {

    /**
     * The void/null from an impulse starts this stream with a single-entry row with an integer at
     * 0, then a stream of CDC events that increment it. The restriction is a half-open interval [n,
     * inf) that indicates the starting point. It is unsplittable.
     */
    @GetInitialRestriction
    public OffsetRange getInitialRestriction() {
      return new OffsetRange(0, Long.MAX_VALUE);
    }

    @GetRestrictionCoder
    public Coder<OffsetRange> getRestrictionCoder() {
      return OffsetRange.Coder.of();
    }

    @NewTracker
    public RestrictionTracker<OffsetRange, Long> newTracker(@Restriction OffsetRange restriction) {
      return new OffsetRangeTracker(restriction);
    }

    @ProcessElement
    public ProcessContinuation process(
        @Element String id,
        RestrictionTracker<OffsetRange, Long> tracker,
        OutputReceiver<Row> output)
        throws InterruptedException {

      Long offset = tracker.currentRestriction().getFrom();

      Row current = Row.withSchema(SCHEMA).addValues(id, offset).build();
      Row previous;
      if (offset == 0L) {
        if (!tracker.tryClaim(offset)) {
          return ProcessContinuation.stop();
        } else {
          output.outputWithKind(current, ValueKind.INSERT);
        }
        previous = current;
        ++offset;
      } else {
        // Starting from nonzero offset, we need to output an UPDATE_BEFORE.
        // Noting the unfortunate clairvoyance required...
        previous = Row.withSchema(SCHEMA).addValues(id, offset - 1).build();
      }

      while (offset < tracker.currentRestriction().getTo() && tracker.tryClaim(offset)) {
        current = Row.withSchema(SCHEMA).addValues(id, offset).build();

        output.outputWithKind(previous, ValueKind.UPDATE_BEFORE);
        output.outputWithKind(current, ValueKind.UPDATE_AFTER);

        ++offset;
        Thread.sleep(200);
        previous = current;
      }
      LOG.info("Runner told me to stop at offset {}", offset);
      return ProcessContinuation.stop();
    }
  }

  static void runExample(Options options) throws Exception {
    Pipeline pipeline = Pipeline.create(options);

    List<String> ids = new ArrayList<>(options.getNumIds());
    for (int i = 0; i < options.getNumIds(); ++i) {
      ids.add(i, UUID.randomUUID().toString());
    }

    pipeline
        .apply(Create.of(ids))
        .apply(ParDo.of(new FakeCdcStream()))
        .setCoder(RowCoder.of(SCHEMA))
        .apply(
            ParDo.of(
                new DoFn<Row, Void>() {
                  @ProcessElement
                  public void process(@Element Row row, ValueKind kind) {
                    LOG.info("[{}] {}: {}", kind, row.getString("id"), row.getInt64("counter"));
                  }
                }));

    pipeline.run().waitUntilFinish();
  }

  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    runExample(options);
  }
}
