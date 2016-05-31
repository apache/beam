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

package org.apache.beam.runners.spark.translation.streaming;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.spark.translation.streaming.utils.PAssertStreaming;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.PAssert.IterableAssert;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A version of {@link PAssert}, more specifically
 * {@link IterableAssert#containsInAnyOrder(Iterable)} that does not use side inputs, suitable for
 * use when the input {@link PCollection} conforms to certain restrictions.
 *
 * <p>Specifically, if the input {@link PCollection} will only ever produce one pane per key, and
 * all windows contain the same elements, this will succeed.
 *
 * <p>Compatible with {@link PAssertStreaming}.
 */
public class AssertContains<InputT> extends PTransform<PCollection<InputT>, PDone> {
  private final Collection<InputT> expected;

  public AssertContains(Collection<InputT> expected) {
    this.expected = expected;
  }

  public PDone apply(PCollection<InputT> input) {
    input
        .apply(WithKeys.<Long, InputT>of(0L))
        .apply(GroupByKey.<Long, InputT>create())
        .apply(Values.<Iterable<InputT>>create())
        .apply(ParDo.of(new AssertContentsEqualFn<InputT>(input.getCoder(), expected)));

    return PDone.in(input.getPipeline());
  }

  private static class AssertContentsEqualFn<InputT> extends DoFn<Iterable<InputT>, Long> {
    /** For compatibility with {@link PAssert}. */
    private final Aggregator<Integer, Integer> successes =
        createAggregator(PAssertStreaming.SUCCESS_COUNTER, new Sum.SumIntegerFn());
    /** For compatibility with {@link PAssert}. */
    private final Aggregator<Integer, Integer> failures =
        createAggregator(PAssertStreaming.FAILURE_COUNTER, new Sum.SumIntegerFn());

    private final Coder<InputT> coder;
    private final Collection<byte[]> encodedExpected;

    private AssertContentsEqualFn(Coder<InputT> coder, Collection<InputT> expected) {
      this.coder = coder;
      ImmutableList.Builder<byte[]> encodedBuilder = ImmutableList.builder();
      for (InputT elem : expected) {
        try {
          encodedBuilder.add(CoderUtils.encodeToByteArray(coder, elem));
        } catch (CoderException e) {
          throw new IllegalArgumentException(
              "Could not encode expected elements with provided coder",
              e);
        }
      }
      encodedExpected = encodedBuilder.build();
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      List<InputT> expected = new ArrayList<>();
      for (byte[] encoded : encodedExpected) {
        expected.add(CoderUtils.decodeFromByteArray(coder, encoded));
      }
      Iterable<InputT> elems = c.element();
      try {
        assertThat(elems, containsInAnyOrder(expected.toArray()));
        successes.addValue(1);
      } catch (AssertionError e) {
        failures.addValue(1);
        throw e;
      }
    }
  }
}
