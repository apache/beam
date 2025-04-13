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
package org.apache.beam.examples;

// beam-playground:
//   name: CombineFns.ComposedCombineFn
//   description: Demonstration of Composed Combine transform usage.
//   multifile: false
//   default_example: false
//   context_line: 143
//   categories:
//     - Schemas
//     - Combiners
//   complexity: MEDIUM
//   tags:
//     - transforms
//     - numbers

// gradle clean execute -DmainClass=org.apache.beam.examples.CoCombineTransformExample
// --args="--runner=DirectRunner" -Pdirect-runner

import java.util.ArrayList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineFns;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example that uses Composed combiners to apply multiple combiners (Sum, Min, Max) on the input
 * PCollection.
 *
 * <p>For a detailed documentation of Composed Combines, see <a
 * href="https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/CombineFns.html">
 * https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/CombineFns.html
 * </a>
 *
 * <p>Remark, the combiners are wrapped in a DropNullFn, because when cobining the input usually has
 * many null values that need to be handled by the combiner.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CoCombineTransformExample {

  /**
   * A wrapper for combiners, that will drop the null elements before applying the combiner. Similar
   * to <a
   * href="https://github.com/apache/beam/blob/e75577c80795e716172602f352b81d6aa764bb83/sdks/java/extensions/sql/src/main/java/org/apache/beam/sdk/extensions/sql/impl/transform/BeamBuiltinAggregations.java#L366">org.apache.beam.sdk.extensions.sql.impl.transform.BeamBuiltinAggregations
   * private DropNullFn()</a>
   */
  public static class DropNullFn<InputT, AccumT, OutputT>
      extends Combine.CombineFn<InputT, AccumT, OutputT> {

    protected final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;

    public DropNullFn(Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public AccumT createAccumulator() {
      return null;
    }

    @Override
    public AccumT addInput(AccumT accumulator, InputT input) {
      if (input == null) {
        return accumulator;
      }

      if (accumulator == null) {
        accumulator = combineFn.createAccumulator();
      }
      return combineFn.addInput(accumulator, input);
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      // filter out nulls
      accumulators = Iterables.filter(accumulators, Predicates.notNull());

      // handle only nulls
      if (!accumulators.iterator().hasNext()) {
        return null;
      }

      return combineFn.mergeAccumulators(accumulators);
    }

    @Override
    public OutputT extractOutput(AccumT accumulator) {
      if (accumulator == null) {
        return null;
      }
      return combineFn.extractOutput(accumulator);
    }

    @Override
    public Coder<AccumT> getAccumulatorCoder(CoderRegistry registry, Coder<InputT> inputCoder)
        throws CannotProvideCoderException {
      Coder<AccumT> coder = combineFn.getAccumulatorCoder(registry, inputCoder);
      if (coder instanceof NullableCoder) {
        return coder;
      }
      return NullableCoder.of(coder);
    }
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);
    // [START main_section]
    // Create input
    PCollection<KV<Long, Long>> inputKV =
        pipeline.apply(
            Create.of(KV.of(1L, 1L), KV.of(1L, 5L), KV.of(2L, 10L), KV.of(2L, 20L), KV.of(3L, 1L)));
    /**
     * Define the function used to filter elements before sending them to the Combiner. With
     * identityFn all elements (here perKey) will be combined.
     */
    SimpleFunction<Long, Long> identityFn =
        new SimpleFunction<Long, Long>() {
          @Override
          public Long apply(Long input) {
            return input;
          }
        };

    // tuple tags to identify the outputs of the Composed Combine
    TupleTag<Long> sumTag = new TupleTag<Long>("sum_n");
    TupleTag<Long> minTag = new TupleTag<Long>("min_n");
    TupleTag<Long> maxTag = new TupleTag<Long>("max_n");

    CombineFns.ComposedCombineFn<Long> composedCombine =
        CombineFns.compose()
            .with(
                identityFn,
                new DropNullFn<Long, long[], Long>(Sum.ofLongs()),
                sumTag) // elements filtered by the identityFn, will be combined in a Sum and the
            // output will be tagged
            .with(identityFn, new DropNullFn<Long, long[], Long>(Min.ofLongs()), minTag)
            .with(identityFn, new DropNullFn<Long, long[], Long>(Max.ofLongs()), maxTag);

    PCollection<KV<Long, CombineFns.CoCombineResult>> combinedData =
        inputKV.apply("Combine all", Combine.perKey(composedCombine));

    // transform the CoCombineResult output into a KV format, simpler to use for printing
    PCollection<KV<Long, Iterable<KV<String, Long>>>> result =
        combinedData.apply(
            ParDo.of(
                new DoFn<
                    KV<Long, CombineFns.CoCombineResult>, KV<Long, Iterable<KV<String, Long>>>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    CombineFns.CoCombineResult e = c.element().getValue();
                    ArrayList<KV<String, Long>> o = new ArrayList<KV<String, Long>>();
                    o.add(KV.of(minTag.getId(), e.get(minTag)));
                    o.add(KV.of(maxTag.getId(), e.get(maxTag)));
                    o.add(KV.of(sumTag.getId(), e.get(sumTag)));
                    c.output(KV.of(c.element().getKey(), o));
                  }
                }));

    // [END main_section]
    // Log values
    result.apply(ParDo.of(new LogOutput<>("PCollection values after CoCombine transform: ")));
    pipeline.run();
  }

  static class LogOutput<T> extends DoFn<T, T> {
    private static final Logger LOG = LoggerFactory.getLogger(LogOutput.class);
    private final String prefix;

    public LogOutput(String prefix) {
      this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      LOG.info(prefix + c.element());
      c.output(c.element());
    }
  }
}
