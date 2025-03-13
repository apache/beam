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
package org.apache.beam.runners.spark.translation;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class PassThrough {

  public static <InputT> SingleOutput<InputT> ofSingleOutput(Coder<InputT> inputCoder) {
    return new SingleOutput<>(inputCoder);
  }

  public static <InputT> MultipleOutput<InputT> ofMultipleOutput(
      TupleTag<InputT> tag1, TupleTag<InputT> tag2) {
    return new MultipleOutput<>(tag1, tag2);
  }

  public static class SingleOutput<InputT>
      extends PTransform<PCollection<InputT>, PCollection<InputT>> {
    private final Coder<InputT> inputCoder;

    public SingleOutput(Coder<InputT> inputCoder) {
      this.inputCoder = inputCoder;
    }

    @Override
    public PCollection<InputT> expand(PCollection<InputT> input) {
      return input
          .apply(
              ParDo.of(
                  new DoFn<InputT, InputT>() {
                    @ProcessElement
                    public void process(@Element InputT input, OutputReceiver<InputT> output) {
                      output.output(input);
                    }
                  }))
          .setCoder(inputCoder);
    }
  }

  public static class MultipleOutput<InputT>
      extends PTransform<PCollection<InputT>, PCollectionTuple> {

    private final TupleTag<InputT> tag1;
    private final TupleTag<InputT> tag2;

    public MultipleOutput(TupleTag<InputT> tag1, TupleTag<InputT> tag2) {
      this.tag1 = tag1;
      this.tag2 = tag2;
    }

    @Override
    public PCollectionTuple expand(PCollection<InputT> input) {
      return input.apply(
          ParDo.of(
                  new DoFn<InputT, InputT>() {
                    @ProcessElement
                    public void process(@Element InputT input, MultiOutputReceiver output) {
                      if (input.hashCode() % 2 == 0) {
                        output.get(tag1).output(input);
                      } else {
                        output.get(tag2).output(input);
                      }
                    }
                  })
              .withOutputTags(tag1, TupleTagList.of(tag2)));
    }
  }
}
