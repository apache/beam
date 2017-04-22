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

package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * A {@link PTransform} that invokes {@link CleanupOperation} after the input {@link PCollection}
 * has been processed.
 */
@VisibleForTesting
class PassThroughThenCleanup<T> extends PTransform<PCollection<T>, PCollection<T>> {

  private CleanupOperation cleanupOperation;

  PassThroughThenCleanup(CleanupOperation cleanupOperation) {
    this.cleanupOperation = cleanupOperation;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    TupleTag<T> mainOutput = new TupleTag<>();
    TupleTag<Void> cleanupSignal = new TupleTag<>();
    PCollectionTuple outputs = input.apply(ParDo.of(new IdentityFn<T>())
        .withOutputTags(mainOutput, TupleTagList.of(cleanupSignal)));

    PCollectionView<Void> cleanupSignalView = outputs.get(cleanupSignal)
        .setCoder(VoidCoder.of())
        .apply(View.<Void>asSingleton().withDefaultValue(null));

    input.getPipeline()
        .apply("Create(CleanupOperation)", Create.of(cleanupOperation))
        .apply("Cleanup", ParDo.of(
            new DoFn<CleanupOperation, Void>() {
              @ProcessElement
              public void processElement(ProcessContext c)
                  throws Exception {
                c.element().cleanup(c.getPipelineOptions());
              }
            }).withSideInputs(cleanupSignalView));

    return outputs.get(mainOutput);
  }

  private static class IdentityFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  abstract static class CleanupOperation implements Serializable {
    abstract void cleanup(PipelineOptions options) throws Exception;
  }
}
