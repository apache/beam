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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PTransform} that invokes {@link CleanupOperation} after the input {@link PCollection}
 * has been processed.
 */
@VisibleForTesting
class PassThroughThenCleanup<T> extends PTransform<PCollection<T>, PCollection<T>> {

  private CleanupOperation cleanupOperation;
  private PCollectionView<String> jobIdSideInput;

  PassThroughThenCleanup(
      CleanupOperation cleanupOperation, PCollectionView<String> jobIdSideInput) {
    this.cleanupOperation = cleanupOperation;
    this.jobIdSideInput = jobIdSideInput;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    TupleTag<T> mainOutput = new TupleTag<>();
    TupleTag<Void> cleanupSignal = new TupleTag<>();
    PCollectionTuple outputs =
        input.apply(
            ParDo.of(new IdentityFn<T>())
                .withOutputTags(mainOutput, TupleTagList.of(cleanupSignal)));

    PCollectionView<Iterable<Void>> cleanupSignalView =
        outputs.get(cleanupSignal).setCoder(VoidCoder.of()).apply(View.asIterable());

    input
        .getPipeline()
        .apply("Create(CleanupOperation)", Create.of(cleanupOperation))
        .apply(
            "Cleanup",
            ParDo.of(
                    new DoFn<CleanupOperation, Void>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        c.element().cleanup(new ContextContainer(c, jobIdSideInput));
                      }
                    })
                .withSideInputs(jobIdSideInput, cleanupSignalView));

    return outputs.get(mainOutput).setCoder(input.getCoder());
  }

  private static class IdentityFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  abstract static class CleanupOperation implements Serializable {
    abstract void cleanup(ContextContainer container) throws Exception;

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      return obj != null && obj.getClass() == this.getClass();
    }
  }

  static class ContextContainer {
    private PCollectionView<String> view;
    private DoFn<?, ?>.ProcessContext context;

    public ContextContainer(DoFn<?, ?>.ProcessContext context, PCollectionView<String> view) {
      this.view = view;
      this.context = context;
    }

    public PipelineOptions getPipelineOptions() {
      return context.getPipelineOptions();
    }

    public String getJobId() {
      return context.sideInput(view);
    }
  }
}
