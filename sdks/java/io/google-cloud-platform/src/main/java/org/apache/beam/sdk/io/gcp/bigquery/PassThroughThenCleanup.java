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
