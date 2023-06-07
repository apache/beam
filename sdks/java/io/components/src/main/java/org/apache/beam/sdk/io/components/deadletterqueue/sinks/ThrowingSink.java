package org.apache.beam.sdk.io.components.deadletterqueue.sinks;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.ObjectUtils.Null;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ThrowingSink<T> extends PTransform<@NonNull PCollection<T>, @NonNull PDone> {

  @Override
  public PDone expand(@NonNull PCollection<T> input) {
    input.apply(ParDo.of(new ThrowingDoFn()));

    return PDone.in(input.getPipeline());
  }

  public class ThrowingDoFn extends DoFn<T, Null> {

    @ProcessElement
    public void processElement(@Element @NonNull T element){
      throw new RuntimeException(element.toString());
    }
  }
}
