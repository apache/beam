package org.apache.beam.runners.samza.translation;

import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Translates {@link org.apache.beam.sdk.io.Read} to Samza input
 * {@link org.apache.samza.operators.MessageStream}.
 */
public class ReadTranslator <T> implements TransformTranslator<PTransform<PBegin, PCollection<T>>> {
  @Override
  public void translate(PTransform<PBegin, PCollection<T>> transform,
                        TransformHierarchy.Node node,
                        TranslationContext ctx) {
    final PCollection<T> output = ctx.getOutput(transform);
    ctx.registerInputMessageStream(output);
  }
}
