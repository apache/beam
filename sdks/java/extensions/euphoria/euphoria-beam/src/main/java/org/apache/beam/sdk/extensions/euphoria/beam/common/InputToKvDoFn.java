package org.apache.beam.sdk.extensions.euphoria.beam.common;

import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * {@link DoFn} which takes input elements and transforms them to {@link KV} using given key
 * extractor.
 */
public class InputToKvDoFn<InputT, K> extends DoFn<InputT, KV<K, InputT>> {

  private final UnaryFunction<InputT, K> keyExtractor;

  public InputToKvDoFn(UnaryFunction<InputT, K> keyExtractor) {
    this.keyExtractor = keyExtractor;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    InputT element = c.element();
    K key = keyExtractor.apply(element);
    c.output(KV.of(key, element));
  }

}
