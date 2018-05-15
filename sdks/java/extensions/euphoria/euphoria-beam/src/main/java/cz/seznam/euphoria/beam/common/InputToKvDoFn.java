package cz.seznam.euphoria.beam.common;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class InputToKvDoFn<InputT, K> extends DoFn<InputT, KV<K, InputT>> {

  private final UnaryFunction<InputT, K> keyExtractor;

  public InputToKvDoFn(UnaryFunction<InputT, K> leftKeyExtractor) {
    this.keyExtractor = leftKeyExtractor;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    InputT element = c.element();
    K key = keyExtractor.apply(element);
    c.output(KV.of(key, element));
  }

}
