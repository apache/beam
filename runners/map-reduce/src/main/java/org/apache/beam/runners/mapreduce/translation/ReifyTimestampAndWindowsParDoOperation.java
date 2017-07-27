package org.apache.beam.runners.mapreduce.translation;

import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Created by peihe on 27/07/2017.
 */
public class ReifyTimestampAndWindowsParDoOperation extends ParDoOperation {

  public ReifyTimestampAndWindowsParDoOperation(
      PipelineOptions options,
      TupleTag<Object> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      WindowingStrategy<?, ?> windowingStrategy) {
    super(options, mainOutputTag, sideOutputTags, windowingStrategy);
  }

  @Override
  DoFn<Object, Object> getDoFn() {
    return (DoFn) new ReifyTimestampAndWindowsDoFn<>();
  }

  public class ReifyTimestampAndWindowsDoFn<K, V>
      extends DoFn<KV<K, V>, KV<K, WindowedValue<V>>> {
    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      KV<K, V> kv = c.element();
      K key = kv.getKey();
      V value = kv.getValue();
      c.output(KV.of(
          key,
          WindowedValue.of(
              value,
              c.timestamp(),
              window,
              c.pane())));
    }
  }
}
