package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Created by peihe on 26/07/2017.
 */
public class GroupAlsoByWindowsParDoOperation extends ParDoOperation {

  private final Coder<?> inputCoder;

  public GroupAlsoByWindowsParDoOperation(
      PipelineOptions options,
      TupleTag<Object> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      WindowingStrategy<?, ?> windowingStrategy,
      Coder<?> inputCoder) {
    super(options, mainOutputTag, sideOutputTags, windowingStrategy);
    this.inputCoder = checkNotNull(inputCoder, "inputCoder");
  }

  @Override
  DoFn<Object, Object> getDoFn() {
    return new GroupAlsoByWindowsViaOutputBufferDoFn(
        windowingStrategy,
        SystemReduceFn.buffering(inputCoder),
        mainOutputTag,
        createOutputManager());
  }
}
