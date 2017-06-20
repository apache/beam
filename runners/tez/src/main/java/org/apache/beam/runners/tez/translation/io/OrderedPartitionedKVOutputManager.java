package org.apache.beam.runners.tez.translation.io;

import org.apache.beam.runners.tez.translation.TranslatorUtil;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;

/**
 * {@link TezOutputManager} implementation that properly writes output to {@link OrderedPartitionedKVOutput}
 */
public class OrderedPartitionedKVOutputManager extends TezOutputManager {

  private OrderedPartitionedKVOutput output;

  public OrderedPartitionedKVOutputManager(LogicalOutput output) {
    super(output);
    if (output.getClass().equals(OrderedPartitionedKVOutput.class)){
      this.output = (OrderedPartitionedKVOutput) output;
      try {
        setWriter((KeyValueWriter) output.getWriter());
      } catch (Exception e) {
        throw new RuntimeException("Error when retrieving writer for output" + e.getMessage());
      }
    } else {
      throw new RuntimeException("Incorrect OutputManager for: " + output.getClass());
    }
  }

  @Override
  public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
    try {
      if (output.getValue() instanceof KV) {
        getWriter().write(TranslatorUtil.convertToBytesWritable(((KV) output.getValue()).getKey()),
            TranslatorUtil.convertToBytesWritable(((KV) output.getValue()).getValue()));
      } else {
        throw new IllegalArgumentException("GroupByKey can only group Key-Value outputs!");
      }
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }
}
