package org.apache.beam.runners.tez.translation.io;

import org.apache.beam.runners.tez.translation.TranslatorUtil;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;

/**
 * {@link TezOutputManager} implementation that properly writes output to {@link UnorderedKVOutput}
 */
public class UnorderedKVEdgeOutputManager extends TezOutputManager {

  private UnorderedKVOutput output;

  public UnorderedKVEdgeOutputManager(LogicalOutput output) {
    super(output);
    if (output.getClass().equals(UnorderedKVOutput.class)){
      this.output = (UnorderedKVOutput) output;
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
      getWriter().write(TranslatorUtil.convertToBytesWritable(getCurrentElement().getValue()),
          TranslatorUtil.convertToBytesWritable(output.getValue()));
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }
}
