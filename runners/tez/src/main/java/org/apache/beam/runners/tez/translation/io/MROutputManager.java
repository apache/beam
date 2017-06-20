package org.apache.beam.runners.tez.translation.io;

import java.io.IOException;
import org.apache.beam.runners.tez.translation.TranslatorUtil;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;

/**
 * {@link TezOutputManager} implementation that properly writes output to {@link MROutput}
 */
public class MROutputManager extends TezOutputManager {

  private MROutput output;

  public MROutputManager(LogicalOutput output) {
    super(output);
    if (output.getClass().equals(MROutput.class)){
      this.output = (MROutput) output;
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
  public void after() {
    try {
      output.flush();
      output.commit();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
    try {
      getWriter().write(null, TranslatorUtil.convertToBytesWritable(output.getValue()));
    } catch (Exception e){
      throw new RuntimeException(e);
    }
  }
}
