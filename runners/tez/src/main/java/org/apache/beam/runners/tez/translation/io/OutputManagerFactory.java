package org.apache.beam.runners.tez.translation.io;

import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;

public class OutputManagerFactory {
  public static TezOutputManager createOutputManager(LogicalOutput output){
    TezOutputManager outputManager;
    if (output.getClass().equals(OrderedPartitionedKVOutput.class)){
      outputManager = new OrderedPartitionedKVOutputManager(output);
    } else if (output.getClass().equals(UnorderedKVOutput.class)){
      outputManager = new UnorderedKVEdgeOutputManager(output);
    } else if (output.getClass().equals(MROutput.class)){
      outputManager = new MROutputManager(output);
    } else {
      throw new RuntimeException("Output type: " + output.getClass() + " is unsupported");
    }
    return outputManager;
  }
}
