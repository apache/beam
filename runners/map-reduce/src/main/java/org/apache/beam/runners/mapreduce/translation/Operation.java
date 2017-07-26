package org.apache.beam.runners.mapreduce.translation;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Created by peihe on 26/07/2017.
 */
public abstract class Operation implements Serializable {
  private final OutputReceiver[] receivers;

  public Operation(int numOutputs) {
    this.receivers = new OutputReceiver[numOutputs];
    for (int i = 0; i < numOutputs; ++i) {
      receivers[i] = new OutputReceiver();
    }
  }

  /**
   * Starts this Operation's execution.
   *
   * <p>Called after all successors consuming operations have been started.
   */
  public void start(TaskInputOutputContext<Object, Object, Object, Object> taskContext) {
    for (OutputReceiver receiver : receivers) {
      if (receiver == null) {
        continue;
      }
      for (Operation operation : receiver.getReceivingOperations()) {
        operation.start(taskContext);
      }
    }
  }

  /**
   * Processes the element.
   */
  public abstract void process(Object elem);

  /**
   * Finishes this Operation's execution.
   *
   * <p>Called after all predecessors producing operations have been finished.
   */
  public void finish() {
    for (OutputReceiver receiver : receivers) {
      if (receiver == null) {
        continue;
      }
      for (Operation operation : receiver.getReceivingOperations()) {
        operation.finish();
      }
    }
  }

  public List<OutputReceiver> getOutputReceivers() {
    return ImmutableList.copyOf(receivers);
  }

  /**
   * Adds an input to this ParDoOperation, coming from the given output of the given source.
   */
  public void attachInput(Operation source, int outputNum) {
    OutputReceiver fanOut = source.receivers[outputNum];
    fanOut.addOutput(this);
  }
}
