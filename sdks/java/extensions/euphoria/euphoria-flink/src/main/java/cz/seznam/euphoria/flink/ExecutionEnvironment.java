package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Unified interface for Flink batch and stream execution environments
 */
public class ExecutionEnvironment {

  private org.apache.flink.api.java.ExecutionEnvironment batchEnv;
  private StreamExecutionEnvironment streamEnv;

  public ExecutionEnvironment(Mode mode, boolean local) {
    if (mode == Mode.BATCH) {
      batchEnv = local ? org.apache.flink.api.java.ExecutionEnvironment.createLocalEnvironment() :
              org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment();
    } else {
      streamEnv = local ? StreamExecutionEnvironment.createLocalEnvironment() :
              StreamExecutionEnvironment.getExecutionEnvironment();
    }
  }

  public  enum Mode {
    /**
     * Uses the batch mode of Flink
     */
    BATCH,

    /**
     * Uses the streaming mode of Flink
     */
    STREAMING
  }

  public void execute() throws Exception {
    if (batchEnv != null) {
      batchEnv.execute();
    } else {
      streamEnv.execute();
    }
  }

  public String dumpExecutionPlan() throws Exception {
    if (batchEnv != null) {
      return batchEnv.getExecutionPlan();
    } else {
      return streamEnv.getExecutionPlan();
    }
  }

  public org.apache.flink.api.java.ExecutionEnvironment getBatchEnv() {
    if (batchEnv == null) {
      throw new IllegalStateException("Batch environment not initialized");
    }

    return batchEnv;
  }

  public StreamExecutionEnvironment getStreamEnv() {
    if (streamEnv == null) {
      throw new IllegalStateException("Stream environment not initialized");
    }
    return streamEnv;
  }

  /**
   * Determines {@link Mode} from given flow
   */
  public static Mode determineMode(Flow flow) {
    // check if sources are bounded or not
    for (Dataset<?> ds : flow.sources()) {
      if (!ds.isBounded()) {
        return Mode.STREAMING;
      }
    }

    // default mode is batch
    return Mode.BATCH;
  }
}
