package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.util.Settings;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Unified interface for Flink batch and stream execution environments.
 */
public class ExecutionEnvironment {

  private org.apache.flink.api.java.ExecutionEnvironment batchEnv;
  private StreamExecutionEnvironment streamEnv;

  private final Settings settings;

  public enum Mode {
    /**
     * Uses the batch mode of Flink.
     */
    BATCH,

    /**
     * Uses the streaming mode of Flink.
     */
    STREAMING
  }

  public ExecutionEnvironment(Mode mode, boolean local) {
    if (mode == Mode.BATCH) {
      batchEnv = local ? org.apache.flink.api.java.ExecutionEnvironment.createLocalEnvironment() :
              org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment();
      settings = readSettings(batchEnv.getConfig().getGlobalJobParameters());
    } else {
      streamEnv = local ? StreamExecutionEnvironment.createLocalEnvironment() :
              StreamExecutionEnvironment.getExecutionEnvironment();
      settings = readSettings(streamEnv.getConfig().getGlobalJobParameters());
    }
  }


  private Settings readSettings(ExecutionConfig.GlobalJobParameters params) {
    Settings ret = new Settings();
    if (params != null) {
      params.toMap().entrySet().stream()
          .forEach(p -> ret.setString(p.getKey(), p.getValue()));
    }
    return ret;
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

  /** Convert global job parameters to euphoria's config. */
  public Settings getSettings() {
    return settings;
  }
}
