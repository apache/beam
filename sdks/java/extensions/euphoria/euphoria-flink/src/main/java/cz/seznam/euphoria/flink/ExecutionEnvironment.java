package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Unified interface for Flink batch and stream execution environments.
 */
public class ExecutionEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionEnvironment.class);

  private org.apache.flink.api.java.ExecutionEnvironment batchEnv;
  private StreamExecutionEnvironment streamEnv;

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

  public ExecutionEnvironment(
      Mode mode, boolean local, Set<Class<?>> registeredClasses) {

    Set<Class<?>> toRegister = getClassesToRegister(registeredClasses);
    
    if (mode == Mode.BATCH) {
      batchEnv = local ? org.apache.flink.api.java.ExecutionEnvironment.createLocalEnvironment() :
              org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment();
      toRegister.forEach(batchEnv::registerType);
    } else {
      streamEnv = local ? StreamExecutionEnvironment.createLocalEnvironment() :
              StreamExecutionEnvironment.getExecutionEnvironment();
      toRegister.forEach(streamEnv::registerType);
    }
    LOG.info("Registered classes {} within flink's runtime", toRegister);
  }

  public void execute() throws Exception {
    if (batchEnv != null) {
      batchEnv.execute();
    } else if (streamEnv != null) {
      streamEnv.execute();
    } else {
      throw new IllegalStateException("No execution environment initialized");
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

  private Set<Class<?>> getClassesToRegister(Set<Class<?>> registeredClasses) {
    HashSet<Class<?>> ret = Sets.newHashSet(registeredClasses);

    // register all types of used windows
    ret.add(Batch.BatchWindow.class);
    ret.add(TimeInterval.class);

    ret.add(Pair.class);
    ret.add(WindowedElement.class);
    return ret;
  }
}
