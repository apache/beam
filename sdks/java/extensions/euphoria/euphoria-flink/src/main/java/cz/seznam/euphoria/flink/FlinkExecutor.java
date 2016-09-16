package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.batch.BatchFlowTranslator;
import cz.seznam.euphoria.flink.streaming.StreamingFlowTranslator;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Future;

/**
 * Executor implementation using Apache Flink as a runtime.
 */
public class FlinkExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkExecutor.class);

  private final boolean localEnv;
  private boolean dumpExecPlan;
  private Optional<AbstractStateBackend> stateBackend = Optional.empty();
  private Duration autoWatermarkInterval = Duration.ofMillis(200);
  private Duration allowedLateness = Duration.ofMillis(0);

  public FlinkExecutor() {
    this(false);
  }

  public FlinkExecutor(boolean localEnv) {
    this.localEnv = localEnv;
  }

  /**
   * Specify whether to dump the flink execution plan before executing
   * a flow using {@link #waitForCompletion(Flow)}.
   */
  public FlinkExecutor setDumpExecutionPlan(boolean dumpExecPlan) {
    this.dumpExecPlan = dumpExecPlan;
    return this;
  }

  @Override
  public Future<Integer> submit(Flow flow) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int waitForCompletion(Flow flow) throws Exception {
    try {
      ExecutionEnvironment.Mode mode = ExecutionEnvironment.determineMode(flow);

      LOG.info("Running flow in {} mode", mode);

      ExecutionEnvironment environment = new ExecutionEnvironment(mode, localEnv);
      Settings settings = flow.getSettings();

      if (mode == ExecutionEnvironment.Mode.STREAMING && stateBackend.isPresent()) {
          environment.getStreamEnv().setStateBackend(stateBackend.get());
      }

      FlowTranslator translator;
      if (mode == ExecutionEnvironment.Mode.BATCH) {
        translator = new BatchFlowTranslator(environment.getBatchEnv());
      } else {
        translator = new StreamingFlowTranslator(
            environment.getStreamEnv(), allowedLateness, autoWatermarkInterval);
      }

      List<DataSink<?>> sinks = translator.translateInto(flow);

      if (dumpExecPlan) {
        LOG.info("Flink execution plan for {}: {}",
            flow.getName(), environment.dumpExecutionPlan());
      }

      try {
        environment.execute(); // blocking operation
      } catch (Exception e) {
        // when exception thrown rollback all sinks
        for (DataSink s : sinks) {
          try {
            s.rollback();
          } catch (Exception ex) {
            LOG.error("Exception during DataSink rollback", ex);
          }
        }
        throw e;
      }

      // when the execution is successful commit all sinks
      Exception ex = null;
      for (DataSink s : sinks) {
        try {
          s.commit();
        } catch (Exception e) {
          // save exception for later and try to commit rest of the sinks
          ex = e;
        }
      }

      // rethrow the exception if any
      if (ex != null) throw ex;

      return 0;
    } catch (Throwable t) {
      t.printStackTrace(System.err);
      LOG.error("Failed to run `waitForCompletion", t);
      throw t;
    }
  }

  /**
   * Set state backend for this executor (used only if needed).
   */
  public FlinkExecutor setStateBackend(AbstractStateBackend backend) {
    this.stateBackend = Optional.of(backend);
    return this;
  }

  /**
   * Specifies the interval in which watermarks are emitted.
   */
  public FlinkExecutor setAutoWatermarkInterval(Duration interval) {
    this.autoWatermarkInterval = Objects.requireNonNull(interval);
    return this;
  }

  /**
   * Specifies the interval in which to allow late comers. This will cause
   * the forwarding of the latest available watermark to be delayed up to
   * this amount of time.
   */
  public FlinkExecutor setAllowedLateness(Duration lateness) {
    this.allowedLateness = Objects.requireNonNull(lateness);
    return this;
  }
}
