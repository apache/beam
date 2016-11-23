package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.batch.BatchFlowTranslator;
import cz.seznam.euphoria.flink.streaming.StreamingFlowTranslator;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;

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
  private final Set<Class<?>> registeredClasses = new HashSet<>();
  private long checkpointInterval = -1L;
  
  // executor to submit flows, if closed all executions should be interrupted
  private final ExecutorService submitExecutor = Executors.newCachedThreadPool();

  public FlinkExecutor() {
    this(false);
  }

  public FlinkExecutor(boolean localEnv) {
    this.localEnv = localEnv;
    if (localEnv) {
      // flink race condition bug hackfix
      if (!MemorySegmentFactory.isInitialized()) {
        MemorySegmentFactory.initializeFactory(HeapMemorySegment.FACTORY);
      }
    }
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
  public CompletableFuture<Executor.Result> submit(Flow flow) {
    return CompletableFuture.supplyAsync(() -> execute(flow), submitExecutor);
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down flink executor.");
    submitExecutor.shutdownNow();
  }
  
  private Executor.Result execute(Flow flow) {
    try {
      ExecutionEnvironment.Mode mode = ExecutionEnvironment.determineMode(flow);

      LOG.info("Running flow in {} mode", mode);

      ExecutionEnvironment environment = new ExecutionEnvironment(
          mode, localEnv, registeredClasses);
      
      Settings settings = flow.getSettings();

      if (mode == ExecutionEnvironment.Mode.STREAMING) {
        if (stateBackend.isPresent()) {
          environment.getStreamEnv().setStateBackend(stateBackend.get());
        }
        if (checkpointInterval > 0) {
          LOG.info("Enabled checkpoints every {} milliseconds", checkpointInterval);
          environment.getStreamEnv().enableCheckpointing(checkpointInterval);
        } else {
          LOG.warn("Not enabling checkpointing, your flow is probably not fault "
              + "tolerant and/or might encounter performance issues!");
        }
      }

      FlowTranslator translator;
      if (mode == ExecutionEnvironment.Mode.BATCH) {
        translator = createBatchTranslator(settings, environment);
      } else {
        translator = createStreamTranslator(settings, environment, allowedLateness, autoWatermarkInterval);
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
        for (DataSink<?> s : sinks) {
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
      for (DataSink<?> s : sinks) {
        try {
          s.commit();
        } catch (Exception e) {
          // save exception for later and try to commit rest of the sinks
          ex = e;
        }
      }

      // rethrow the exception if any
      if (ex != null) {
        throw ex;
      }

      return new Executor.Result();
    } catch (Throwable t) {
      t.printStackTrace(System.err);
      LOG.error("Failed to run flow " + flow.getName(), t);
      throw new RuntimeException(t);
    }
  }
  
  protected FlowTranslator createBatchTranslator(Settings settings, ExecutionEnvironment environment) {
    return new BatchFlowTranslator(settings, environment.getBatchEnv());
  }
  
  protected FlowTranslator createStreamTranslator(Settings settings, 
                                                  ExecutionEnvironment environment,
                                                  Duration allowedLateness, 
                                                  Duration autoWatermarkInterval) {
    return new StreamingFlowTranslator(environment.getStreamEnv(), allowedLateness, autoWatermarkInterval);
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

  /**
   * Register given class to flink.
   */
  public FlinkExecutor registerClass(Class<?> cls) {
    registeredClasses.add(cls);
    return this;
  }

  /**
   * Set the checkpointing interval in milliseconds.
   */
  public FlinkExecutor setCheckpointInterval(long interval) {
    this.checkpointInterval = interval;
    return this;
  }
}
