/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.batch.BatchFlowTranslator;
import cz.seznam.euphoria.flink.streaming.StreamingFlowTranslator;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
  @Nullable
  private Duration checkpointInterval;
  
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
   * Specify whether to dump/log the flink execution plan before executing
   * a flow using {@link #submit(Flow)}.
   *
   * @param dumpExecPlan {@code true} to cause the executor dump a flow's execution
   *          plan prior to executing it
   *
   * @return this instance (for method chaining purposes)
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
        if (checkpointInterval != null) {
          LOG.info("Enabled checkpoints every: {}", checkpointInterval);
          environment.getStreamEnv().enableCheckpointing(checkpointInterval.toMillis());
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
        LOG.info("Before execute");
        environment.execute(); // blocking operation
        LOG.info("After execute");
      } catch (Throwable e) {
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

      LOG.info("Before commit");
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
   *
   * @param backend the backend implementation to be used by the executor
   *
   * @return this instance (for method chaining purposes)
   */
  public FlinkExecutor setStateBackend(AbstractStateBackend backend) {
    this.stateBackend = Optional.of(backend);
    return this;
  }

  /**
   * Specifies the interval in which watermarks are emitted.
   *
   * @param interval the interval in which the executor is supposed
   *         to automatically emit watermarks
   *
   * @return this instance (for method chaining purposes)
   */
  public FlinkExecutor setAutoWatermarkInterval(Duration interval) {
    this.autoWatermarkInterval = Objects.requireNonNull(interval);
    return this;
  }

  /**
   * Specifies the interval in which to allow late comers. This will cause
   * the forwarding of the latest available watermark to be delayed up to
   * this amount of time.
   *
   * @param lateness the allowed latest for late comers
   *
   * @return this instance (for method chaining purposes)
   */
  public FlinkExecutor setAllowedLateness(Duration lateness) {
    this.allowedLateness = Objects.requireNonNull(lateness);
    return this;
  }

  /**
   * Pre-register given class to flink for serialization purposes.
   *
   * @param cls the type of objects which flink is supposed to serialize/deserialize
   *
   * @return this instance (for method chaining purposes)
   */
  public FlinkExecutor registerClass(Class<?> cls) {
    registeredClasses.add(cls);
    return this;
  }

  /**
   * Set the check-pointing interval.
   *
   * @param interval the check-pointing interval for flink
   *
   * @return this instance (for method chaining purposes)
   */
  public FlinkExecutor setCheckpointInterval(@Nonnull Duration interval) {
    this.checkpointInterval = Objects.requireNonNull(interval);
    return this;
  }
}
