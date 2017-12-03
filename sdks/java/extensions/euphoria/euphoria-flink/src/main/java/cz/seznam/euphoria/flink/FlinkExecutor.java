/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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

import com.esotericsoftware.kryo.Serializer;
import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.accumulators.VoidAccumulatorProvider;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.AbstractExecutor;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.accumulators.FlinkAccumulatorFactory;
import cz.seznam.euphoria.flink.batch.BatchElement;
import cz.seznam.euphoria.flink.batch.BatchFlowTranslator;
import cz.seznam.euphoria.flink.streaming.StreamingElement;
import cz.seznam.euphoria.flink.streaming.StreamingFlowTranslator;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Executor implementation using Apache Flink as a runtime.
 */
public class FlinkExecutor extends AbstractExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkExecutor.class);

  private final boolean localEnv;
  private boolean dumpExecPlan;
  private Optional<AbstractStateBackend> stateBackend = Optional.empty();
  private Duration autoWatermarkInterval = Duration.ofMillis(200);
  private Duration allowedLateness = Duration.ofMillis(0);
  private Duration latencyTracking = Duration.ofSeconds(2);
  private FlinkAccumulatorFactory accumulatorFactory =
          new FlinkAccumulatorFactory.Adapter(VoidAccumulatorProvider.getFactory());
  private final HashMap<Class<?>, Class<? extends Serializer>> registeredClasses = getDefaultClasses();
  @Nullable
  private Duration checkpointInterval;

  public FlinkExecutor() {
    this(false);
  }

  public FlinkExecutor(boolean localEnv) {
    this.localEnv = localEnv;
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
  public void setAccumulatorProvider(AccumulatorProvider.Factory factory) {
    this.accumulatorFactory = new FlinkAccumulatorFactory.Adapter(
            Objects.requireNonNull(factory));
  }

  /**
   * Set accumulator provider that will be used to collect metrics and counters.
   * When no provider is set a default instance of {@link VoidAccumulatorProvider}
   * will be used.
   *
   * @param factory Factory to create an instance of accumulator provider.
   */
  public void setAccumulatorProvider(FlinkAccumulatorFactory factory) {
    this.accumulatorFactory = Objects.requireNonNull(factory);
  }

  protected Executor.Result execute(Flow flow) {
    try {
      ExecutionEnvironment.Mode mode = ExecutionEnvironment.determineMode(flow);

      LOG.info("Running flow in {} mode", mode);

      ExecutionEnvironment environment = new ExecutionEnvironment(
          mode, localEnv, getParallelism(), registeredClasses);
      environment.getExecutionConfig().setLatencyTrackingInterval(latencyTracking.toMillis());

      Settings settings = flow.getSettings();

      if (mode == ExecutionEnvironment.Mode.STREAMING) {
        stateBackend.ifPresent(be -> environment.getStreamEnv().setStateBackend(be));
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
        translator = createBatchTranslator(
            settings, environment, accumulatorFactory);
      } else {
        translator = createStreamTranslator(
            settings, environment, accumulatorFactory,
            allowedLateness, autoWatermarkInterval);
      }

      List<DataSink<?>> sinks = translator.translateInto(flow);

      if (dumpExecPlan) {
        if (mode == ExecutionEnvironment.Mode.BATCH) {
          // ~ see https://issues.apache.org/jira/browse/FLINK-6296
          LOG.warn("Dumping the executing plan in {} mode" +
                   " cause Flink to fail a succeeding executing attempt" +
                   " in certain scenarios! Please try calling" +
                   " #setDumpExecutionPlan(false) if your flow" +
                   " does start executing.");
        }
        LOG.info("Flink execution plan for {}: {}",
            flow.getName(), environment.dumpExecutionPlan());
      }

      try {
        LOG.debug("Before execute");
        environment.execute(); // blocking operation
        LOG.debug("After execute");
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

      LOG.debug("Before commit");
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
      LOG.debug("After commit");

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

  protected FlowTranslator createBatchTranslator(Settings settings,
                                                 ExecutionEnvironment environment,
                                                 FlinkAccumulatorFactory accumulatorFactory) {
    return new BatchFlowTranslator(settings, environment.getBatchEnv(), accumulatorFactory);
  }

  protected FlowTranslator createStreamTranslator(Settings settings,
                                                  ExecutionEnvironment environment,
                                                  FlinkAccumulatorFactory accumulatorFactory,
                                                  Duration allowedLateness,
                                                  Duration autoWatermarkInterval) {
    return new StreamingFlowTranslator(
            settings, environment.getStreamEnv(), accumulatorFactory,
            allowedLateness, autoWatermarkInterval);
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
   * Specifies the interval at which latency tracking markers will be emited.
   *
   * @param interval the period at which to emit latency tracking metrics
   *
   * @return this instance (for method chaining purposes)
   *
   * @see ExecutionConfig#setLatencyTrackingInterval(long)
   */
  public FlinkExecutor setLatencyTrackingInterval(Duration interval) {
    this.latencyTracking = Objects.requireNonNull(interval);
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
    registeredClasses.put(cls, null);
    return this;
  }

  /**
   * Pre-register given class to flink for serialization purposes.
   *
   * @param cls the type of objects which flink is supposed to serialize/deserialize
   * @param classSeriliazer serilizer
   * @return this instance (for method chaining purposes)
   */
  public FlinkExecutor registerClass(Class<?> cls, Class<? extends Serializer> classSeriliazer) {
    registeredClasses.put(cls, classSeriliazer);
    return this;
  }

  /**
   * Pre-register given classes to flink for serialization purposes.
   *
   * @param classes the classes types to register
   * @return this
   */
  public FlinkExecutor registerClasses(Class<?>... classes) {
    Arrays.stream(classes).forEach(this::registerClass);
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

  /**
   * Retrieve parallelism (in case of local runner).
   * Distributed runner returns always -1.
   *
   * @return explicit parallelism of the runner or -1 if default
   */
  protected int getParallelism() {
    return -1;
  }

  // return classes that should be registered by default
  // because the flink executor (might) use them by default
  private HashMap<Class<?>, Class<? extends Serializer>> getDefaultClasses() {
    HashMap<Class<?>, Class<? extends Serializer>> classSerializerMap = new HashMap<>();
    classSerializerMap.put(Pair.class, null);
    classSerializerMap.put(Window.class, null);
    classSerializerMap.put(GlobalWindowing.class, null);
    classSerializerMap.put(TimeInterval.class, null);
    classSerializerMap.put(BatchElement.class, null);
    classSerializerMap.put(StreamingElement.class, null);
    return classSerializerMap;

  }
}
