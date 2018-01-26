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
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeSliding;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.flink.batch.BatchElement;
import cz.seznam.euphoria.flink.streaming.StreamingElement;
import cz.seznam.euphoria.flink.streaming.windowing.KeyedMultiWindowedElement;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

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

  ExecutionEnvironment(
      Mode mode,
      boolean local,
      int parallelism,
      HashMap<Class<?>, Class<? extends Serializer>> registeredClasses) {

    HashMap<Class<?>, Class<? extends Serializer>> toRegister = getClassesToRegister(registeredClasses);

    LOG.info(
        "Creating ExecutionEnvironment mode {} with parallelism {}",
        mode, parallelism);
    if (mode == Mode.BATCH) {
      batchEnv = local ? org.apache.flink.api.java.ExecutionEnvironment.createLocalEnvironment(parallelism) :
              org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment();
      registerClasses(toRegister, batchEnv);
    } else {
      streamEnv = local ? StreamExecutionEnvironment.createLocalEnvironment(parallelism) :
              StreamExecutionEnvironment.getExecutionEnvironment();
      registerClasses(toRegister, streamEnv);
    }
    LOG.info("Registered classes {} within flink's runtime", toRegister);
  }

  public ExecutionConfig getExecutionConfig() {
    if (batchEnv != null) {
      return batchEnv.getConfig();
    } else if (streamEnv != null) {
      return streamEnv.getConfig();
    } else {
      throw new IllegalStateException("No execution environment initialized");
    }
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
   * Determines {@link Mode} of the given flow.
   *
   * @param flow the flow to inspect
   *
   * @return the given flow's mode; never {@code null}
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

  @SuppressWarnings("unchecked")
  private void registerClasses(HashMap<Class<?>, Class<? extends Serializer>> toRegister,
                               org.apache.flink.api.java.ExecutionEnvironment environment) {
    toRegister.forEach((Class<?> key, Class<? extends Serializer> value) -> {
      if (value != null) {
        environment.registerTypeWithKryoSerializer(key, (Class<? extends Serializer<?>>) value);
      } else {
        environment.registerType(key);
      }
    });
  }

  private void registerClasses(HashMap<Class<?>, Class<? extends Serializer>> toRegister,
                               StreamExecutionEnvironment environment) {
    toRegister.forEach((Class<?> key, Class<? extends Serializer> value) -> {
      if (value != null) {
        environment.registerTypeWithKryoSerializer(key, value);
      } else {
        environment.registerType(key);
      }
    });
  }

  private HashMap<Class<?>, Class<? extends Serializer>> getClassesToRegister(
      HashMap<Class<?>, Class<? extends Serializer>> registeredClasses) {
    HashMap<Class<?>, Class<? extends Serializer>> ret = new HashMap<>(registeredClasses);
    // register all types of used windows
    ret.put(GlobalWindowing.Window.class, null);
    ret.put(TimeInterval.class, null);
    ret.put(TimeSliding.SlidingWindowSet.class, null);
    ret.put(Either.class, null);
    ret.put(Pair.class, null);
    ret.put(Triple.class, null);
    ret.put(StreamingElement.class, null);
    ret.put(BatchElement.class, null);
    ret.put(KeyedMultiWindowedElement.class, null);
    return ret;
  }
}
