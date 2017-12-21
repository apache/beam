/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.accumulators.VoidAccumulatorProvider;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.shadow.com.google.common.collect.Sets;
import cz.seznam.euphoria.spark.accumulators.SparkAccumulatorFactory;
import java.util.Arrays;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

/**
 * Executor implementation using Apache Spark as a runtime.
 */
public class SparkExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(SparkExecutor.class);

  private final JavaSparkContext sparkContext;
  private final ExecutorService submitExecutor = Executors.newCachedThreadPool();

  private SparkAccumulatorFactory accumulatorFactory =
          new SparkAccumulatorFactory.Adapter(VoidAccumulatorProvider.getFactory());

  public SparkExecutor(SparkConf conf) {
    sparkContext = new JavaSparkContext(conf);
  }

  public SparkExecutor() {
    this(new SparkConf());
  }

  @Override
  public CompletableFuture<Result> submit(Flow flow) {
    return CompletableFuture.supplyAsync(() -> execute(flow), submitExecutor);
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down spark executor.");
    sparkContext.close(); // works with spark.yarn.maxAppAttempts=1 otherwise yarn will restart the appmaster
    submitExecutor.shutdownNow();
  }

  /**
   * Set accumulator provider that will be used to collect metrics and counters.
   * When no provider is set a default instance of {@link VoidAccumulatorProvider}
   * will be used.
   *
   * @param factory Factory to create an instance of accumulator provider.
   */
  public void setAccumulatorProvider(SparkAccumulatorFactory factory) {
    this.accumulatorFactory = Objects.requireNonNull(factory);
  }

  @Override
  public void setAccumulatorProvider(AccumulatorProvider.Factory factory) {
    this.accumulatorFactory = new SparkAccumulatorFactory.Adapter(
            Objects.requireNonNull(factory));
  }

  private Result execute(Flow flow) {
    if (!isBoundedInput(flow)) {
      throw new UnsupportedOperationException("Spark executor doesn't support unbounded input");
    }

    // clone the accumulators factory first to make sure
    // each running flow owns its own instance
    SparkAccumulatorFactory clonedFactory = SerializationUtils.clone(accumulatorFactory);

    // init accumulators
    clonedFactory.init(sparkContext);

    List<DataSink<?>> sinks = Collections.emptyList();
    try {
      // FIXME blocking operation in Spark
      SparkFlowTranslator translator =
              new SparkFlowTranslator(sparkContext, flow.getSettings(), clonedFactory);
      sinks = translator.translateInto(flow);
    } catch (Exception e) {
      // FIXME in case of exception list of sinks will be empty
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

    return new Result();
  }

  /**
   * Checks if the given {@link Flow} reads bounded inputs
   *
   * @param flow the flow to inspect
   *
   * @return {@code true} if all sources are bounded
   */
  protected boolean isBoundedInput(Flow flow) {
    // check if sources are bounded or not
    for (Dataset<?> ds : flow.sources()) {
      if (!ds.isBounded()) {
        return false;
      }
    }
    return true;
  }


  /**
   * Pre-register given classes to spark's kryo for serialization purposes.
   *
   * @param classes the classes types to register
   * @return this
   */
  public SparkExecutor registerClasses(Class<?>... classes) {
    this.sparkContext.getConf().registerKryoClasses(
        Stream.concat(
            Arrays.stream(classes),
            getDefaultClasses().stream())
        .toArray(l -> new Class[l]));
    return this;
  }

  // return classes that should be registered by default
  // because the flink executor (might) use them by default
  private Set<Class<?>> getDefaultClasses() {
    return Sets.newHashSet(
        Pair.class,
        Window.class,
        GlobalWindowing.Window.class,
        TimeInterval.class,
        SparkElement.class);
  }


}
