/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.cdap;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.SubmitterLifecycle;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.cdap.context.BatchContextImpl;
import org.apache.beam.sdk.io.cdap.context.BatchSinkContextImpl;
import org.apache.beam.sdk.io.cdap.context.BatchSourceContextImpl;
import org.apache.beam.sdk.io.cdap.context.StreamingSourceContextImpl;
import org.apache.beam.sdk.io.sparkreceiver.ReceiverBuilder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class wrapper for a CDAP plugin. */
@AutoValue
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class Plugin<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(Plugin.class);
  private static final String PREPARE_RUN_METHOD_NAME = "prepareRun";
  private static final String GET_STREAM_METHOD_NAME = "getStream";

  protected @Nullable PluginConfig pluginConfig;
  protected @Nullable Configuration hadoopConfiguration;
  protected @Nullable SubmitterLifecycle cdapPluginObj;

  /** Gets the context of a plugin. */
  public abstract BatchContextImpl getContext();

  /** Gets the main class of a plugin. */
  public abstract Class<?> getPluginClass();

  /** Gets InputFormat or OutputFormat class for a plugin. */
  public @Nullable abstract Class<?> getFormatClass();

  /** Gets InputFormatProvider or OutputFormatProvider class for a plugin. */
  public @Nullable abstract Class<?> getFormatProviderClass();

  /** Gets Spark {@link Receiver} class for a CDAP plugin. */
  public @Nullable abstract Class<? extends Receiver<V>> getReceiverClass();

  /**
   * Gets a {@link SerializableFunction} that defines how to get record offset for CDAP {@link
   * Plugin} class.
   */
  public @Nullable abstract SerializableFunction<V, Long> getGetOffsetFn();

  /**
   * Gets a {@link SerializableFunction} that defines how to get constructor arguments for {@link
   * Receiver} using {@link PluginConfig}.
   */
  public @Nullable abstract SerializableFunction<PluginConfig, Object[]>
      getGetReceiverArgsFromConfigFn();

  /** Sets a plugin config. */
  public Plugin<K, V> withConfig(PluginConfig pluginConfig) {
    this.pluginConfig = pluginConfig;
    return this;
  }

  /** Gets a plugin config. */
  public @Nullable PluginConfig getPluginConfig() {
    return pluginConfig;
  }

  /**
   * Calls {@link SubmitterLifecycle#prepareRun(Object)} method on the {@link #cdapPluginObj}
   * passing needed {@param config} configuration object as a parameter. This method is needed for
   * validating connection to the CDAP sink/source and performing initial tuning.
   */
  public void prepareRun() {
    if (isUnbounded()) {
      // Not needed for unbounded plugins
      return;
    }
    if (cdapPluginObj == null) {
      instantiateCdapPluginObj();
    }
    checkStateNotNull(cdapPluginObj, "Cdap Plugin object can't be null!");
    try {
      cdapPluginObj.prepareRun(getContext());
    } catch (Exception e) {
      LOG.error("Error while prepareRun", e);
      throw new IllegalStateException("Error while prepareRun", e);
    }
    if (getPluginType().equals(PluginConstants.PluginType.SOURCE)) {
      for (Map.Entry<String, String> entry :
          getContext().getInputFormatProvider().getInputFormatConfiguration().entrySet()) {
        getHadoopConfiguration().set(entry.getKey(), entry.getValue());
      }
    } else {
      for (Map.Entry<String, String> entry :
          getContext().getOutputFormatProvider().getOutputFormatConfiguration().entrySet()) {
        getHadoopConfiguration().set(entry.getKey(), entry.getValue());
      }
      getHadoopConfiguration().set(MRJobConfig.ID, String.valueOf(1));
    }
  }

  /** Creates an instance of {@link #cdapPluginObj} using {@link #pluginConfig}. */
  private void instantiateCdapPluginObj() {
    PluginConfig pluginConfig = getPluginConfig();
    checkStateNotNull(pluginConfig, "PluginConfig should be not null!");
    try {
      Constructor<?> constructor = getPluginClass().getDeclaredConstructor(pluginConfig.getClass());
      constructor.setAccessible(true);
      cdapPluginObj = (SubmitterLifecycle) constructor.newInstance(pluginConfig);
    } catch (Exception e) {
      LOG.error("Can not instantiate CDAP plugin class", e);
      throw new IllegalStateException("Can not call prepareRun");
    }
  }

  /** Sets a plugin Hadoop configuration. */
  public Plugin<K, V> withHadoopConfiguration(Class<K> formatKeyClass, Class<V> formatValueClass) {
    Class<?> formatClass = getFormatClass();
    checkStateNotNull(formatClass, "Format class can't be null!");
    PluginConstants.Format formatType = getFormatType();
    PluginConstants.Hadoop hadoopType = getHadoopType();

    getHadoopConfiguration()
        .setClass(hadoopType.getFormatClass(), formatClass, formatType.getFormatClass());
    getHadoopConfiguration().setClass(hadoopType.getKeyClass(), formatKeyClass, Object.class);
    getHadoopConfiguration().setClass(hadoopType.getValueClass(), formatValueClass, Object.class);

    return this;
  }

  /** Sets a plugin Hadoop configuration. */
  public Plugin<K, V> withHadoopConfiguration(Configuration hadoopConfiguration) {
    this.hadoopConfiguration = hadoopConfiguration;
    return this;
  }

  /** Gets a plugin Hadoop configuration. */
  public Configuration getHadoopConfiguration() {
    if (hadoopConfiguration == null) {
      hadoopConfiguration = new Configuration(false);
    }
    return hadoopConfiguration;
  }

  /** Gets a plugin type. */
  public abstract PluginConstants.PluginType getPluginType();

  /** Gets a format type. */
  private PluginConstants.Format getFormatType() {
    return getPluginType() == PluginConstants.PluginType.SOURCE
        ? PluginConstants.Format.INPUT
        : PluginConstants.Format.OUTPUT;
  }

  /** Gets a Hadoop type. */
  private PluginConstants.Hadoop getHadoopType() {
    return getPluginType() == PluginConstants.PluginType.SOURCE
        ? PluginConstants.Hadoop.SOURCE
        : PluginConstants.Hadoop.SINK;
  }

  /** Gets value of a plugin type. */
  public static PluginConstants.PluginType initPluginType(Class<?> pluginClass)
      throws IllegalArgumentException {
    if (StreamingSource.class.isAssignableFrom(pluginClass)
        || BatchSource.class.isAssignableFrom(pluginClass)) {
      return PluginConstants.PluginType.SOURCE;
    } else if (BatchSink.class.isAssignableFrom(pluginClass)) {
      return PluginConstants.PluginType.SINK;
    } else {
      throw new IllegalArgumentException("Provided class should be source or sink plugin");
    }
  }

  /** Initializes {@link BatchContextImpl} for CDAP plugin. */
  public static BatchContextImpl initContext(Class<?> cdapPluginClass) {
    // Init context and determine input or output
    Class<?> contextClass;
    List<Method> methods = new ArrayList<>(Arrays.asList(cdapPluginClass.getDeclaredMethods()));
    Class<?> cdapPluginSuperclass = cdapPluginClass.getSuperclass();
    if (cdapPluginSuperclass != null) {
      methods.addAll(Arrays.asList(cdapPluginSuperclass.getDeclaredMethods()));
    }
    for (Method method : methods) {
      if (method.getName().equals(PREPARE_RUN_METHOD_NAME)) {
        contextClass = method.getParameterTypes()[0];
        if (contextClass.equals(BatchSourceContext.class)) {
          return new BatchSourceContextImpl();
        } else if (contextClass.equals(BatchSinkContext.class)) {
          return new BatchSinkContextImpl();
        }
      } else if (method.getName().equals(GET_STREAM_METHOD_NAME)) {
        return new StreamingSourceContextImpl();
      }
    }
    throw new IllegalStateException("Cannot determine context class");
  }

  /** Gets value of a plugin type. */
  public Boolean isUnbounded() {
    Boolean isUnbounded = null;

    for (Annotation annotation : getPluginClass().getDeclaredAnnotations()) {
      if (annotation.annotationType().equals(io.cdap.cdap.api.annotation.Plugin.class)) {
        String pluginType = ((io.cdap.cdap.api.annotation.Plugin) annotation).type();
        isUnbounded = pluginType != null && pluginType.startsWith("streaming");
      }
    }
    if (isUnbounded == null) {
      throw new IllegalArgumentException("CDAP plugin class must have Plugin annotation!");
    }
    return isUnbounded;
  }

  /** Gets a {@link ReceiverBuilder}. */
  public ReceiverBuilder<V, ? extends Receiver<V>> getReceiverBuilder() {
    checkState(isUnbounded(), "Receiver Builder is supported only for unbounded plugins");

    Class<?> pluginClass = getPluginClass();
    Class<? extends Receiver<V>> receiverClass = getReceiverClass();
    SerializableFunction<PluginConfig, Object[]> getReceiverArgsFromConfigFn =
        getGetReceiverArgsFromConfigFn();
    PluginConfig pluginConfig = getPluginConfig();

    checkStateNotNull(pluginConfig, "Plugin config can not be null!");
    checkStateNotNull(pluginClass, "Plugin class can not be null!");
    checkStateNotNull(receiverClass, "Receiver class can not be null!");
    checkStateNotNull(
        getReceiverArgsFromConfigFn, "Get receiver args from config function can not be null!");

    return new ReceiverBuilder<>(receiverClass)
        .withConstructorArgs(getReceiverArgsFromConfigFn.apply(pluginConfig));
  }

  /**
   * Creates a batch plugin instance.
   *
   * @param newPluginClass class of the CDAP plugin {@link io.cdap.cdap.api.annotation.Plugin}.
   * @param newFormatClass Hadoop Input or Output format class.
   * @param newFormatProviderClass Hadoop Input or Output format provider class.
   */
  public static <K, V> Plugin<K, V> createBatch(
      Class<?> newPluginClass, Class<?> newFormatClass, Class<?> newFormatProviderClass) {
    return Plugin.<K, V>builder()
        .setPluginClass(newPluginClass)
        .setFormatClass(newFormatClass)
        .setFormatProviderClass(newFormatProviderClass)
        .setPluginType(Plugin.initPluginType(newPluginClass))
        .setContext(Plugin.initContext(newPluginClass))
        .build();
  }

  /**
   * Creates a streaming plugin instance.
   *
   * @param newPluginClass class of the CDAP plugin {@link io.cdap.cdap.api.plugin.Plugin}.
   * @param getOffsetFn {@link SerializableFunction} that defines how to get record offset for CDAP
   *     {@link io.cdap.cdap.api.annotation.Plugin} class.
   * @param receiverClass Spark {@link Receiver} class for a CDAP plugin.
   * @param getReceiverArgsFromConfigFn {@link SerializableFunction} that defines how to get
   *     constructor arguments for {@link Receiver} using {@link PluginConfig}.
   */
  public static <K, V> Plugin<K, V> createStreaming(
      Class<?> newPluginClass,
      SerializableFunction<V, Long> getOffsetFn,
      Class<? extends Receiver<V>> receiverClass,
      SerializableFunction<PluginConfig, Object[]> getReceiverArgsFromConfigFn) {
    return Plugin.<K, V>builder()
        .setPluginClass(newPluginClass)
        .setPluginType(Plugin.initPluginType(newPluginClass))
        .setContext(Plugin.initContext(newPluginClass))
        .setGetOffsetFn(getOffsetFn)
        .setReceiverClass(receiverClass)
        .setGetReceiverArgsFromConfigFn(getReceiverArgsFromConfigFn)
        .build();
  }

  /**
   * Creates a streaming plugin instance with default function for getting args for {@link
   * Receiver}.
   *
   * @param newPluginClass class of the CDAP plugin {@link io.cdap.cdap.api.plugin.Plugin}.
   * @param getOffsetFn {@link SerializableFunction} that defines how to get record offset for CDAP
   *     {@link io.cdap.cdap.api.annotation.Plugin} class.
   * @param receiverClass Spark {@link Receiver} class for a CDAP plugin.
   */
  public static <K, V> Plugin<K, V> createStreaming(
      Class<?> newPluginClass,
      SerializableFunction<V, Long> getOffsetFn,
      Class<? extends Receiver<V>> receiverClass) {
    return createStreaming(
        newPluginClass, getOffsetFn, receiverClass, config -> new Object[] {config});
  }

  /** Creates a plugin builder instance. */
  public static <K, V> Builder<K, V> builder() {
    return new AutoValue_Plugin.Builder<>();
  }

  /** Builder class for a {@link Plugin}. */
  @AutoValue.Builder
  public abstract static class Builder<K, V> {
    public abstract Builder<K, V> setPluginClass(Class<?> newPluginClass);

    public abstract Builder<K, V> setFormatClass(Class<?> newFormatClass);

    public abstract Builder<K, V> setFormatProviderClass(Class<?> newFormatProviderClass);

    public abstract Builder<K, V> setGetOffsetFn(SerializableFunction<V, Long> getOffsetFn);

    public abstract Builder<K, V> setGetReceiverArgsFromConfigFn(
        SerializableFunction<PluginConfig, Object[]> getReceiverArgsFromConfigFn);

    public abstract Builder<K, V> setReceiverClass(Class<? extends Receiver<V>> receiverClass);

    public abstract Builder<K, V> setPluginType(PluginConstants.PluginType newPluginType);

    public abstract Builder<K, V> setContext(BatchContextImpl context);

    public abstract Plugin<K, V> build();
  }
}
