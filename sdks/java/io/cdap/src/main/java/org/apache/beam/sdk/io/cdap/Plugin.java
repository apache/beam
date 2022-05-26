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

import com.google.auto.value.AutoValue;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.common.lang.InstantiatorFactory;
import io.cdap.cdap.etl.api.SubmitterLifecycle;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.io.cdap.context.BatchContextImpl;
import org.apache.beam.sdk.io.cdap.context.BatchSinkContextImpl;
import org.apache.beam.sdk.io.cdap.context.BatchSourceContextImpl;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class wrapper for a CDAP plugin. */
@AutoValue
@SuppressWarnings({
  "rawtypes",
  "unchecked",
  "assignment.type.incompatible",
  "initialization.fields.uninitialized"
})
public abstract class Plugin {

  private static final Logger LOG = LoggerFactory.getLogger(Plugin.class);

  protected @Nullable PluginConfig pluginConfig;
  protected @Nullable Configuration hadoopConfiguration;
  protected @Nullable SubmitterLifecycle cdapPluginObj;

  /** Gets the context of a plugin. */
  public abstract BatchContextImpl getContext();

  /** Gets the main class of a plugin. */
  public abstract Class<?> getPluginClass();

  /** Gets InputFormat or OutputFormat class for a plugin. */
  public abstract Class<?> getFormatClass();

  /** Gets InputFormatProvider or OutputFormatProvider class for a plugin. */
  public abstract Class<?> getFormatProviderClass();

  /** Sets a plugin config. */
  public Plugin withConfig(PluginConfig pluginConfig) {
    this.pluginConfig = pluginConfig;
    return this;
  }

  /** Gets a plugin config. */
  public PluginConfig getPluginConfig() {
    return pluginConfig;
  }

  /**
   * Calls {@link SubmitterLifecycle#prepareRun(Object)} method on the {@link #cdapPluginObj}
   * passing needed {@param config} configuration object as a parameter. This method is needed for
   * validating connection to the CDAP sink/source and performing initial tuning.
   */
  public void prepareRun() {
    if (cdapPluginObj == null) {
      InstantiatorFactory instantiatorFactory = new InstantiatorFactory(false);
      cdapPluginObj =
          (SubmitterLifecycle) instantiatorFactory.get(TypeToken.of(getPluginClass())).create();
    }
    try {
      cdapPluginObj.prepareRun(getContext());
      for (Map.Entry<String, String> entry :
          getContext().getInputFormatProvider().getInputFormatConfiguration().entrySet()) {
        getHadoopConfiguration().set(entry.getKey(), entry.getValue());
      }
    } catch (Exception e) {
      LOG.error("Error while prepareRun", e);
      throw new IllegalStateException("Error while prepareRun");
    }
  }

  /** Sets a plugin Hadoop configuration. */
  public Plugin withHadoopConfiguration(Class<?> formatKeyClass, Class<?> formatValueClass) {
    PluginConstants.Format formatType = getFormatType();
    PluginConstants.Hadoop hadoopType = getHadoopType();

    getHadoopConfiguration()
        .setClass(hadoopType.getFormatClass(), getFormatClass(), formatType.getFormatClass());
    getHadoopConfiguration().setClass(hadoopType.getKeyClass(), formatKeyClass, Object.class);
    getHadoopConfiguration().setClass(hadoopType.getValueClass(), formatValueClass, Object.class);

    return this;
  }

  /** Sets a plugin Hadoop configuration. */
  public Plugin withHadoopConfiguration(Configuration hadoopConfiguration) {
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
    if (BatchSource.class.isAssignableFrom(pluginClass)) {
      return PluginConstants.PluginType.SOURCE;
    } else if (BatchSink.class.isAssignableFrom(pluginClass)) {
      return PluginConstants.PluginType.SINK;
    } else {
      throw new IllegalArgumentException("Provided class should be source or sink plugin");
    }
  }

  public static BatchContextImpl initContext(Class<?> cdapPluginClass) {
    // Init context and determine input or output
    Class<?> contextClass = null;
    List<Method> methods = new ArrayList<>(Arrays.asList(cdapPluginClass.getDeclaredMethods()));
    if (cdapPluginClass.getSuperclass() != null) {
      methods.addAll(Arrays.asList(cdapPluginClass.getSuperclass().getDeclaredMethods()));
    }
    for (Method method : methods) {
      if (method.getName().equals("prepareRun")) {
        contextClass = method.getParameterTypes()[0];
      }
    }
    if (contextClass == null) {
      throw new IllegalStateException("Cannot determine context class");
    }

    if (contextClass.equals(BatchSourceContext.class)) {
      return new BatchSourceContextImpl();
    } else if (contextClass.equals(BatchSinkContext.class)) {
      return new BatchSinkContextImpl();
    } else {
      return new BatchSourceContextImpl();
    }
  }

  /** Gets value of a plugin type. */
  public Boolean isUnbounded() {
    Boolean isUnbounded = null;

    for (io.cdap.cdap.api.annotation.Plugin annotation :
        getPluginClass().getDeclaredAnnotationsByType(io.cdap.cdap.api.annotation.Plugin.class)) {
      String pluginType = annotation.type();
      isUnbounded = pluginType != null && pluginType.startsWith("streaming");
    }
    if (isUnbounded == null) {
      throw new IllegalArgumentException("CDAP plugin class must have Plugin annotation!");
    }
    return isUnbounded;
  }

  /** Creates a plugin instance. */
  public static Plugin create(
      Class<?> newPluginClass, Class<?> newFormatClass, Class<?> newFormatProviderClass) {
    return builder()
        .setPluginClass(newPluginClass)
        .setFormatClass(newFormatClass)
        .setFormatProviderClass(newFormatProviderClass)
        .setPluginType(Plugin.initPluginType(newPluginClass))
        .setContext(Plugin.initContext(newPluginClass))
        .build();
  }

  /** Creates a plugin builder instance. */
  public static Builder builder() {
    return new AutoValue_Plugin.Builder();
  }

  /** Builder class for a {@link Plugin}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setPluginClass(Class<?> newPluginClass);

    public abstract Builder setFormatClass(Class<?> newFormatClass);

    public abstract Builder setFormatProviderClass(Class<?> newFormatProviderClass);

    public abstract Builder setPluginType(PluginConstants.PluginType newPluginType);

    public abstract Builder setContext(BatchContextImpl context);

    public abstract Plugin build();
  }
}
