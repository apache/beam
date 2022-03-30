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
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import java.lang.annotation.Annotation;
import org.apache.hadoop.conf.Configuration;

/** Class wrapper for a CDAP plugin. */
@AutoValue
public abstract class Plugin {
  protected PluginConfig pluginConfig;
  protected Configuration hadoopConfiguration;

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

  /** Sets a plugin Hadoop configuration. */
  public Plugin withHadoopConfiguration(Class<?> formatKeyClass, Class<?> formatValueClass) {
    PluginConstants.Format formatType = getFormatType();
    PluginConstants.Hadoop hadoopType = getHadoopType();

    this.hadoopConfiguration = new Configuration(false);

    this.hadoopConfiguration.setClass(
        hadoopType.getFormatClass(), getFormatClass(), formatType.getFormatClass());
    this.hadoopConfiguration.setClass(hadoopType.getKeyClass(), formatKeyClass, Object.class);
    this.hadoopConfiguration.setClass(hadoopType.getValueClass(), formatValueClass, Object.class);

    return this;
  }

  /** Sets a plugin Hadoop configuration. */
  public Plugin withHadoopConfiguration(Configuration hadoopConfiguration) {
    this.hadoopConfiguration = hadoopConfiguration;

    return this;
  }

  /** Gets a plugin Hadoop configuration. */
  public Configuration getHadoopConfiguration() {
    return hadoopConfiguration;
  }

  /** Gets a plugin type. */
  public abstract PluginConstants.PluginType getPluginType();

  /** Gets if a plugin is unbounded. */
  public abstract Boolean getIsUnbounded();

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

  /** Gets value of a plugin type. */
  public static Boolean isUnbounded(Class<?> pluginClass) {
    Boolean isUnbounded = null;

    for (Annotation annotation : pluginClass.getDeclaredAnnotations()) {
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

  /** Creates a plugin instance. */
  public static Plugin create(
      Class<?> newPluginClass, Class<?> newFormatClass, Class<?> newFormatProviderClass) {
    return builder()
        .setPluginClass(newPluginClass)
        .setFormatClass(newFormatClass)
        .setFormatProviderClass(newFormatProviderClass)
        .setPluginType(Plugin.initPluginType(newPluginClass))
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

    public abstract Builder setIsUnbounded(Boolean isUnbounded);

    public abstract Plugin build();
  }
}
