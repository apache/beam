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

import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.SubmitterLifecycle;
import io.cdap.cdap.etl.api.batch.BatchContext;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.beam.sdk.io.cdap.context.BatchContextImpl;
import org.apache.beam.sdk.io.cdap.context.BatchSinkContextImpl;
import org.apache.beam.sdk.io.cdap.context.BatchSourceContextImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for working with any CDAP plugin class that marked with {@link Plugin}
 *
 * @param <T> CDAP plugin class
 */
@SuppressWarnings({"unchecked", "rawtypes", "UnusedVariable"})
public class CdapPlugin<T extends SubmitterLifecycle> {

  private static final Logger LOG = LoggerFactory.getLogger(CdapPlugin.class);

  private enum Format {
    INPUT("mapreduce.job.inputformat.class", "key.class", "value.class"),
    OUTPUT("mapreduce.job.outputformat.class", "key.class", "value.class");

    private final String formatClass;
    private final String keyClass;
    private final String valueClass;

    Format(String formatClass, String keyClass, String valueClass) {
      this.formatClass = formatClass;
      this.keyClass = keyClass;
      this.valueClass = valueClass;
    }
  }

  private final Format format;
  private final Class<?> cdapPluginClass;
  private final BatchContextImpl context;

  private Configuration hadoopConf;
  private Boolean isUnbounded;

  protected Class<?> formatClass;
  protected Class<?> keyClass;
  protected Class<?> valueClass;

  private T cdapPluginObj;

  public CdapPlugin(Class<T> cdapPluginClass) {
    this.cdapPluginClass = cdapPluginClass;

    // Determine is unbounded or bounded
    for (Annotation annotation : cdapPluginClass.getDeclaredAnnotations()) {
      if (annotation.annotationType().equals(Plugin.class)) {
        String pluginType = ((Plugin) annotation).type();
        isUnbounded = pluginType != null && pluginType.startsWith("streaming");
      }
    }
    if (isUnbounded == null) {
      throw new IllegalArgumentException("CDAP plugin class must have Plugin annotation!");
    }

    // Init context and determine input or output
    Class<?> contextClass = null;
    for (Method method : cdapPluginClass.getDeclaredMethods()) {
      if (method.getName().equals("prepareRun")) {
        contextClass = method.getParameterTypes()[0];
      }
    }
    if (contextClass == null) {
      throw new IllegalStateException("Cannot determine context class");
    }

    if (contextClass.equals(BatchSourceContext.class)) {
      context = new BatchSourceContextImpl();
      format = Format.INPUT;
    } else if (contextClass.equals(BatchSinkContext.class)) {
      context = new BatchSinkContextImpl();
      format = Format.OUTPUT;
    } else {
      // TODO: context = new StreamingSourceContextImpl();
      format = Format.INPUT;
      context = new BatchSourceContextImpl();
    }
  }

  public boolean isUnbounded() {
    return isUnbounded;
  }

  public Configuration getHadoopConf() {
    if (hadoopConf == null) {
      hadoopConf = new Configuration(false);
    }
    return hadoopConf;
  }

  /**
   * Calls {@link SubmitterLifecycle#prepareRun(Object)} method on the {@link #cdapPluginObj}
   * passing needed {@param config} configuration object as a parameter. This method is needed for
   * setting up {@link #formatClass}, validating connection to the CDAP sink/source and performing
   * initial tuning.
   */
  public void prepareRun(PluginConfig config) {
    // TODO: validate

    if (cdapPluginObj == null) {
      for (Constructor<?> constructor : cdapPluginClass.getDeclaredConstructors()) {
        constructor.setAccessible(true);
        try {
          cdapPluginObj = (T) constructor.newInstance(config);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
          LOG.error("Can not instantiate CDAP plugin class", e);
          throw new IllegalStateException("Can not call prepareRun");
        }
      }
    }
    try {
      cdapPluginObj.prepareRun(context);
    } catch (Exception e) {
      LOG.error("Error while prepareRun", e);
      throw new IllegalStateException("Error while prepareRun");
    }
    try {
      this.formatClass = Class.forName(context.getInputFormatProvider().getInputFormatClassName());
    } catch (ClassNotFoundException e) {
      LOG.error("Can not get format class by name", e);
      throw new IllegalStateException("Can not get format class by name");
    }
    getHadoopConf().setClass(format.formatClass, formatClass, InputFormat.class);
    getHadoopConf().setClass(format.keyClass, keyClass, Object.class);
    getHadoopConf().setClass(format.valueClass, valueClass, Object.class);

    for (Map.Entry<String, String> entry :
        context.getInputFormatProvider().getInputFormatConfiguration().entrySet()) {
      getHadoopConf().set(entry.getKey(), entry.getValue());
    }
  }

  public BatchContext getContext() {
    return context;
  }

  public void setKeyClass(Class<?> keyClass) {
    this.keyClass = keyClass;
  }

  public void setValueClass(Class<?> valueClass) {
    this.valueClass = valueClass;
  }
}
