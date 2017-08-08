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
package org.apache.beam.runners.mapreduce;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.Set;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * {@link PipelineOptions} for {@link MapReduceRunner}.
 */
public interface MapReducePipelineOptions extends PipelineOptions {

  /** Classes that are used as the boundary in the stack trace to find the callers class name. */
  Set<String> PIPELINE_OPTIONS_FACTORY_CLASSES = ImmutableSet.of(
      PipelineOptionsFactory.class.getName(),
      PipelineOptionsFactory.Builder.class.getName(),
      "org.apache.beam.sdk.options.ProxyInvocationHandler");

  @Description("The jar class of the user Beam program.")
  @Default.InstanceFactory(JarClassInstanceFactory.class)
  Class<?> getJarClass();
  void setJarClass(Class<?> jarClass);

  @Description("The directory for files output.")
  @Default.String("/tmp/mapreduce/")
  String getFileOutputDir();
  void setFileOutputDir(String fileOutputDir);

  class JarClassInstanceFactory implements DefaultValueFactory<Class<?>> {
    @Override
    public Class<?> create(PipelineOptions options) {
      return findCallersClassName(options);
    }

    /**
     * Returns the simple name of the calling class using the current threads stack.
     */
    private static Class<?> findCallersClassName(PipelineOptions options) {
      Iterator<StackTraceElement> elements =
          Iterators.forArray(Thread.currentThread().getStackTrace());
      // First find the PipelineOptionsFactory/Builder class in the stack trace.
      while (elements.hasNext()) {
        StackTraceElement next = elements.next();
        if (PIPELINE_OPTIONS_FACTORY_CLASSES.contains(next.getClassName())) {
          break;
        }
      }
      // Then find the first instance after that is not the PipelineOptionsFactory/Builder class.
      while (elements.hasNext()) {
        StackTraceElement next = elements.next();
        if (!PIPELINE_OPTIONS_FACTORY_CLASSES.contains(next.getClassName())
            && !next.getClassName().contains("com.sun.proxy.$Proxy")
            && !next.getClassName().equals(options.getRunner().getName())) {
          try {
            return Class.forName(next.getClassName());
          } catch (ClassNotFoundException e) {
            break;
          }
        }
      }
      return null;
    }
  }
}
