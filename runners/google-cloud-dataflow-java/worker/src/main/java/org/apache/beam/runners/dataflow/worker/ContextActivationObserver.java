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
package org.apache.beam.runners.dataflow.worker;

import com.google.auto.service.AutoService;
import java.io.Closeable;
import java.util.ServiceLoader;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.sdk.annotations.Experimental;

/**
 * A {@link ContextActivationObserver} provides a contract to register objects with the
 * DataflowExecutionContext.
 *
 * <p>All methods of a {@link ContextActivationObserver} are required to be thread safe.
 */
public interface ContextActivationObserver extends Closeable {
  /**
   * Returns a {@link Closeable} based on the given {@link ExecutionStateTracker}. The closeable
   * will be invoked when the {@link DataflowExecutionContext} is deactivated.
   */
  public Closeable activate(ExecutionStateTracker executionStateTracker);

  /**
   * {@link ContextActivationObserver}s can be automatically registered with this SDK by creating a
   * {@link ServiceLoader} entry and a concrete implementation of this interface.
   *
   * <p>It is optional but recommended to use one of the many build time tools such as {@link
   * AutoService} to generate the necessary META-INF files automatically.
   */
  @Experimental
  public interface Registrar {
    /**
     * Returns a boolean indicating whether DataflowExecutionContext should register this observer
     * or not.
     */
    boolean isEnabled();

    /**
     * Returns a {@link ContextActivationObserver} which will be registered by default within each
     * {@link ContextActivationObserverRegistry registry} instance.
     */
    ContextActivationObserver getContextActivationObserver();
  }
}
