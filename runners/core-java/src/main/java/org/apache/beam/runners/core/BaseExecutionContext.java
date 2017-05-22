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
package org.apache.beam.runners.core;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base class for implementations of {@link ExecutionContext}.
 *
 * <p>A concrete subclass should implement {@link #createStepContext} to create the appropriate
 * {@link BaseStepContext} implementation. Any {@code StepContext} created will
 * be cached for the lifetime of this {@link ExecutionContext}.
 *
 * <p>BaseExecutionContext is generic to allow implementing subclasses to return a concrete subclass
 * of {@link BaseStepContext} from {@link #getOrCreateStepContext(String, String)} and
 * {@link #getAllStepContexts()} without forcing each subclass to override the method, e.g.
 * <pre>{@code
 * {@literal @}Override
 * StreamingModeExecutionContext.StepContext getOrCreateStepContext(...) {
 *   return (StreamingModeExecutionContext.StepContext) super.getOrCreateStepContext(...);
 * }
 * }</pre>
 *
 * <p>When a subclass of {@code BaseExecutionContext} has been downcast, the return types of
 * {@link #createStepContext(String, String)},
 * {@link #getOrCreateStepContext(String, String)}, and {@link #getAllStepContexts()}
 * will be appropriately specialized.
 */
public abstract class BaseExecutionContext<T extends StepContext>
    implements ExecutionContext {

  private Map<String, T> cachedStepContexts = new LinkedHashMap<>();

  /**
   * Implementations should override this to create the specific type
   * of {@link BaseStepContext} they need.
   */
  protected abstract T createStepContext(String stepName, String transformName);

  /**
   * Returns the {@link BaseStepContext} associated with the given step.
   */
  @Override
  public T getOrCreateStepContext(String stepName, String transformName) {
    final String finalStepName = stepName;
    final String finalTransformName = transformName;
    return getOrCreateStepContext(
        stepName,
        new CreateStepContextFunction<T>() {
          @Override
          public T create() {
            return createStepContext(finalStepName, finalTransformName);
          }
        });
  }

  /**
   * Factory method interface to create an execution context if none exists during
   * {@link #getOrCreateStepContext(String, CreateStepContextFunction)}.
   */
  protected interface CreateStepContextFunction<T extends org.apache.beam.runners.core.StepContext> {
    T create();
  }

  protected final T getOrCreateStepContext(String stepName,
      CreateStepContextFunction<T> createContextFunc) {
    T context = cachedStepContexts.get(stepName);
    if (context == null) {
      context = createContextFunc.create();
      cachedStepContexts.put(stepName, context);
    }

    return context;
  }

  /**
   * Returns a collection view of all of the {@link BaseStepContext}s.
   */
  @Override
  public Collection<? extends T> getAllStepContexts() {
    return Collections.unmodifiableCollection(cachedStepContexts.values());
  }

}
