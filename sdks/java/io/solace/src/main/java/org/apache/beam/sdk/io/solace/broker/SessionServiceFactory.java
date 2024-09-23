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
package org.apache.beam.sdk.io.solace.broker;

import com.solacesystems.jcsmp.Queue;
import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This abstract class serves as a blueprint for creating `SessionServiceFactory` objects. It introduces a
 * queue property and mandates the implementation of a create() method in concrete subclasses.
 *
 * <p>For basic authentication, use {@link BasicAuthJcsmpSessionServiceFactory}</p>.
 *
 * <p>For other situations, you need to extend this class. Classes extending from this abstract class must implement the `equals` method so two instances
 * can be compared by value, and not by reference. We recommend using AutoValue for that.
 *
 * <pre>{@code
 * @AutoValue
 * public abstract class MyFactory implements SessionServiceClientFactory {
 *
 *   abstract String value1();
 *
 *   abstract String value2();
 *
 *   public static MyFactory create(String value1, String value2) {
 *     return new AutoValue_MyFactory.Builder(value1, value2);
 *   }
 *
 *   ...
 *
 *   @Override
 *   public SessionService create() {
 *     ...
 *   }
 *}</pre>
 */
public abstract class SessionServiceFactory implements Serializable {
  /**
   * A reference to a Queue object. This is set when the pipeline is constructed (in the {@link
   * org.apache.beam.sdk.io.solace.SolaceIO.Read#expand(org.apache.beam.sdk.values.PBegin)} method).
   * This could be used to associate the created SessionService with a specific queue for message
   * handling.
   */
  @Nullable Queue queue;

  /**
   * This is the core method that subclasses must implement. It defines how to construct and return
   * a SessionService object.
   */
  public abstract SessionService create();

  /**
   * You need to override this method to be able to compare these objects by value. We recommend
   * using AutoValue for that.
   */
  @Override
  public abstract boolean equals(@Nullable Object other);

  /**
   * You need to override this method to be able to compare these objects by value. We recommend
   * using AutoValue for that.
   */
  @Override
  public abstract int hashCode();
  /**
   * This method is called in the {@link
   * org.apache.beam.sdk.io.solace.SolaceIO.Read#expand(org.apache.beam.sdk.values.PBegin)} method
   * to set the Queue reference.
   */
  public void setQueue(Queue queue) {
    this.queue = queue;
  }
}
