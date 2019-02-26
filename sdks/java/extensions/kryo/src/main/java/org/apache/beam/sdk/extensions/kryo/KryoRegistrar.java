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
package org.apache.beam.sdk.extensions.kryo;

import com.esotericsoftware.kryo.Kryo;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;

/**
 * Interface to be implemented by clients to register their classes with {@link Kryo}.
 *
 * <p>{@link KryoRegistrar KryoRegistrars} implementations have to be stateless and {@link
 * Serializable}.
 */
@Experimental
public interface KryoRegistrar extends Serializable {

  /**
   * Implementations should call variants of {@link Kryo#register(Class)} to register custom classes
   * with given {@link Kryo} instance. It should be stateless and resulting in the same type
   * registrations (including order of registration) every time it is called.
   *
   * @param kryo {@link Kryo} instance to be used for type registration
   */
  void registerClasses(Kryo kryo);
}
