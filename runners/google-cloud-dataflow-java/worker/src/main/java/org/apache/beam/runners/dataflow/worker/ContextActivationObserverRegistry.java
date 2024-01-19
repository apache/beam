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

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.util.common.ReflectHelpers.ObjectsClassComparator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.slf4j.LoggerFactory;

/**
 * A {@link ContextActivationObserverRegistry} allows creating a {@link ContextActivationObserver}.
 */
public class ContextActivationObserverRegistry {

  static {
    LoggerFactory.getLogger(ContextActivationObserverRegistry.class);
  }

  private static final List<ContextActivationObserver>
      REGISTERED_CONTEXT_ACTIVATION_OBSERVER_FACTORIES;

  static {
    List<ContextActivationObserver> contextActivationObserversToRegister = new ArrayList<>();

    // Enumerate all the ContextActivationObserver.Registrars, adding them to registry.
    Set<ContextActivationObserver.Registrar> registrars =
        Sets.newTreeSet(ObjectsClassComparator.INSTANCE);
    registrars.addAll(
        Lists.newArrayList(
            ServiceLoader.load(
                ContextActivationObserver.Registrar.class, ReflectHelpers.findClassLoader())));

    for (ContextActivationObserver.Registrar registrar : registrars) {
      if (registrar.isEnabled()) {
        contextActivationObserversToRegister.add(registrar.getContextActivationObserver());
      }
    }

    REGISTERED_CONTEXT_ACTIVATION_OBSERVER_FACTORIES =
        ImmutableList.copyOf(contextActivationObserversToRegister);
  }

  /**
   * Creates a ContextActivationObserverRegistry containing registrations for all standard
   * ContextActivationObservers part of the core Java Apache Beam SDK and also any registrations
   * provided by {@link ContextActivationObserver.Registrar}s.
   */
  public static ContextActivationObserverRegistry createDefault() {
    return new ContextActivationObserverRegistry();
  }

  /** The list of {@link ContextActivationObserver}s. */
  private final List<ContextActivationObserver> contextActivationObservers;

  private ContextActivationObserverRegistry() {
    contextActivationObservers = new ArrayList<>(REGISTERED_CONTEXT_ACTIVATION_OBSERVER_FACTORIES);
  }

  /** Returns all {@link ContextActivationObserver}s registered with this registry. */
  public List<ContextActivationObserver> getContextActivationObservers() {
    return contextActivationObservers;
  }
}
