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
package org.apache.beam.runners.jet.processors;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.SupplierEx;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * /** * Jet {@link com.hazelcast.jet.core.Processor} implementation for Beam's Impulse primitive.
 */
public class ImpulseP extends AbstractProcessor {

  private final String ownerId; // do not remove it, very useful for debugging

  private ImpulseP(String ownerId) {
    this.ownerId = ownerId;
  }

  public static SupplierEx<Processor> supplier(String ownerId) {
    return () -> new ImpulseP(ownerId);
  }

  @Override
  public boolean complete() {
    return tryEmit(
        WindowedValue.valueInGlobalWindow(
            new byte[0])); // todo: should EACH processor emit this byte[] or just a SINGLE one?
  }
}
