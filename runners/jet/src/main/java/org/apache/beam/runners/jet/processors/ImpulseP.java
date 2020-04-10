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

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.beam.runners.jet.Utils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * /** * Jet {@link com.hazelcast.jet.core.Processor} implementation for Beam's Impulse primitive.
 */
public class ImpulseP extends AbstractProcessor {

  private final boolean active;
  private final Coder outputCoder;
  private final String ownerId; // do not remove it, very useful for debugging

  private ImpulseP(boolean active, Coder outputCoder, String ownerId) {
    this.active = active;
    this.outputCoder = outputCoder;
    this.ownerId = ownerId;
  }

  @Override
  public boolean complete() {
    if (active) {
      return tryEmit(Utils.encode(WindowedValue.valueInGlobalWindow(new byte[0]), outputCoder));
    } else {
      return true;
    }
  }

  public static ProcessorMetaSupplier supplier(Coder outputCoder, String ownerId) {
    return new ImpulseMetaProcessorSupplier(outputCoder, ownerId);
  }

  private static class ImpulseMetaProcessorSupplier implements ProcessorMetaSupplier {

    private final Coder outputCoder;
    private final String ownerId;

    private ImpulseMetaProcessorSupplier(Coder outputCoder, String ownerId) {
      this.outputCoder = outputCoder;
      this.ownerId = ownerId;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(
        @Nonnull List<Address> addresses) {
      return address -> new ImpulseProcessorSupplier(outputCoder, ownerId);
    }
  }

  private static class ImpulseProcessorSupplier<T> implements ProcessorSupplier {
    private final Coder outputCoder;
    private final String ownerId;
    private transient ProcessorSupplier.Context context;

    private ImpulseProcessorSupplier(Coder outputCoder, String ownerId) {
      this.outputCoder = outputCoder;
      this.ownerId = ownerId;
    }

    @Override
    public void init(@Nonnull Context context) {
      this.context = context;
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
      int indexBase = context.memberIndex() * context.localParallelism();
      List<Processor> res = new ArrayList<>(count);
      for (int i = 0; i < count; i++, indexBase++) {
        res.add(new ImpulseP(indexBase == 0, outputCoder, ownerId));
      }
      return res;
    }
  }
}
