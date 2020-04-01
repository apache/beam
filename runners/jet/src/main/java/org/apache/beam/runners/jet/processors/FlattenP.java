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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.beam.runners.jet.DAGBuilder;
import org.apache.beam.runners.jet.Utils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;

/** Jet {@link com.hazelcast.jet.core.Processor} implementation for Beam's Flatten primitive. */
public class FlattenP extends AbstractProcessor {

  private final Map<Integer, Coder> inputOrdinalCoders;
  private final Coder outputCoder;

  @SuppressWarnings("FieldCanBeLocal") // do not remove, useful for debugging
  private final String ownerId;

  private FlattenP(Map<Integer, Coder> inputOrdinalCoders, Coder outputCoder, String ownerId) {
    this.inputOrdinalCoders = inputOrdinalCoders;
    this.outputCoder = outputCoder;
    this.ownerId = ownerId;
  }

  @Override
  protected boolean tryProcess(int ordinal, @Nonnull Object item) {
    Coder inputCoder = inputOrdinalCoders.get(ordinal);
    WindowedValue<Object> windowedValue = Utils.decodeWindowedValue((byte[]) item, inputCoder);
    return tryEmit(Utils.encode(windowedValue, outputCoder));
  }

  /** Jet {@link Processor} supplier that will provide instances of {@link FlattenP}. */
  public static final class Supplier implements SupplierEx<Processor>, DAGBuilder.WiringListener {

    private final Map<String, Coder> inputCollectionCoders;
    private final Coder outputCoder;
    private final String ownerId;
    private final Map<Integer, Coder> inputOrdinalCoders;

    public Supplier(Map<String, Coder> inputCoders, Coder outputCoder, String ownerId) {
      this.inputCollectionCoders = inputCoders;
      this.outputCoder = outputCoder;
      this.ownerId = ownerId;
      this.inputOrdinalCoders = new HashMap<>();
    }

    @Override
    public Processor getEx() {
      return new FlattenP(inputOrdinalCoders, outputCoder, ownerId);
    }

    @Override
    public void isOutboundEdgeOfVertex(Edge edge, String edgeId, String pCollId, String vertexId) {
      // do nothing
    }

    @Override
    public void isInboundEdgeOfVertex(Edge edge, String edgeId, String pCollId, String vertexId) {
      if (ownerId.equals(vertexId)) {
        Coder coder = inputCollectionCoders.get(edgeId);
        inputOrdinalCoders.put(edge.getDestOrdinal(), coder);
      }
    }
  }
}
