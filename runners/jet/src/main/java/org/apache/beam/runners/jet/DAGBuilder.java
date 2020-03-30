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
package org.apache.beam.runners.jet;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

/** Utility class for wiring up Jet DAGs based on Beam pipelines. */
public class DAGBuilder {

  private final DAG dag = new DAG();
  private final int localParallelism;

  private final Map<String, Vertex> edgeStartPoints = new HashMap<>();
  private final Map<String, List<Vertex>> edgeEndPoints = new HashMap<>();
  private final Map<String, Coder> edgeCoders = new HashMap<>();
  private final Map<String, String> pCollsOfEdges = new HashMap<>();
  private final Set<String> sideInputCollections = new HashSet<>();

  private final List<WiringListener> listeners = new ArrayList<>();

  private int vertexId = 0;

  DAGBuilder(JetPipelineOptions options) {
    this.localParallelism = options.getJetDefaultParallelism();
  }

  DAG getDag() {
    wireUp();
    return dag;
  }

  void registerConstructionListeners(WiringListener listener) {
    listeners.add(listener);
  }

  String newVertexId(String transformName) {
    return vertexId++ + " (" + transformName + ")";
  }

  void registerCollectionOfEdge(String edgeId, String pCollId) {
    String prevPCollId = pCollsOfEdges.put(edgeId, pCollId);
    if (prevPCollId != null) {
      throw new RuntimeException("Oops!");
    }
  }

  void registerEdgeStartPoint(String edgeId, Vertex vertex, Coder coder) {
    Objects.requireNonNull(edgeId);
    Objects.requireNonNull(vertex);
    Objects.requireNonNull(coder);

    Vertex prevVertex = edgeStartPoints.put(edgeId, vertex);
    if (prevVertex != null) {
      throw new RuntimeException("Oops!");
    }

    Coder prevCoder = edgeCoders.put(edgeId, coder);
    if (prevCoder != null) {
      throw new RuntimeException("Oops!");
    }
  }

  void registerEdgeEndPoint(String edgeId, Vertex vertex) {
    edgeEndPoints.computeIfAbsent(edgeId, x -> new ArrayList<>()).add(vertex);
  }

  void registerSideInput(PCollectionView<?> view) {
    sideInputCollections.add(view.getTagInternal().getId());
  }

  Vertex addVertex(String id, ProcessorMetaSupplier processorMetaSupplier) {
    return dag.newVertex(id, processorMetaSupplier);
  }

  Vertex addVertex(String id, SupplierEx<Processor> processor) {
    return dag.newVertex(id, processor).localParallelism(localParallelism);
  }

  private void wireUp() {
    new WiringInstaller().wireUp();
  }

  /**
   * Listener that can be registered with a {@link DAGBuilder} in order to be notified when edges
   * are being registered.
   */
  public interface WiringListener {

    void isOutboundEdgeOfVertex(Edge edge, String edgeId, String pCollId, String vertexId);

    void isInboundEdgeOfVertex(Edge edge, String edgeId, String pCollId, String vertexId);
  }

  private class WiringInstaller {

    private final Map<Vertex, Integer> inboundOrdinals = new HashMap<>();
    private final Map<Vertex, Integer> outboundOrdinals = new HashMap<>();

    void wireUp() {
      Collection<String> edgeIds = new HashSet<>();
      edgeIds.addAll(edgeStartPoints.keySet());
      edgeIds.addAll(edgeEndPoints.keySet());

      for (String edgeId : edgeIds) {
        String pCollId = pCollsOfEdges.get(edgeId);
        if (pCollId == null) {
          throw new RuntimeException("Oops!");
        }

        Vertex sourceVertex = edgeStartPoints.get(edgeId);
        if (sourceVertex == null) {
          throw new RuntimeException("Oops!");
        }

        Coder edgeCoder = edgeCoders.get(edgeId);
        if (edgeCoder == null) {
          throw new RuntimeException("Oops!");
        }

        List<Vertex> destinationVertices =
            edgeEndPoints.getOrDefault(edgeId, Collections.emptyList());
        boolean sideInputEdge = sideInputCollections.contains(pCollId);
        for (Vertex destinationVertex : destinationVertices) {
          addEdge(sourceVertex, destinationVertex, edgeCoder, edgeId, pCollId, sideInputEdge);
        }
      }
    }

    private void addEdge(
        Vertex sourceVertex,
        Vertex destinationVertex,
        Coder coder,
        String edgeId,
        String pCollId,
        boolean sideInputEdge) {
      try {
        Edge edge =
            Edge.from(sourceVertex, getNextFreeOrdinal(sourceVertex, false))
                .to(destinationVertex, getNextFreeOrdinal(destinationVertex, true));
        edge = edge.distributed();
        if (sideInputEdge) {
          edge = edge.broadcast();
        } else {
          edge = edge.partitioned(new PartitionedKeyExtractor(coder));
        }
        dag.edge(edge);

        String sourceVertexName = sourceVertex.getName();
        String destinationVertexName = destinationVertex.getName();
        for (WiringListener listener : listeners) {
          listener.isInboundEdgeOfVertex(edge, edgeId, pCollId, destinationVertexName);
          listener.isOutboundEdgeOfVertex(edge, edgeId, pCollId, sourceVertexName);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private int getNextFreeOrdinal(Vertex vertex, boolean inbound) {
      Map<Vertex, Integer> ordinals = inbound ? inboundOrdinals : outboundOrdinals;
      int nextOrdinal = 1 + ordinals.getOrDefault(vertex, -1);
      ordinals.put(vertex, nextOrdinal);
      return nextOrdinal;
    }
  }

  private static class PartitionedKeyExtractor<K, V> implements FunctionEx<byte[], Object> {
    private final WindowedValue.WindowedValueCoder<KV<K, V>> coder;

    PartitionedKeyExtractor(Coder coder) {
      this.coder =
          Utils.isKeyedValueCoder(coder)
              ? (WindowedValue.WindowedValueCoder<KV<K, V>>) coder
              : null;
    }

    @Override
    public Object applyEx(byte[] b) throws Exception {
      if (coder == null) {
        return "ALL";
      } else {
        WindowedValue<KV<K, V>> windowedValue =
            CoderUtils.decodeFromByteArray(coder, b); // todo: decoding twice....
        KvCoder<K, V> kvCoder = (KvCoder<K, V>) coder.getValueCoder();
        return CoderUtils.encodeToByteArray(
            kvCoder.getKeyCoder(), windowedValue.getValue().getKey());
      }
    }
  }
}
