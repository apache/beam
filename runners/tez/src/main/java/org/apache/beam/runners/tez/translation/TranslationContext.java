package org.apache.beam.runners.tez.translation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.tez.TezPipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

/**
 * Maintains context data for {@link TransformTranslator}s.
 * Tracks and maintains each individual {@link Vertex} and their {@link Edge} connections.
 */
public class TranslationContext {

  private final TezPipelineOptions pipelineOptions;
  private final TezConfiguration config;

  private AppliedPTransform<?, ?, ?> currentTransform;
  private String currentName;
  private Map<TupleTag<?>, PValue> currentInputs;
  private Map<TupleTag<?>, PValue> currentOutputs;

  private Map<String, Vertex> vertexMap = new HashMap<>();
  private Map<PValue, Vertex> vertexInputMap = new HashMap<>();
  private Map<PValue, Vertex> vertexOutputMap = new HashMap<>();

  private Set<Pair<PValue, PValue>> shuffleSet = new HashSet<>();

  private Map<PValue, DataSourceDescriptor> sourceMap = new HashMap<>();
  private Map<PValue, DataSinkDescriptor> sinkMap = new HashMap<>();

  public TranslationContext(TezPipelineOptions options, TezConfiguration config){
    this.pipelineOptions = options;
    this.config = config;
  }

  public void setCurrentTransform(TransformHierarchy.Node treeNode) {
    this.currentTransform = treeNode.toAppliedPTransform();
    this.currentInputs = treeNode.getInputs();
    this.currentOutputs = treeNode.getOutputs();
    this.currentName = treeNode.getFullName();
  }

  public void addVertex(String name, Vertex vertex, PValue input, PValue output) {
    vertexMap.put(name, vertex);
    vertexInputMap.put(input, vertex);
    vertexOutputMap.put(output, vertex);
  }

  public void addShufflePair(PValue input, PValue output) {
    shuffleSet.add(Pair.of(input, output));
  }

  public Set<Pair<PValue, PValue>> getShuffleSet(){
    return this.shuffleSet;
  }

  public void addSource(PValue output, DataSourceDescriptor dataSource) {
    sourceMap.put(output, dataSource);
  }

  public void addSink(PValue input, DataSinkDescriptor dataSink) {
    sinkMap.put(input, dataSink);
  }

  public TezConfiguration getConfig() {
    return config;
  }

  public AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  public String getCurrentName() {
    return currentName;
  }

  public Map<TupleTag<?>, PValue> getCurrentInputs() {
    return currentInputs;
  }

  public Map<TupleTag<?>, PValue> getCurrentOutputs() {
    return currentOutputs;
  }

  /**
   * Populates the given Tez dag with the context's {@link Vertex} and {@link Edge}.
   * @param dag Empty Tez dag to be populated.
   */
  public void populateDAG(DAG dag){

    for (Vertex v : vertexMap.values()){
      dag.addVertex(v);
    }

    //Add Sources
    sourceMap.forEach( (value, source) -> {
      Vertex vertex = vertexInputMap.get(value);
      if (vertex != null){
        vertex.addDataSource(value.getName(), source);
      }
    });

    //Add Sinks
    sinkMap.forEach( (value, source) -> {
      Vertex vertex = vertexOutputMap.get(value);
      if (vertex != null){
        vertex.addDataSink(value.getName(), source);
      }
    });

    //Add Shuffle Edges
    for (Pair<PValue, PValue> pair : shuffleSet){
      Vertex inputVertex = vertexOutputMap.get(pair.getLeft());
      Vertex outputVertex = vertexInputMap.get(pair.getRight());
      OrderedPartitionedKVEdgeConfig edgeConfig = OrderedPartitionedKVEdgeConfig.newBuilder(
          BytesWritable.class.getName(), BytesWritable.class.getName(), HashPartitioner.class.getName()).build();
      dag.addEdge(Edge.create(inputVertex, outputVertex, edgeConfig.createDefaultEdgeProperty()));
    }

    //Add Edges
    vertexInputMap.forEach( (PValue inputValue, Vertex inputVertex) -> {
      vertexOutputMap.forEach( (outputValue, outputVertex) -> {
        if (inputValue.equals(outputValue)){
          UnorderedKVEdgeConfig edgeConfig = UnorderedKVEdgeConfig.newBuilder(BytesWritable.class.getName(),
              BytesWritable.class.getName()).build();
          dag.addEdge(Edge.create(outputVertex, inputVertex, edgeConfig.createDefaultOneToOneEdgeProperty()));
        }
      });
    });
  }


}
