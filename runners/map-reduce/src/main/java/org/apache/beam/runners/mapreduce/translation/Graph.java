package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Created by peihe on 06/07/2017.
 */
public class Graph {

  private final Map<Step, Vertex> vertices;
  private final Map<HeadTail, Edge> edges;
  private final Set<Vertex> leafVertices;

  public Graph() {
    this.vertices = Maps.newHashMap();
    this.edges = Maps.newHashMap();
    this.leafVertices = Sets.newHashSet();
  }

  public Vertex addVertex(Step step) {
    checkState(!vertices.containsKey(step));
    Vertex v = new Vertex(step);
    vertices.put(step, v);
    leafVertices.add(v);
    return v;
  }

  public Edge addEdge(Vertex head, Vertex tail) {
    HeadTail headTail = HeadTail.of(head, tail);
    checkState(!edges.containsKey(headTail));
    Edge e = new Edge(headTail);
    edges.put(headTail, e);
    head.addOutgoing(e);
    tail.addIncoming(e);
    leafVertices.remove(head);
    return e;
  }

  public Vertex getVertex(Step step) {
    return vertices.get(step);
  }

  public Edge getEdge(Vertex head, Vertex tail) {
    return edges.get(HeadTail.of(head, tail));
  }

  public Iterable<Vertex> getAllVertices() {
    return vertices.values();
  }

  public Iterable<Edge> getAllEdges() {
    return edges.values();
  }

  public Iterable<Vertex> getLeafVertices() {
    return ImmutableList.copyOf(leafVertices);
  }

  public void accept(GraphVisitor visitor) {
    for (Vertex v : leafVertices) {
      v.accept(visitor);
    }
  }

  //TODO: add equals, hashCode, toString for following classses.

  public static class Vertex {
    private final Step step;
    private final Set<Edge> incoming;
    private final Set<Edge> outgoing;

    public Vertex(Step step) {
      this.step = checkNotNull(step, "step");
      this.incoming = Sets.newHashSet();
      this.outgoing = Sets.newHashSet();
    }

    public Step getStep() {
      return step;
    }

    public Set<Edge> getIncoming() {
      return incoming;
    }

    public Set<Edge> getOutgoing() {
      return outgoing;
    }

    public boolean isSource() {
      PTransform<?, ?> transform = step.getTransform();
      return transform instanceof Read.Bounded || transform instanceof Read.Unbounded;
    }

    public boolean isGroupByKey() {
      return step.getTransform() instanceof GroupByKey;
    }

    public void addIncoming(Edge edge) {
      incoming.add(edge);
    }

    public void addOutgoing(Edge edge) {
      outgoing.add(edge);
    }

    public void accept(GraphVisitor visitor) {
      PTransform<?, ?> transform = step.getTransform();
      if (transform instanceof ParDo.SingleOutput || transform instanceof ParDo.MultiOutput) {
        visitor.visitParDo(this);
      } else if (transform instanceof GroupByKey) {
        visitor.visitGroupByKey(this);
      } else if (transform instanceof Read.Bounded) {
        visitor.visitRead(this);
      } else if (transform instanceof Flatten.PCollections
          || transform instanceof Flatten.Iterables) {
        visitor.visitFlatten(this);
      } else {
        throw new RuntimeException("Unexpected transform type: " + transform.getClass());
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof Vertex) {
        Vertex other = (Vertex) obj;
        return step.equals(other.step);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.getClass(), step);
    }

    @Override
    public String toString() {
      return new ReflectionToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .setExcludeFieldNames(new String[] { "outgoing", "incoming" }).toString();
    }
  }

  public static class Edge {
    private final HeadTail headTail;
    private final Set<NodePath> paths;

    public static Edge of(Vertex head, Vertex tail) {
      return of(HeadTail.of(head, tail));
    }

    public static Edge of(HeadTail headTail) {
      return new Edge(headTail);
    }

    private Edge(HeadTail headTail) {
      this.headTail = checkNotNull(headTail, "headTail");
      this.paths = Sets.newHashSet();
    }

    public Vertex getHead() {
      return headTail.getHead();
    }

    public Vertex getTail() {
      return headTail.getTail();
    }

    public Set<NodePath> getPaths() {
      return paths;
    }

    public void addPath(NodePath path) {
      paths.add(checkNotNull(path, "path"));
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof Edge) {
        Edge other = (Edge) obj;
        return headTail.equals(other.headTail) && paths.equals(paths);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(headTail, paths);
    }

    @Override
    public String toString() {
      return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
  }

  public static class NodePath {
    private final LinkedList<Step> path;

    public NodePath() {
      this.path = new LinkedList<>();
    }

    public NodePath(NodePath nodePath) {
      this.path = new LinkedList<>(nodePath.path);
    }

    public void addFirst(Step step) {
      path.addFirst(step);
    }

    public void addLast(Step step) {
      path.addLast(step);
    }

    public Iterable<Step> steps() {
      return ImmutableList.copyOf(path);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof NodePath) {
        NodePath other = (NodePath) obj;
        return path.equals(other.path);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.getClass(), path.hashCode());
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (Step step : path) {
        sb.append(step.getFullName() + "|");
      }
      sb.deleteCharAt(sb.length() - 1);
      return sb.toString();
    }
  }

  @AutoValue
  public abstract static class Step {
    abstract String getFullName();
    // TODO: remove public
    public abstract PTransform<?, ?> getTransform();
    abstract List<TupleTag<?>> getInputs();
    abstract List<TupleTag<?>> getOutputs();

    public static Step of(
        String fullName,
        PTransform<?, ?> transform,
        List<TupleTag<?>> inputs,
        List<TupleTag<?>> outputs) {
      return new org.apache.beam.runners.mapreduce.translation.AutoValue_Graph_Step(
          fullName, transform, inputs, outputs);
    }
  }

  @AutoValue
  public abstract static class HeadTail {
    abstract Vertex getHead();
    abstract Vertex getTail();

    public static HeadTail of(Vertex head, Vertex tail) {
      return new org.apache.beam.runners.mapreduce.translation.AutoValue_Graph_HeadTail(head, tail);
    }
  }
}
