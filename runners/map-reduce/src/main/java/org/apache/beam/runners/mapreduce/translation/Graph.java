package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Created by peihe on 06/07/2017.
 */
public class Graph {

  private final Map<PTransform, Vertex> vertices;
  private final Map<HeadTail, Edge> edges;
  private final Set<Vertex> leafVertices;

  public Graph() {
    this.vertices = Maps.newHashMap();
    this.edges = Maps.newHashMap();
    this.leafVertices = Sets.newHashSet();
  }

  public Vertex addVertex(PTransform<?, ?> transform) {
    checkState(!vertices.containsKey(transform));
    Vertex v = new Vertex(transform);
    vertices.put(transform, v);
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

  public Vertex getVertex(PTransform<?, ?> transform) {
    return vertices.get(transform);
  }

  public Edge getEdge(Vertex head, Vertex tail) {
    return edges.get(HeadTail.of(head, tail));
  }

  public Set<Vertex> getLeafVertices() {
    return leafVertices;
  }

  public void accept(GraphVisitor visitor) {
    for (Vertex v : leafVertices) {
      v.accept(visitor);
    }
  }

  //TODO: add equals, hashCode, toString for following classses.

  public static class Vertex {
    private final PTransform<?, ?> transform;
    private final Set<Edge> incoming;
    private final Set<Edge> outgoing;

    public Vertex(PTransform transform) {
      this.transform = checkNotNull(transform, "transform");
      this.incoming = Sets.newHashSet();
      this.outgoing = Sets.newHashSet();
    }

    public PTransform<?, ?> getTransform() {
      return transform;
    }

    public Set<Edge> getIncoming() {
      return incoming;
    }

    public Set<Edge> getOutgoing() {
      return outgoing;
    }

    public boolean isSource() {
      return transform instanceof Read.Bounded || transform instanceof Read.Unbounded;
    }

    public boolean isGroupByKey() {
      return transform instanceof GroupByKey;
    }

    public void addIncoming(Edge edge) {
      incoming.add(edge);
    }

    public void addOutgoing(Edge edge) {
      outgoing.add(edge);
    }

    public void accept(GraphVisitor visitor) {
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
  }

  public static class NodePath {
    private final LinkedList<PTransform<?, ?>> path;

    public NodePath() {
      this.path = new LinkedList<>();
    }

    public NodePath(NodePath nodePath) {
      this.path = new LinkedList<>(nodePath.path);
    }

    public void addFirst(PTransform<?, ?> transform) {
      path.addFirst(transform);
    }

    public void addLast(PTransform<?, ?> transform) {
      path.addLast(transform);
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
