
package cz.seznam.euphoria.core.client.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A directed acyclic graph of nodes of type T.
 */
public class DAG<T> {

  public static final class Node<T> {
    final List<Node<T>> children = new ArrayList<>();
    final T value;
    final List<Node<T>> parents = new ArrayList<>();
    Node(T value) {
      this.value = value;
    }
    Node(T value, List<Node<T>> parents) {
      this(value);
      this.parents.addAll(parents);
    }

    public List<Node<T>> getParents() {
      return Collections.unmodifiableList(parents);
    }
    public List<Node<T>> getChildren() {
      return Collections.unmodifiableList(children);
    }
    public T get() {
      return value;
    }

    /** Make a copy of this node. */
    Node<T> copy() {
      Node<T> clone = new Node<>(value, parents);
      clone.children.addAll(this.children);
      return clone;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj instanceof Node) {
        return ((Node) obj).value.equals(value);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public String toString() {
      return "Node(" + value.toString() + ")";
    }

  }

  final List<Node<T>> roots = new ArrayList<>();
  final Map<T, Node<T>> nodeMap = new HashMap<>();

  private DAG() {

  }

  /**
   * Construct a new DAG with given nodes as root nodes.
   */
  @SuppressWarnings("unchecked")
  public static <T> DAG<T> of(T ...rootElements) {
    return of(Arrays.asList(rootElements));
  }
  

  /**
   * Construct a new DAG with given nodes as root nodes.
   */
  public static <T> DAG<T> of(Iterable<T> rootElements) {
    DAG<T> ret = new DAG<>();
    for (T elem : rootElements) {
      ret.add(elem);
    }
    return ret;
  }


  /**
   * Add new element.
   * If no parents, add this as a root element.
   */
  public void add(T elem, T... parents) {
    add(elem, Arrays.asList(parents));
  }


  /**
   * Add new element.
   * If no parents, add this as a root element.
   */
  public void add(T elem, List<T> parents) {
    final Node<T> node;
    if (parents.isEmpty()) {
      roots.add(node = new Node<>(elem));
    } else {
      List<Node<T>> parentNodes = parents.stream()
          .map(this::getNode).collect(Collectors.toList());
      node = new Node<>(elem, parentNodes);
      parentNodes.stream().forEach(p -> p.children.add(node));
    }
    if (nodeMap.containsKey(elem)) {
      throw new IllegalArgumentException(
          "Element " + elem + " is already added to the graph.");
    }
    nodeMap.put(elem, node);
  }


  /**
   * Retrieve node for the given value.
   */
  public Node<T> getNode(T elem) {
    Node<T> ret = nodeMap.get(elem);
    if (ret == null) {
      throw new IllegalStateException("No node with value " + elem + " found");
    }
    return ret;
  }


  /**
   * Retrieve list of root nodes.
   */
  public Collection<Node<T>> getRoots() {
    return roots;
  }


  /**
   * Retrieve leaf nodes (that is nodes with no children).
   */
  public Collection<Node<T>> getLeafs() {
    return nodeMap.values().stream().filter(n -> n.children.isEmpty())
        .collect(Collectors.toList());
  }


  /**
   * Retrieve a subgraph containing the given node as a single leaf node and
   * a transitive closure of parents.
   */
  public DAG<T> parentSubGraph(T elem) {
    DAG<T> ret = new DAG<>();
    LinkedHashSet<Node<T>> nodeList = new LinkedHashSet<>();
    LinkedList<Node<T>> notYetAdded = new LinkedList<>();
    notYetAdded.add(getNode(elem));
    while (!notYetAdded.isEmpty()) {
      Node<T> node = notYetAdded.pollLast();
      // remove if present, we need to update the position of that node in the list
      nodeList.remove(node);
      nodeList.add(node);
      notYetAdded.addAll(node.parents);
    }
    // reverse the nodeList
    LinkedList<Node<T>> reversedNodes = new LinkedList<>();
    for (Node<T> n : nodeList) {
      reversedNodes.addFirst(n);
    }
    // iterate over the nodeList reversed and add nodes
    for (Node<T> node : reversedNodes) {
      List<T> parents = node.parents
          .stream().map(n -> n.value)
          .collect(Collectors.toList());
      ret.add(node.value, parents);
    }

    return ret;
  }
  

  /** Retrieve number of nodes in the DAG. */
  public int size() {
    return nodeMap.size();
  }


  /** Retrieve read-only collection of nodes of this DAG. */
  public Stream<T> nodes() {
    return nodeMap.values().stream().map(n -> n.value);
  }



}
