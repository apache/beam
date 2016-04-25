
package cz.seznam.euphoria.core.client.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A single Node in DAG.
 */
public final class Node<T> {

  final List<Node<T>> children = new ArrayList<>();
  final T value;
  final List<Node<T>> parents = new ArrayList<>();

  @SuppressWarnings("unchecked")
  private static final Node nullNode = new Node(null);

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

  public Node<T> getSingleParent() {
    if (parents.size() == 1) {
      return parents.iterator().next();
    }
    throw new IllegalStateException("Asked for single parent while node has parents "
        + parents);
  }

  public Node<T> getSingleParentOrNull() {
    if (parents.size() > 1) {
      throw new IllegalStateException("Node has too many parents: " + parents.size());
    }
    if (parents.isEmpty()) {
      return Node.nullNode();
    }
    return parents.iterator().next();
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
  @SuppressWarnings(value = "unchecked")
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof Node) {
      return Objects.equals(((Node) obj).value, value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    if (value != null) {
      return value.hashCode();
    }
    return 0;
  }

  @Override
  public String toString() {
    return "Node(" + String.valueOf(value) + ")";
  }

  /** A {@b null} node - node with null value, no children and no parents. */
  @SuppressWarnings("unchecked")
  public static <T> Node<T> nullNode() {
    return nullNode;
  }

}
