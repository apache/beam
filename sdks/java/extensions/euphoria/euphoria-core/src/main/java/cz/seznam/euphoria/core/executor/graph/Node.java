/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.executor.graph;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A single Node in DAG.
 */
@Audience(Audience.Type.EXECUTOR)
public final class Node<T> {

  final List<Node<T>> children = new ArrayList<>();
  @Nullable
  final T value;
  final List<Node<T>> parents = new ArrayList<>();

  @SuppressWarnings("unchecked")
  private static final Node NULL_NODE = new Node(null);

  Node(@Nullable T value) {
    this.value = value;
  }

  Node(@Nullable T value, List<Node<T>> parents) {
    this(value);
    this.parents.addAll(parents);
  }

  public List<Node<T>> getParents() {
    return Collections.unmodifiableList(parents);
  }

  public Node<T> getSingleParent() {
    if (parents.size() == 1) {
      return parents.get(0);
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

  /**
   * Make a copy of this node.
   *
   * @return a copy of this node sharing the value though
   */
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

  /**
   * @param <T> the type of value of the {@code null} node - can safely be any
   *
   * @return the {@code null} node - node with null value, no children and no parents.
   */
  @SuppressWarnings("unchecked")
  public static <T> Node<T> nullNode() {
    return NULL_NODE;
  }

}
