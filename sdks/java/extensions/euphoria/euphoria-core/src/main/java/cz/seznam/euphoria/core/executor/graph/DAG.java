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
import cz.seznam.euphoria.core.client.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A directed acyclic graph of nodes of type T.
 *
 * @param <T> the type of values in the graph
 */
@Audience(Audience.Type.EXECUTOR)
public class DAG<T> {

  final List<Node<T>> roots = new ArrayList<>();
  final Map<T, Node<T>> nodeMap = new HashMap<>();

  private DAG() {

  }

  /**
   * Constructs a new, empty DAG.
   *
   * @param <T> the type of nodes to be stored in the new DAG
   *
   * @return a new, empty dag
   */
  public static <T> DAG<T> empty() {
    return of(Collections.emptyList());
  }

  /**
   * Construct a new DAG with given nodes as root nodes.
   *
   * @param <T> the type of the nodes
   *
   * @param rootElements the set of root elements to traverse
   *         and build up the DAG from
   *
   * @return the dag of the given root elements
   */
  @SuppressWarnings("unchecked")
  public static <T> DAG<T> of(T ...rootElements) {
    return of(Arrays.asList(rootElements));
  }

  /**
   * Construct a new DAG with given nodes as root nodes.
   *
   * @param <T> the type of the elements/values in the new DAG
   *
   * @param rootElements the set of root elements to traverse
   *         and build up the DAG from
   *
   * @return the dag of the given root elements
   */
  public static <T> DAG<T> of(Iterable<T> rootElements) {
    DAG<T> ret = new DAG<>();
    for (T elem : rootElements) {
      ret.add(elem);
    }
    return ret;
  }


  /**
   * Add new element. If no parents, add this as a root element.
   *
   * @param elem the element to be added to this DAG
   * @param parents the parent elements of <tt>elem</tt>
   *
   * @return this instance (for method chaining purposes)
   */
  @SafeVarargs
  public final DAG<T> add(T elem, T... parents) {
    add(elem, Arrays.asList(parents));
    return this;
  }


  /**
   * Add new element. If no parents, add this as a root element.
   *
   * @param elem the element to be added to this DAG
   * @param parents the parent elements of <tt>elem</tt>
   *
   * @return this instance (for method chaining purposes)
   */
  public DAG<T> add(T elem, List<T> parents) {
    final Node<T> node;
    if (parents.isEmpty()) {
      roots.add(node = new Node<>(elem));
    } else {
      List<Node<T>> parentNodes = parents.stream()
          .map(this::getNode).collect(Collectors.toList());
      node = new Node<>(elem, parentNodes);
      parentNodes.forEach(p -> p.children.add(node));
    }
    if (nodeMap.containsKey(elem)) {
      throw new IllegalArgumentException(
          "Element " + elem + " is already added to the graph.");
    }
    nodeMap.put(elem, node);
    return this;
  }


  /**
   * Retrieves the node for the given value.
   *
   * @param elem the element to find in this DAG
   *
   * @return the node within this DAG hosting the given element
   *
   * @throws IllegalStateException if there is no such node
   */
  public Node<T> getNode(T elem) {
    Node<T> ret = nodeMap.get(elem);
    if (ret == null) {
      throw new IllegalStateException("No node with value " + elem + " found");
    }
    return ret;
  }


  /**
   * @return the list of root nodes of this DAG
   */
  public Collection<Node<T>> getRoots() {
    return roots;
  }


  /**
   * @return a collection of leaf nodes of this DAG, i.e. nodes with no children
   */
  public Collection<Node<T>> getLeafs() {
    return nodeMap.values().stream().filter(n -> n.children.isEmpty())
        .collect(Collectors.toList());
  }


  /**
   * Retrieve a subgraph containing the given node as a single leaf node and
   * a transitive closure of parents.
   *
   * @param elem the element whose sub-graph to identify
   *
   * @return the identified sub-graph
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


  /** @return the number of nodes in this DAG */
  public int size() {
    return nodeMap.size();
  }


  /** @return all nodes of this DAG in no particular order */
  public Stream<T> nodes() {
    return nodeMap.values().stream().map(n -> n.value);
  }

  /**
   * Retrieves a stream of nodes in traversal order (i.e. from non-dependent nodes
   * to those depending on the already served ones.)
   * <p>
   * In the returned stream, a node at a certain position is likely to be a
   * dependency of a node at a later position; reversely, a node at a certain
   * position is guaranteed <i>not to be</i> a dependency of all nodes at earlier
   * positions.
   *
   * @return a stream of all nodes in traversal order
   */
  public Stream<Node<T>> traverse() {
    List<Node<T>> ret = new LinkedList<>();
    Set<Node<T>> closed = new HashSet<>();
    Queue<Node<T>> open = new LinkedList<>();
    open.addAll(this.roots);

    while (!open.isEmpty()) {
      Node<T> next = open.poll();
      if (!closed.contains(next)) {
        if (closed.containsAll(next.parents)) {
          open.addAll(next.children);
          ret.add(next);
          closed.add(next);
        } else {
          // ~ add the node again to the list of nodes to visit;
          // this time at the end of the list such that its
          // missing parents will be visited first
          open.add(next);
        }
      }
    }

    return ret.stream();
  }

  @Override
  public String toString() {
    // iterate the DAG DFS and write it to string
    StringBuilder sb = new StringBuilder();
    Deque<Pair<Integer, Node<T>>> open = new LinkedList<>();
    this.roots.forEach(r -> open.add(Pair.of(0, r)));
    while (!open.isEmpty()) {
      Pair<Integer, Node<T>> poll = open.removeFirst();
      for (int i = 0; i < poll.getFirst(); i++) {
        sb.append(" ");
      }
      sb.append(poll.getSecond().get());
      sb.append("\n");
      poll.getSecond().children
          .forEach(n -> open.addFirst(Pair.of(poll.getFirst() + 1, n)));
    }
    return sb.toString();
  }

}
