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
package org.apache.beam.sdk.extensions.euphoria.core.executor.graph;

import com.google.common.collect.Iterables;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** DAG test suite. */
public class DAGTest {

  @Test
  public void testAdd() {
    DAG<Integer> dag = DAG.of(1);
    assertEquals(dag.size(), 1);
  }

  @Test
  public void testGetNodeExists() {
    DAG<Integer> dag = DAG.of(1);
    assertEquals(new Integer(1), dag.getNode(1).get());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetNodeDoesntExists() {
    DAG<Integer> dag = DAG.of(1);
    dag.getNode(2);
  }

  @Test
  public void testGetRoots() {
    DAG<Integer> dag = DAG.of(1, 2, 3);
    Collection<Node<Integer>> roots = dag.getRoots();
    assertEquals(3, roots.size());
    assertTrue(roots.contains(new Node<>(1)));
    assertTrue(roots.contains(new Node<>(2)));
    assertTrue(roots.contains(new Node<>(3)));
  }

  @Test
  public void testGetLeafs() {
    DAG<Integer> dag = DAG.of(1, 2, 3);
    // 4 is child of 2 and 3
    dag.add(4, 2, 3);
    // 5 is a child of 4 and 2
    dag.add(5, 4, 2);
    // 6 if a child of 5 and 3
    dag.add(6, 5, 3);
    // 1 and 6 are the leafs
    Collection<Node<Integer>> leafs = dag.getLeafs();
    assertEquals(2, leafs.size());
    assertTrue(leafs.contains(new Node<>(1)));
    assertTrue(leafs.contains(new Node<>(6)));
  }

  @Test
  public void testParentSubGraph() {
    DAG<Integer> dag = DAG.of(1, 2, 3);
    // 4 is child of 2 and 3
    dag.add(4, 2, 3);
    // 5 is a child of 4 and 2
    dag.add(5, 4, 2);
    // 6 if a child of 5 and 3
    dag.add(6, 5, 3);
    // 1 and 6 are the leafs

    // first test the subgraph of leaf 1
    DAG<Integer> oneSubgraph = dag.parentSubGraph(1);

    assertEquals(1, oneSubgraph.size());
    Node<Integer> one = Iterables.getOnlyElement(oneSubgraph.getRoots());
    assertEquals(new Integer(1), one.get());
    assertEquals(0, one.getParents().size());
    assertEquals(0, one.getChildren().size());
    assertTrue(oneSubgraph.getLeafs().contains(new Node<>(1)));
    assertTrue(oneSubgraph.getRoots().contains(new Node<>(1)));

    // second test the subgraph of leaf 6
    // this graph consists of nodes 2, 3, 4, 5 and 6
    DAG<Integer> sixSubgraph = dag.parentSubGraph(6);

    assertEquals(5, sixSubgraph.size());
    Node<Integer> six = Iterables.getOnlyElement(sixSubgraph.getLeafs());
    assertEquals(six, sixSubgraph.getNode(6));
    assertEquals(new Integer(6), six.get());
    assertEquals(2, six.getParents().size());
    assertTrue(six.getParents().contains(new Node<>(5)));
    assertTrue(six.getParents().contains(new Node<>(3)));
    Node<Integer> five = sixSubgraph.getNode(5);
    assertEquals(2, five.getParents().size());
    assertTrue(five.getParents().contains(new Node<>(2)));
    assertTrue(five.getParents().contains(new Node<>(4)));
    assertEquals(1, five.getChildren().size());
    Node<Integer> four = sixSubgraph.getNode(4);
    assertEquals(2, four.getParents().size());
    assertTrue(four.getParents().contains(new Node<>(2)));
    assertTrue(four.getParents().contains(new Node<>(3)));
    assertEquals(1, four.getChildren().size());
    Node<Integer> three = sixSubgraph.getNode(3);
    assertEquals(0, three.getParents().size());
    assertEquals(2, three.getChildren().size());
    assertTrue(three.getChildren().contains(new Node<>(6)));
    assertTrue(three.getChildren().contains(new Node<>(4)));
    Node<Integer> two = sixSubgraph.getNode(2);
    assertEquals(0, two.getParents().size());
    assertEquals(2, two.getChildren().size());
    assertTrue(two.getChildren().contains(new Node<>(5)));
    assertTrue(two.getChildren().contains(new Node<>(4)));

    // and last test the subgraph of node 5
    DAG<Integer> fiveSubgraph = dag.parentSubGraph(5);

    assertEquals(4, fiveSubgraph.size());
    five = Iterables.getOnlyElement(fiveSubgraph.getLeafs());
    assertEquals(five, fiveSubgraph.getNode(5));
    assertEquals(new Integer(5), five.get());
    assertEquals(2, five.getParents().size());
    assertTrue(five.getParents().contains(new Node<>(2)));
    assertTrue(five.getParents().contains(new Node<>(4)));
    four = fiveSubgraph.getNode(4);
    assertEquals(2, four.getParents().size());
    assertTrue(four.getParents().contains(new Node<>(2)));
    assertTrue(four.getParents().contains(new Node<>(3)));
    assertEquals(1, four.getChildren().size());
    three = fiveSubgraph.getNode(3);
    assertEquals(0, three.getParents().size());
    assertEquals(1, three.getChildren().size());
    assertTrue(three.getChildren().contains(new Node<>(4)));
    two = fiveSubgraph.getNode(2);
    assertEquals(0, two.getParents().size());
    assertEquals(2, two.getChildren().size());
    assertTrue(two.getChildren().contains(new Node<>(5)));
    assertTrue(two.getChildren().contains(new Node<>(4)));
  }

  @Test
  public void testTraversal() {
    DAG<Integer> dag = DAG.of(1, 2, 3);
    // 4 is child of 2 and 3
    dag.add(4, 2, 3);
    // 5 is a child of 4 and 2
    dag.add(5, 4, 2);
    // 6 if a child of 5 and 3
    dag.add(6, 5, 3);
    // 1 and 6 are the leafs
    List<Integer> bfs = dag.traverse().map(Node::get).collect(Collectors.toList());

    assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), bfs);
  }
}
