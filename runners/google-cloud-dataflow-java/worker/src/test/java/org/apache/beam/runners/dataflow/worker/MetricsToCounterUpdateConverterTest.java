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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.dataflow.model.BoundedTrie;
import com.google.api.services.dataflow.model.BoundedTrieNode;
import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.beam.runners.core.metrics.BoundedTrieData;
import org.apache.beam.runners.core.metrics.StringSetData;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Kind;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MetricsToCounterUpdateConverterTest {

  private static final String STEP_NAME = "testStep";
  private static final String METRIC_NAME = "testMetric";
  private static final String NAMESPACE = "testNamespace";

  private MetricKey createMetricKey() {
    return MetricKey.create(STEP_NAME, MetricName.named(NAMESPACE, METRIC_NAME));
  }

  @Test
  public void testFromStringSet() {
    MetricKey key = createMetricKey();
    boolean isCumulative = false;
    StringSetData stringSetData = StringSetData.create(ImmutableSet.of("a", "b", "c"));

    CounterUpdate counterUpdate =
        MetricsToCounterUpdateConverter.fromStringSet(key, isCumulative, stringSetData);

    assertEquals(
        Kind.SET.toString(), counterUpdate.getStructuredNameAndMetadata().getMetadata().getKind());
    assertFalse(counterUpdate.getCumulative());
    assertEquals(
        stringSetData.stringSet(), new HashSet<>(counterUpdate.getStringList().getElements()));
  }

  @Test
  public void testFromBoundedTrie() {
    MetricKey key = createMetricKey();
    BoundedTrieData boundedTrieData = new BoundedTrieData();
    boundedTrieData.add(ImmutableList.of("ab"));
    boundedTrieData.add(ImmutableList.of("cd"));

    CounterUpdate counterUpdate =
        MetricsToCounterUpdateConverter.fromBoundedTrie(key, true, boundedTrieData);

    assertEquals(
        Kind.SET.toString(), counterUpdate.getStructuredNameAndMetadata().getMetadata().getKind());
    assertTrue(counterUpdate.getCumulative());
    BoundedTrie trie = counterUpdate.getBoundedTrie();
    assertEquals(100, (int) trie.getBound());
    assertNull(trie.getSingleton());

    BoundedTrieNode root = getMiddleNode(ImmutableList.of("ab", "cd"));
    assertEquals(root, trie.getRoot());
  }

  @Test
  public void testGetBoundedTrieNodeLevels() {
    BoundedTrieData boundedTrieData = new BoundedTrieData();
    boundedTrieData.add(ImmutableList.of("ab"));
    boundedTrieData.add(ImmutableList.of("cd"));
    boundedTrieData.add(ImmutableList.of("ef", "gh"));
    boundedTrieData.add(ImmutableList.of("ef", "xy"));

    BoundedTrie actualTrie =
        MetricsToCounterUpdateConverter.getBoundedTrie(boundedTrieData.toProto());

    BoundedTrie expectedTrie = new BoundedTrie();
    expectedTrie.setBound(100);
    BoundedTrieNode root = new BoundedTrieNode();
    Map<String, BoundedTrieNode> rootChildren = new HashMap<>();
    rootChildren.put("ab", getEmptyNode());
    rootChildren.put("cd", getEmptyNode());
    rootChildren.put("ef", getMiddleNode(ImmutableList.of("gh", "xy")));
    root.setChildren(rootChildren);
    root.setTruncated(false);
    expectedTrie.setRoot(root);
    expectedTrie.setSingleton(null);
    assertEquals(expectedTrie, actualTrie);
  }

  @Test
  public void testGetBoundedTrieNodeSingleton() {
    BoundedTrieData boundedTrieData = new BoundedTrieData();
    boundedTrieData.add(ImmutableList.of("ab"));
    BoundedTrie actualTrie =
        MetricsToCounterUpdateConverter.getBoundedTrie(boundedTrieData.toProto());

    BoundedTrie expectedTrie = new BoundedTrie();
    expectedTrie.setBound(100);
    expectedTrie.setSingleton(ImmutableList.of("ab"));
    expectedTrie.setRoot(null);

    assertEquals(expectedTrie, actualTrie);
  }

  private static BoundedTrieNode getMiddleNode(ImmutableList<String> elements) {
    BoundedTrieNode middleNode = new BoundedTrieNode();
    Map<String, BoundedTrieNode> children = new HashMap<>();
    elements.forEach(val -> children.put(val, getEmptyNode()));
    middleNode.setChildren(children);
    middleNode.setTruncated(false);
    return middleNode;
  }

  private static BoundedTrieNode getEmptyNode() {
    BoundedTrieNode leafNode = new BoundedTrieNode();
    leafNode.setChildren(Collections.emptyMap());
    leafNode.setTruncated(false);
    return leafNode;
  }
}
