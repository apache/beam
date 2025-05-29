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
package org.apache.beam.runners.core.metrics;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrie;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.metrics.BoundedTrieResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/**
 * Experimental and subject to incompatible changes, or even removal, in a future releases.
 *
 * <p>Represents data stored in a bounded trie. This data structure is used to efficiently store and
 * aggregate a collection of string sequences, paths/FQN with a limited size.
 *
 * <p>This class is thread-safe but the underlying BoundedTrieNode contained in it isn't. This is
 * intentional for performance concerns. Hence, this class does not expose the contained node and
 * should not be modified to do so in future when used with multiple threads. This class choose to
 * achieve thread-safety through locks rather than just creating and returning immutable instances
 * to its caller because the combining of a large and wide trie require per-node copy which is more
 * expensive than synchronization.
 *
 * <p>Note: {@link #equals(Object)}, {@link #hashCode()} of this class are not synchronized and if
 * their usage needs synchronization then the client should do it.
 */
@Internal
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@SuppressFBWarnings(
    value = "IS2_INCONSISTENT_SYNC",
    justification = "Some access on purpose are left unsynchronized")
public class BoundedTrieData implements Serializable {

  private static final int DEFAULT_BOUND = 100; // Default maximum size of the trie

  /** Returns the singleton path if this {@link BoundedTrieData} represents a single path. */
  @Nullable private List<String> singleton;

  /**
   * Returns an {@link Optional} containing the root {@link BoundedTrieNode} if this {@link
   * BoundedTrieData} represents a trie.
   */
  @Nullable private BoundedTrieNode root;

  /** Returns the maximum size of the trie. */
  private int bound;

  public BoundedTrieData() {
    this(null, null, DEFAULT_BOUND);
  }

  public BoundedTrieData(List<String> singleton) {
    this(singleton, null, DEFAULT_BOUND);
  }

  public BoundedTrieData(BoundedTrieNode root) {
    this(null, root, DEFAULT_BOUND);
  }

  /**
   * Constructs a new BoundedTrieData object.
   *
   * <p>A BoundedTrieData object represents the data stored in a bounded trie. It can either be a
   * singleton list of strings, or a trie with a given root and bound.
   *
   * @param singleton the singleton list of strings, or null if the data is a trie
   * @param root the root of the trie, or null if the data is a singleton list
   * @param bound the maximum number of elements allowed in the trie
   * @throws IllegalArgumentException if both {@code singleton} and {@code root} are non-null
   */
  public BoundedTrieData(
      @Nullable List<String> singleton, @Nullable BoundedTrieNode root, int bound) {
    assert singleton == null || root == null;
    this.singleton = singleton;
    this.root = root;
    this.bound = bound;
  }

  /** Converts this {@link BoundedTrieData} to its proto {@link BoundedTrie}. */
  public synchronized BoundedTrie toProto() {
    BoundedTrie.Builder builder = BoundedTrie.newBuilder();
    builder.setBound(this.bound);
    if (this.singleton != null) {
      builder.addAllSingleton(this.singleton);
    }
    if (this.root != null) {
      builder.setRoot(this.root.toProto());
    }
    return builder.build();
  }

  /** Creates a {@link BoundedTrieData} instance from its proto {@link BoundedTrie}. */
  public static BoundedTrieData fromProto(BoundedTrie proto) {
    if (proto.hasRoot()) {
      return new BoundedTrieData(
          null, BoundedTrieNode.fromProto(proto.getRoot()), proto.getBound());
    } else {
      return new BoundedTrieData(proto.getSingletonList(), null, proto.getBound());
    }
  }

  /** Returns this {@link BoundedTrieData} as a {@link BoundedTrieNode}. */
  @Nonnull
  private synchronized BoundedTrieNode asTrie() {
    if (this.root != null) {
      return this.root;
    } else {
      BoundedTrieNode trieNode = new BoundedTrieNode();
      if (this.singleton != null) {
        trieNode.add(this.singleton);
      }
      return trieNode;
    }
  }

  /** Returns a new {@link BoundedTrieData} instance that is a deep copy of this instance. */
  public synchronized BoundedTrieData getCumulative() {
    List<String> singleton = this.singleton == null ? null : new ArrayList<>(this.singleton);
    // deep copy
    BoundedTrieNode root = this.root == null ? null : this.root.deepCopy();
    return new BoundedTrieData(singleton, root, this.bound);
  }

  /**
   * Returns {@link BoundedTrieResult}, which represents all path in the bounded trie. The last
   * element in each path is a boolean in string representation denoting whether this path was
   * truncated. i.e. <["a", "b", "false"], ["c", "true"]>
   *
   * @return The set of paths.
   */
  public synchronized BoundedTrieResult extractResult() {
    if (this.root == null) {
      if (this.singleton == null) {
        return BoundedTrieResult.empty();
      } else {
        List<String> list = new ArrayList<>(this.singleton);
        list.add(String.valueOf(false));
        return BoundedTrieResult.create(Collections.singleton(list));
      }
    } else {
      return BoundedTrieResult.create(new HashSet<>(this.root.flattened()));
    }
  }

  /**
   * Adds a new path to this {@link BoundedTrieData} and hence the {@link BoundedTrieData} is
   * modified.
   *
   * @param segments The path to add.
   */
  public synchronized void add(Iterable<String> segments) {
    List<String> segmentsParts = ImmutableList.copyOf(segments);
    if (segmentsParts.isEmpty()) {
      return;
    }
    if (this.singleton == null && this.root == null) {
      // empty case
      this.singleton = segmentsParts;
    } else if (this.singleton != null && this.singleton.equals(segmentsParts)) {
      // skip
    } else {
      if (this.root == null) {
        this.root = this.asTrie();
        this.singleton = null;
      }
      this.root.add(segmentsParts);
      if (this.root.getSize() > this.bound) {
        this.root.trim();
      }
    }
  }

  /**
   * Combines this {@link BoundedTrieData} with another {@link BoundedTrieData} by doing a deep
   * copy.
   *
   * @param other The other {@link BoundedTrieData} to combine with.
   * @return The combined {@link BoundedTrieData}.
   */
  public BoundedTrieData combine(@Nonnull BoundedTrieData other) {
    BoundedTrieData otherDeepCopy;
    // other can be modified in some different thread, and we need to atomically access
    // its fields to combine correctly. Furthermore, doing this whole method under
    // synchronized(other) is not safe as it might lead to deadlock. Assume the current
    // thread got lock on 'this' and is executing combine with `other` and waiting to get a
    // lock on `other` while some other thread is performing `other.combiner(this)` and
    // waiting to get a lock on `this` object.
    // Here it is safe to get a lock on other as we don't yet hold a lock on this to end up with
    // race condition.
    synchronized (other) {
      if (other.root == null && other.singleton == null) {
        return this;
      }
      otherDeepCopy = other.getCumulative();
    }

    synchronized (this) {
      if (this.root == null && this.singleton == null) {
        return otherDeepCopy;
      }
      otherDeepCopy.root = otherDeepCopy.asTrie();
      otherDeepCopy.singleton = null;
      otherDeepCopy.root.merge(this.asTrie());
      otherDeepCopy.bound = Math.min(this.bound, otherDeepCopy.bound);
      while (otherDeepCopy.root.getSize() > otherDeepCopy.bound) {
        otherDeepCopy.root.trim();
      }
      return otherDeepCopy;
    }
  }

  /**
   * Returns the number of paths stored in this trie.
   *
   * @return The size of the trie.
   */
  public synchronized int size() {
    if (this.singleton != null) {
      return 1;
    } else if (this.root != null) {
      return root.getSize();
    } else {
      return 0;
    }
  }

  public synchronized void clear() {
    this.root = null;
    this.singleton = null;
    this.bound = DEFAULT_BOUND;
  }

  /**
   * Checks if the trie contains the given path.
   *
   * @param value The path to check.
   * @return True if the trie contains the path, false otherwise.
   */
  public synchronized boolean contains(@Nonnull List<String> value) {
    if (this.singleton != null) {
      return value.equals(this.singleton);
    } else if (this.root != null) {
      return this.root.contains(value);
    } else {
      return false;
    }
  }

  /** @return true if this {@link BoundedTrieData} is empty else false. */
  public boolean isEmpty() {
    return (root == null || root.children.isEmpty()) && (singleton == null || singleton.isEmpty());
  }

  @Override
  public final boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || this.getClass() != other.getClass()) {
      return false;
    }
    BoundedTrieData that = (BoundedTrieData) other;
    return this.asTrie().equals(that.asTrie());
  }

  @Override
  public final int hashCode() {
    return this.asTrie().hashCode();
  }

  @Override
  public final String toString() {
    return "BoundedTrieData(" + this.asTrie() + ")";
  }

  // ------------------------------ BoundedTrieNode Implementation ------------------------------
  /**
   * BoundedTrieNode implementation. This class is not thread-safe and relies on the {@link
   * BoundedTrieData} which uses this class to ensure thread-safety by acquiring a lock on the root
   * of the tree itself. This avoids acquiring and release N nodes in a path. This class is not
   * intended to be used directly outside of {@link BoundedTrieData} with multiple threads.
   */
  @VisibleForTesting
  public static class BoundedTrieNode implements Serializable {

    public static final String TRUNCATED_TRUE = String.valueOf(true);
    public static final String TRUNCATED_FALSE = String.valueOf(false);
    /**
     * A map from strings to child nodes. Each key represents a segment of a path/FQN, and the
     * corresponding value represents the subtree rooted at that segment.
     */
    private Map<String, BoundedTrieNode> children;

    /**
     * A flag indicating whether this node has been truncated. A truncated node represents an
     * aggregation/roll-up of multiple paths that share a common prefix.
     */
    private boolean truncated;

    /**
     * The size of the subtree rooted at this node. This represents the number of distinct paths
     * that pass through this node.
     */
    private int size;

    /** Constructs an empty `BoundedTrieNode` with size 1 and not truncated. */
    public BoundedTrieNode() {
      this(new HashMap<>(), false, 1);
    }

    /**
     * Constructs a `BoundedTrieNode` with the given children, truncation status, and size.
     *
     * @param children The children of this node.
     * @param truncated Whether this node is truncated.
     * @param size The size of the subtree rooted at this node.
     */
    public BoundedTrieNode(
        @Nonnull Map<String, BoundedTrieNode> children, boolean truncated, int size) {
      this.children = children;
      this.size = size;
      this.truncated = truncated;
    }

    /**
     * Constructs a deep copy of this `BoundedTrieNode`.
     *
     * @return A deep copy of this node.
     */
    BoundedTrieNode deepCopy() {
      BoundedTrieNode copyNode = new BoundedTrieNode();
      copyNode.truncated = this.truncated;
      copyNode.size = this.size;
      copyNode.children = new HashMap<>();
      // deep copy
      this.children.forEach((key, value) -> copyNode.children.put(key, value.deepCopy()));
      return copyNode;
    }

    /**
     * Adds a path represented by the given list of segments to this trie.
     *
     * @param segments The segments of the path to add.
     * @return The change in the size of the subtree rooted at this node.
     */
    int add(List<String> segments) {
      if (truncated || segments.isEmpty()) {
        return 0;
      }
      String head = segments.get(0);
      List<String> tail = segments.subList(1, segments.size());
      boolean wasEmpty = children.isEmpty();
      BoundedTrieNode currChild = children.get(head);
      int delta = 0;
      if (currChild == null) {
        currChild = new BoundedTrieNode();
        children.put(head, currChild);
        delta = wasEmpty ? 0 : 1;
      }
      if (!tail.isEmpty()) {
        delta += currChild.add(tail);
      }
      size += delta;
      return delta;
    }

    /**
     * Adds multiple paths to this trie.
     *
     * @param segmentsIter An iterator over the paths to add.
     * @return The total change in the size of the subtree rooted at this node.
     */
    @VisibleForTesting
    int addAll(List<List<String>> segmentsIter) {
      return segmentsIter.stream().mapToInt(this::add).sum();
    }

    /**
     * Trims this trie by truncating the largest subtree.
     *
     * @return The change in the size of the subtree rooted at this node.
     */
    int trim() {
      if (children.isEmpty()) {
        return 0;
      }
      BoundedTrieNode maxChild =
          Collections.max(children.values(), Comparator.comparingInt(BoundedTrieNode::getSize));
      int delta;
      if (maxChild.size == 1) {
        delta = 1 - size;
        truncated = true;
        children = new HashMap<>();
      } else {
        delta = maxChild.trim();
      }
      size += delta;
      return delta;
    }

    /**
     * Merges the given `BoundedTrieNode` into this node and as a result this node is changed.
     *
     * @param other The node to merge.
     * @return The change in the size of the subtree rooted at this node.
     */
    int merge(BoundedTrieNode other) {
      if (truncated) {
        return 0;
      }
      if (other.truncated) {
        truncated = true;
        children = new HashMap<>();
        int delta = 1 - size;
        size += delta;
        return delta;
      }
      if (other.children.isEmpty()) {
        return 0;
      }
      if (children.isEmpty()) {
        children.putAll(other.children);
        int delta = other.size - this.size;
        size += delta;
        return delta;
      }
      int delta = 0;
      for (Map.Entry<String, BoundedTrieNode> entry : other.children.entrySet()) {
        String prefix = entry.getKey();
        BoundedTrieNode otherChild = entry.getValue();
        BoundedTrieNode thisChild = children.get(prefix);
        if (thisChild == null) {
          children.put(prefix, otherChild);
          delta += otherChild.size;
        } else {
          delta += thisChild.merge(otherChild);
        }
      }
      size += delta;
      return delta;
    }

    /**
     * Returns a flattened representation of this trie.
     *
     * <p>The flattened representation is a list of lists of strings, where each inner list
     * represents a path in the trie and the last element in the list is a boolean in string
     * representation denoting whether this path was truncated. i.e. <["a", "b", "false"], ["c",
     * "true"]>
     *
     * @return The flattened representation of this trie.
     */
    List<List<String>> flattened() {
      List<List<String>> result = new ArrayList<>();
      if (truncated) {
        result.add(Collections.singletonList(TRUNCATED_TRUE));
      } else if (children.isEmpty()) {
        result.add(Collections.singletonList(TRUNCATED_FALSE));
      } else {
        List<String> prefixes = new ArrayList<>(children.keySet());
        Collections.sort(prefixes);
        for (String prefix : prefixes) {
          BoundedTrieNode child = children.get(prefix);
          if (child != null) {
            for (List<String> flattened : child.flattened()) {
              List<String> newList = new ArrayList<>();
              newList.add(prefix);
              newList.addAll(flattened);
              result.add(newList);
            }
          }
        }
      }
      return result;
    }

    /**
     * Converts this `BoundedTrieNode` to proto.
     *
     * @return The {@link org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode}
     *     representation of this node.
     */
    org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode toProto() {
      org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode.Builder builder =
          org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode.newBuilder();
      builder.setTruncated(truncated);
      children.forEach((key, value) -> builder.putChildren(key, value.toProto()));
      return builder.build();
    }

    /**
     * Constructs a `BoundedTrieNode` from proto.
     *
     * @param proto The {@link org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode}
     *     representation of the node.
     * @return The corresponding `BoundedTrieNode`.
     */
    static BoundedTrieNode fromProto(
        org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode proto) {
      BoundedTrieNode node = new BoundedTrieNode();
      if (proto.getTruncated()) {
        node.truncated = true;
        node.children = new HashMap<>();
      } else {
        node.children = new HashMap<>();
        proto
            .getChildrenMap()
            .forEach((key, value) -> node.children.put(key, BoundedTrieNode.fromProto(value)));
        node.size =
            Math.max(1, node.children.values().stream().mapToInt(BoundedTrieNode::getSize).sum());
      }
      return node;
    }

    /**
     * Checks if the trie contains the given path represented by the list of segments.
     *
     * @param segments The segments of the path to check.
     * @return True if the trie contains the path, false otherwise.
     */
    boolean contains(List<String> segments) {
      if (truncated || segments.isEmpty()) {
        return true;
      }
      String head = segments.get(0);
      List<String> tail = segments.subList(1, segments.size());
      return children.containsKey(head) && children.get(head).contains(tail);
    }

    /**
     * Returns the size of the subtree rooted at this node.
     *
     * @return The size of the subtree.
     */
    public int getSize() {
      return size;
    }

    /**
     * Returns whether this node is truncated.
     *
     * @return Whether this node is truncated.
     */
    boolean isTruncated() {
      return truncated;
    }

    @Override
    public int hashCode() {
      int result = 17; // standard prime numbers
      result = 31 * result + size;
      // recursively traverse to calculate hashcode of each node
      result = 31 * result + children.hashCode();
      result = 31 * result + (truncated ? 1 : 0);
      return result;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      BoundedTrieNode that = (BoundedTrieNode) other;
      return truncated == that.truncated && children.equals(that.children);
    }

    @Override
    public String toString() {
      return "{"
          + flattened().stream()
              .map(list -> "'" + String.join("", list) + "'")
              .collect(Collectors.joining(", "))
          + "}";
    }
  }
}
