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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
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
import org.apache.beam.sdk.metrics.BoundedTrieResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/**
 * Represents data stored in a bounded trie. This data structure is used to efficiently store and
 * aggregate a collection of string sequences, paths/FQN with a limited size.
 *
 * <p>The trie can be in one of two states:
 *
 * <ul>
 *   <li>**Singleton:** Contains a single path.
 *   <li>**Trie:** Contains a {@link BoundedTrieNode} representing the root of the trie.
 * </ul>
 */
@AutoValue
public abstract class BoundedTrieData implements Serializable {

  private static final int DEFAULT_BOUND = 100; // Default maximum size of the trie

  /**
   * Returns an {@link Optional} containing the singleton path if this {@link BoundedTrieData}
   * represents a single path.
   */
  public abstract Optional<List<String>> singleton();

  /**
   * Returns an {@link Optional} containing the root {@link BoundedTrieNode} if this {@link
   * BoundedTrieData} represents a trie.
   */
  public abstract Optional<BoundedTrieNode> root();

  /** Returns the maximum size of the trie. */
  public abstract int bound();

  /**
   * Creates a {@link BoundedTrieData} instance.
   *
   * @param singleton The singleton path (optional).
   * @param root The root node of the trie (optional).
   * @param bound The maximum size of the trie.
   * @throws IllegalArgumentException If both or neither of {@code singleton} and {@code root} are
   *     specified or both are null, or if {@code bound} is less than 1.
   */
  public static BoundedTrieData create(
      @Nullable List<String> singleton, @Nullable BoundedTrieNode root, int bound) {
    Preconditions.checkArgument(
        (singleton == null ^ root == null),
        "Either and only one of singleton or root must be specified.");
    Preconditions.checkArgument(bound >= 1, "Bound must be at least 1.");
    return new AutoValue_BoundedTrieData(
        Optional.ofNullable(singleton), Optional.ofNullable(root), bound);
  }

  /**
   * Creates a {@link BoundedTrieData} instance from a {@link BoundedTrieNode} with the default
   * bound.
   *
   * @param root The root node of the trie.
   */
  public static BoundedTrieData create(@Nonnull BoundedTrieNode root) {
    return create(null, root, DEFAULT_BOUND);
  }

  /**
   * Creates a {@link BoundedTrieData} instance from a singleton path with the default bound.
   *
   * @param singleton The singleton path.
   */
  public static BoundedTrieData create(@Nonnull List<String> singleton) {
    return create(singleton, null, DEFAULT_BOUND);
  }

  /** Converts this {@link BoundedTrieData} to its proto {@link BoundedTrie}. */
  public BoundedTrie toProto() {
    BoundedTrie.Builder builder = BoundedTrie.newBuilder();
    builder.setBound(bound());
    singleton().ifPresent(builder::addAllSingleton);
    root().ifPresent(r -> builder.setRoot(r.toProto()));
    return builder.build();
  }

  /** Creates a {@link BoundedTrieData} instance from its proto {@link BoundedTrie}. */
  public static BoundedTrieData fromProto(BoundedTrie proto) {
    List<String> singleton = proto.getSingletonList();
    BoundedTrieNode root = proto.hasRoot() ? BoundedTrieNode.fromProto(proto.getRoot()) : null;
    return create(singleton, root, proto.getBound());
  }

  /** Returns this {@link BoundedTrieData} as a {@link BoundedTrieNode}. */
  public BoundedTrieNode asTrie() {
    return root()
        .orElseGet(
            () -> {
              BoundedTrieNode newRoot = new BoundedTrieNode();
              singleton().ifPresent(newRoot::add);
              return newRoot;
            });
  }

  /** Returns a new {@link BoundedTrieData} instance that is a deep copy of this instance. */
  public BoundedTrieData getCumulative() {
    return root().isPresent()
        ? create(null, new BoundedTrieNode(root().get()), bound())
        : create(singleton().get(), null, bound());
  }

  /** Extracts the data from this {@link BoundedTrieData} as a {@link BoundedTrieResult}. */
  public BoundedTrieResult getBoundedTrieResult() {
    if (root().isPresent()) {
      return BoundedTrieResult.create(new HashSet<>(root().get().flattened()));
    } else if (singleton().isPresent()) {
      List<String> list = new ArrayList<>(singleton().get());
      list.add(String.valueOf(false));
      return BoundedTrieResult.create(ImmutableSet.of(list));
    } else {
      return BoundedTrieResult.empty();
    }
  }

  /**
   * Adds a new path to this {@link BoundedTrieData}.
   *
   * @param segments The path to add.
   * @return A new {@link BoundedTrieData} instance with the added path.
   */
  public BoundedTrieData add(Iterable<String> segments) {
    List<String> segmentsParts = ImmutableList.copyOf(segments);
    if (root().isPresent() && singleton().isPresent()) {
      return create(segmentsParts, null, bound());
    } else if (singleton().isPresent() && singleton().get().equals(segmentsParts)) {
      return this; // Optimize for re-adding the same value.
    } else {
      BoundedTrieNode newRoot = new BoundedTrieNode(asTrie());
      newRoot.add(segmentsParts);
      if (newRoot.getSize() > bound()) {
        newRoot.trim();
      }
      return create(null, newRoot, bound());
    }
  }

  /**
   * Combines this {@link BoundedTrieData} with another {@link BoundedTrieData}.
   *
   * @param other The other {@link BoundedTrieData} to combine with.
   * @return A new {@link BoundedTrieData} instance representing the combined data.
   */
  public BoundedTrieData combine(BoundedTrieData other) {
    if (root().isPresent() && singleton().isPresent()) {
      return other;
    } else if (other.root().isPresent() && other.singleton().isPresent()) {
      return this;
    } else {
      BoundedTrieNode combined = new BoundedTrieNode(asTrie());
      combined.merge(other.asTrie());
      int bound = Math.min(this.bound(), other.bound());
      while (combined.getSize() > bound) {
        combined.trim();
      }
      return create(null, combined, bound);
    }
  }

  /**
   * Returns the number of paths stored in this trie.
   *
   * @return The size of the trie.
   */
  public int size() {
    if (singleton().isPresent()) {
      return 1;
    } else if (root().isPresent()) {
      return root().get().getSize();
    } else {
      return 0;
    }
  }

  /**
   * Checks if the trie contains the given path.
   *
   * @param value The path to check.
   * @return True if the trie contains the path, false otherwise.
   */
  public boolean contains(List<String> value) {
    if (singleton().isPresent()) {
      return value.equals(singleton().get());
    } else if (root().isPresent()) {
      return root().get().contains(value);
    } else {
      return false;
    }
  }

  /** Returns an empty {@link BoundedTrieData} instance. */
  public static BoundedTrieData empty() {
    return EmptyBoundedTrieData.INSTANCE;
  }

  @Override
  public final boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    BoundedTrieData that = (BoundedTrieData) other;
    return this.asTrie().equals(that.asTrie());
  }

  @Override
  public final int hashCode() {
    return asTrie().hashCode();
  }

  @Override
  public final String toString() {
    return "BoundedTrieData(" + asTrie() + ")";
  }

  // ---------------------------- EmptyBoundedTrieData Implementation ---------------------------
  /**
   * An immutable implementation of {@link BoundedTrieData} representing an empty trie. This class
   * provides a singleton instance for efficiency.
   */
  public static class EmptyBoundedTrieData extends BoundedTrieData {

    private static final EmptyBoundedTrieData INSTANCE = new EmptyBoundedTrieData();
    private static final int DEFAULT_BOUND = 1; // Define the default bound here

    private EmptyBoundedTrieData() {}

    /**
     * Returns an {@link Optional} containing an empty list of strings, representing the singleton
     * path in an empty trie.
     */
    @Override
    public Optional<List<String>> singleton() {
      return Optional.of(ImmutableList.of());
    }

    /**
     * Returns an {@link Optional} containing an empty {@link BoundedTrieNode} representing the root
     * of an empty trie.
     */
    @Override
    public Optional<BoundedTrieNode> root() {
      return Optional.of(new BoundedTrieNode(ImmutableMap.of(), false, DEFAULT_BOUND));
    }

    /** Returns the default bound for the empty trie. */
    @Override
    public int bound() {
      return DEFAULT_BOUND;
    }

    /**
     * Returns an empty {@link BoundedTrieResult}. This represents the result of extracting data
     * from an empty trie.
     */
    @Override
    public BoundedTrieResult getBoundedTrieResult() {
      return BoundedTrieResult.empty();
    }
  }

  // ------------------------------ BoundedTrieNode Implementation ------------------------------
  protected static class BoundedTrieNode implements Serializable {
    /**
     * A map from strings to child nodes. Each key represents a segment of a path/FQN, and the
     * corresponding value represents the subtree rooted at that segment.
     */
    @Nonnull private Map<String, BoundedTrieNode> children;

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
    BoundedTrieNode() {
      this(new HashMap<>(), false, 1);
    }

    /**
     * Constructs a `BoundedTrieNode` with the given children, truncation status, and size.
     *
     * @param children The children of this node.
     * @param truncated Whether this node is truncated.
     * @param size The size of the subtree rooted at this node.
     */
    BoundedTrieNode(@Nonnull Map<String, BoundedTrieNode> children, boolean truncated, int size) {
      this.children = children;
      this.size = size;
      this.truncated = truncated;
    }

    /**
     * Constructs a deep copy of the given `BoundedTrieNode`.
     *
     * @param other The node to copy.
     */
    public BoundedTrieNode(BoundedTrieNode other) {
      this.truncated = other.truncated;
      this.size = other.size;
      this.children = new HashMap<>();
      // deep copy
      other.children.forEach((key, value) -> children.put(key, new BoundedTrieNode(value)));
    }

    /**
     * Adds a path represented by the given list of segments to this trie.
     *
     * @param segments The segments of the path to add.
     * @return The change in the size of the subtree rooted at this node.
     */
    public int add(List<String> segments) {
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
    public int addAll(List<List<String>> segmentsIter) {
      return segmentsIter.stream().mapToInt(this::add).sum();
    }

    /**
     * Trims this trie by truncating the largest subtree.
     *
     * @return The change in the size of the subtree rooted at this node.
     */
    public int trim() {
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
     * Merges the given `BoundedTrieNode` into this node.
     *
     * @param other The node to merge.
     * @return The change in the size of the subtree rooted at this node.
     */
    public int merge(BoundedTrieNode other) {
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
        children = new HashMap<>(other.children);
        int delta = other.size - size;
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
    public List<List<String>> flattened() {
      List<List<String>> result = new ArrayList<>();
      if (truncated) {
        result.add(Collections.singletonList(String.valueOf(true)));
      } else if (children.isEmpty()) {
        result.add(Collections.singletonList(String.valueOf(false)));
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
    public org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode toProto() {
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
    public static BoundedTrieNode fromProto(
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
    public boolean contains(List<String> segments) {
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
    public boolean isTruncated() {
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
