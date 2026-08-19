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
package org.apache.beam.runners.dataflow.worker.util;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.Work.KeyGroup;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor.QueuedWork;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A custom, thread-safe doubly-linked BlockingQueue. In addition to global FIFO ordering, the queue
 * supports polling work by computation + key group in FIFO order
 */
class KeyGroupWorkQueue extends AbstractQueue<Runnable> implements BlockingQueue<Runnable> {

  public static final Runnable SENTINEL_RUNNABLE =
      () -> {
        throw new IllegalStateException("sentinel runnable called");
      };

  static class Node {
    // If keyGroup is non-null, task is an instance of QueuedWork
    final Runnable task;
    final @Nullable String computationId;
    final Work.@Nullable KeyGroup keyGroup;
    // cached keyGroupList if the Node is part of one.
    @Nullable KeyGroupWorkList keyGroupList;

    // prevNode, nextNode are used for the global order across all queued Runnables
    @Nullable Node prevNode;
    @Nullable Node nextNode;

    // prevKeyGroupNode and nextKeyGroupNode are used for the keyGroup level lists linking
    // QueuedWork with same keyGroup
    @Nullable Node prevKeyGroupNode;
    @Nullable Node nextKeyGroupNode;

    Node(Runnable task) {
      this.task = task;
      if (task instanceof QueuedWork) {
        this.computationId = ((QueuedWork) task).getWork().getComputationId();
        this.keyGroup = ((QueuedWork) task).getWork().getKeyGroup();
      } else {
        this.computationId = null;
        this.keyGroup = null;
      }
    }
  }

  /** Double linked list implementing key group level queue */
  private static class KeyGroupWorkList {
    final Node head = new Node(SENTINEL_RUNNABLE);
    final Node tail = new Node(SENTINEL_RUNNABLE);

    KeyGroupWorkList() {
      head.nextKeyGroupNode = tail;
      tail.prevKeyGroupNode = head;
    }

    boolean isEmpty() {
      return head.nextKeyGroupNode == tail;
    }

    void append(Node node) {
      Node last = checkStateNotNull(tail.prevKeyGroupNode);
      node.prevKeyGroupNode = last;
      node.nextKeyGroupNode = tail;
      last.nextKeyGroupNode = node;
      tail.prevKeyGroupNode = node;
    }

    void remove(Node node) {
      @Nullable Node prev = node.prevKeyGroupNode;
      @Nullable Node next = node.nextKeyGroupNode;
      if (prev != null && next != null) {
        prev.nextKeyGroupNode = next;
        next.prevKeyGroupNode = prev;
        node.prevKeyGroupNode = null;
        node.nextKeyGroupNode = null;
      }
    }
  }

  private final ReentrantLock lock;
  private final Condition notEmpty;

  // Sentinels for the global list
  @GuardedBy("lock")
  private final Node globalHead = new Node(SENTINEL_RUNNABLE);

  @GuardedBy("lock")
  private final Node globalTail = new Node(SENTINEL_RUNNABLE);

  @GuardedBy("lock")
  private final Map<QueueKey, KeyGroupWorkList> keyGroupQueueMap = new HashMap<>();

  @GuardedBy("lock")
  private int size = 0;

  public KeyGroupWorkQueue(boolean fair) {
    this.lock = new ReentrantLock(fair);
    this.notEmpty = lock.newCondition();
    globalHead.nextNode = globalTail;
    globalTail.prevNode = globalHead;
  }

  @GuardedBy("lock")
  private void unlinkNode(Node node) {
    // An existing node should always have previous and next since we have sentinels
    // 1. Unlink from global list
    Node prevG = checkArgumentNotNull(node.prevNode);
    Node nextG = checkArgumentNotNull(node.nextNode);
    prevG.nextNode = nextG;
    nextG.prevNode = prevG;
    node.prevNode = null;
    node.nextNode = null;

    // 2. Unlink from key group list
    KeyGroupWorkList list = node.keyGroupList;
    if (list != null) {
      list.remove(node);
      if (list.isEmpty()) {
        String compId = checkStateNotNull(node.computationId);
        Work.KeyGroup keyGroup = checkStateNotNull(node.keyGroup);
        QueueKey key = new QueueKey(compId, keyGroup);
        keyGroupQueueMap.remove(key);
      }
      node.keyGroupList = null;
    }
    --size;
  }

  @GuardedBy("lock")
  private @Nullable Node removeFirstGlobal() {
    Node first = checkStateNotNull(globalHead.nextNode);
    if (first == globalTail) {
      return null;
    }
    unlinkNode(first);
    return first;
  }

  /**
   * Remove and Return QueuedWork for the computationId, keyGroup in the FIFO order. Returns null,
   * if there are no matches.
   *
   * @param keyGroup should not be equal to KeyGroup.DEFAULT
   */
  public @Nullable QueuedWork pollWork(String computationId, Work.KeyGroup keyGroup) {
    checkArgument(computationId != null && keyGroup != null && !keyGroup.equals(KeyGroup.DEFAULT));
    QueueKey key = new QueueKey(computationId, keyGroup);
    lock.lock();
    try {
      KeyGroupWorkList keyGroupWorkList = keyGroupQueueMap.get(key);
      if (keyGroupWorkList == null || keyGroupWorkList.isEmpty()) {
        return null;
      }

      // Retrieve the first pending task for this computation and keyGroup in O(1)
      Node firstNode = checkStateNotNull(keyGroupWorkList.head.nextKeyGroupNode);
      if (firstNode == keyGroupWorkList.tail) {
        return null;
      }
      unlinkNode(firstNode);

      return (QueuedWork) firstNode.task;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean offer(@NonNull Runnable runnable) {
    Node node = new Node(checkStateNotNull(runnable));
    lock.lock();
    try {
      // Append to global list tail
      Node lastG = checkStateNotNull(globalTail.prevNode);
      node.prevNode = lastG;
      node.nextNode = globalTail;
      lastG.nextNode = node;
      globalTail.prevNode = node;

      // Append to key group list if applicable
      String compId = node.computationId;
      Work.KeyGroup keyGroup = node.keyGroup;
      if (compId != null && keyGroup != null && !keyGroup.equals(KeyGroup.DEFAULT)) {
        QueueKey key = new QueueKey(compId, keyGroup);
        KeyGroupWorkList keyGroupWorkList =
            keyGroupQueueMap.computeIfAbsent(key, k -> new KeyGroupWorkList());
        keyGroupWorkList.append(node);
        node.keyGroupList = keyGroupWorkList;
      }

      ++size;
      notEmpty.signal();
      return true;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void put(Runnable e) throws InterruptedException {
    offer(e); // Unbounded queue
  }

  @Override
  public boolean offer(Runnable e, long timeout, TimeUnit unit) throws InterruptedException {
    return offer(e); // Unbounded queue
  }

  @Override
  public @Nullable Runnable poll() {
    lock.lock();
    try {
      @Nullable Node node = removeFirstGlobal();
      return (node != null) ? node.task : null;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Runnable take() throws InterruptedException {
    lock.lockInterruptibly();
    try {
      while (size == 0) {
        notEmpty.await();
      }
      @Nullable Node node = removeFirstGlobal();
      checkStateNotNull(node, "Queue is empty but size was " + size);
      return node.task;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public @Nullable Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
    long nanos = unit.toNanos(timeout);
    lock.lockInterruptibly();
    try {
      while (size == 0) {
        if (nanos <= 0) {
          return null;
        }
        nanos = notEmpty.awaitNanos(nanos);
      }
      @Nullable Node node = removeFirstGlobal();
      return (node != null) ? node.task : null;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public @Nullable Runnable peek() {
    lock.lock();
    try {
      Node first = checkStateNotNull(globalHead.nextNode);
      if (first == globalTail) {
        return null;
      }
      return first.task;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int size() {
    lock.lock();
    try {
      return size;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean isEmpty() {
    lock.lock();
    try {
      return size == 0;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean remove(Object o) {
    if (o == null) return false;
    lock.lock();
    try {
      // Walk the global queue in O(N) to find and unlink the node
      Node curr = checkStateNotNull(globalHead.nextNode);
      while (curr != globalTail) {
        if (o.equals(curr.task)) {
          unlinkNode(curr);
          return true;
        }
        curr = checkStateNotNull(curr.nextNode);
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean contains(Object o) {
    if (o == null) return false;
    lock.lock();
    try {
      Node curr = checkStateNotNull(globalHead.nextNode);
      while (curr != globalTail) {
        if (o.equals(curr.task)) {
          return true;
        }
        curr = checkStateNotNull(curr.nextNode);
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int remainingCapacity() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int drainTo(Collection<? super Runnable> c) {
    return drainTo(c, Integer.MAX_VALUE);
  }

  @Override
  public int drainTo(Collection<? super Runnable> c, int maxElements) {
    if (c == null) throw new NullPointerException();
    if (c == this) throw new IllegalArgumentException();
    if (maxElements <= 0) return 0;
    lock.lock();
    try {
      int added = 0;
      Node curr = checkStateNotNull(globalHead.nextNode);
      while (curr != globalTail && added < maxElements) {
        Node next = checkStateNotNull(curr.nextNode);
        unlinkNode(curr);
        Runnable task = curr.task;
        c.add(task);
        ++added;
        curr = next;
      }
      return added;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void clear() {
    lock.lock();
    try {
      Node curr = checkStateNotNull(globalHead.nextNode);
      while (curr != globalTail) {
        Node next = checkStateNotNull(curr.nextNode);
        unlinkNode(curr);
        curr = next;
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Iterator<Runnable> iterator() {
    lock.lock();
    try {
      ImmutableList.Builder<Runnable> builder = ImmutableList.builderWithExpectedSize(size);
      Node curr = checkStateNotNull(globalHead.nextNode);
      while (curr != globalTail) {
        builder.add(curr.task);
        curr = checkStateNotNull(curr.nextNode);
      }
      return builder.build().iterator();
    } finally {
      lock.unlock();
    }
  }

  static final class QueueKey {
    private final String computationId;
    private final Work.KeyGroup keyGroup;

    QueueKey(String computationId, Work.KeyGroup keyGroup) {
      this.computationId = computationId;
      this.keyGroup = keyGroup;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof QueueKey)) {
        return false;
      }
      QueueKey other = (QueueKey) o;
      return computationId.equals(other.computationId) && keyGroup.equals(other.keyGroup);
    }

    @Override
    public int hashCode() {
      return Objects.hash(computationId, keyGroup);
    }

    @Override
    public String toString() {
      return "QueueKey{"
          + "computationId='"
          + computationId
          + '\''
          + ", keyGroup="
          + keyGroup
          + '}';
    }
  }
}
