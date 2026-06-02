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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor.QueuedWork;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A custom, thread-safe doubly-linked BlockingQueue. In addition to global FIFO ordering, the queue
 * supports polling work by computation + key group in FIFO order
 */
class KeyGroupWorkQueue extends AbstractQueue<Runnable> implements BlockingQueue<Runnable> {

  static class Node {
    final @Nullable Runnable task;
    final @Nullable String computationId;
    final Work.@Nullable KeyGroup keyGroup;

    // prevNode, nextNode are used for the global order across all queued Runnables
    @Nullable Node prevNode;
    @Nullable Node nextNode;

    // prevKeyGroupNode and nextKeyGroupNode are used for the keyGroup level lists linking
    // QueuedWork with same keyGroup
    @Nullable Node prevKeyGroupNode;
    @Nullable Node nextKeyGroupNode;

    Node(@Nullable Runnable task) {
      this.task = task;
      if (task instanceof QueuedWork) {
        this.computationId = ((QueuedWork) task).getWork().getComputationId();
        this.keyGroup = ((QueuedWork) task).getWork().getKeyGroup().orElse(null);
      } else {
        this.computationId = null;
        this.keyGroup = null;
      }
    }
  }

  /** Double linked list implementing key group level queue */
  private static class KeyGroupWorkList {
    final Node head;
    final Node tail;

    KeyGroupWorkList() {
      head = new Node(null);
      tail = new Node(null);
      head.nextKeyGroupNode = tail;
      tail.prevKeyGroupNode = head;
    }

    boolean isEmpty() {
      return head.nextKeyGroupNode == tail;
    }

    void append(Node node) {
      @Nullable Node last = tail.prevKeyGroupNode;
      if (last == null) {
        throw new NullPointerException("tail.prevComp is null");
      }
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

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition notEmpty = lock.newCondition();

  // Sentinels for the global list
  private final Node globalHead = new Node(null);
  private final Node globalTail = new Node(null);

  private final Map<QueueKey, KeyGroupWorkList> keyGroupQueueMap = new HashMap<>();

  private int size = 0;

  public KeyGroupWorkQueue() {
    globalHead.nextNode = globalTail;
    globalTail.prevNode = globalHead;
  }

  private void unlinkNode(Node node) {
    // 1. Unlink from global list
    Node prevG = node.prevNode;
    Node nextG = node.nextNode;
    if (prevG != null && nextG != null) {
      prevG.nextNode = nextG;
      nextG.prevNode = prevG;
    }
    node.prevNode = null;
    node.nextNode = null;

    // 2. Unlink from key group list
    if (node.computationId != null) {
      QueueKey key = QueueKey.create(node.computationId, node.keyGroup);
      KeyGroupWorkList keyGroupQueue = keyGroupQueueMap.get(key);
      if (keyGroupQueue != null) {
        keyGroupQueue.remove(node);
        if (keyGroupQueue.isEmpty()) {
          keyGroupQueueMap.remove(key);
        }
      }
    }
    --size;
  }

  private @Nullable Node removeFirstGlobal() {
    @Nullable Node first = globalHead.nextNode;
    if (first == null || first == globalTail) {
      return null;
    }
    unlinkNode(first);
    return first;
  }

  /**
   * Remove and Return QueuedWork for the computationId, keyGroup in the FIFO order Returns null, if
   * there are no matches.
   */
  public @Nullable QueuedWork pollWork(String computationId, Work.KeyGroup keyGroup) {
    if (computationId == null || keyGroup == null) {
      return null;
    }
    lock.lock();
    try {
      QueueKey key = QueueKey.create(computationId, keyGroup);
      KeyGroupWorkList keyGroupWorkList = keyGroupQueueMap.get(key);
      if (keyGroupWorkList == null || keyGroupWorkList.isEmpty()) {
        return null;
      }

      // Retrieve the first pending task for this computation and keyGroup in O(1)
      @Nullable Node firstNode = keyGroupWorkList.head.nextKeyGroupNode;
      if (firstNode == null || firstNode == keyGroupWorkList.tail) {
        return null;
      }
      unlinkNode(firstNode);

      Runnable task = firstNode.task;
      if (task == null) {
        return null;
      }
      return (QueuedWork) task;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean offer(Runnable runnable) {
    if (runnable == null) throw new NullPointerException();
    lock.lock();
    try {
      Node node = new Node(runnable);

      // Append to global list tail
      @Nullable Node lastG = globalTail.prevNode;
      if (lastG == null) {
        throw new NullPointerException("globalTail.prevNode is null");
      }
      node.prevNode = lastG;
      node.nextNode = globalTail;
      lastG.nextNode = node;
      globalTail.prevNode = node;

      // Append to key group list if applicable
      if (node.computationId != null) {
        QueueKey key = QueueKey.create(node.computationId, node.keyGroup);
        KeyGroupWorkList keyGroupWorkList =
            keyGroupQueueMap.computeIfAbsent(key, k -> new KeyGroupWorkList());
        keyGroupWorkList.append(node);
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
      Runnable task = node.task;
      checkStateNotNull(task, "Encountered null task in queue");
      return task;
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
      @Nullable Node first = globalHead.nextNode;
      if (first == null || first == globalTail) {
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
      @Nullable Node curr = globalHead.nextNode;
      while (curr != null && curr != globalTail) {
        if (o.equals(curr.task)) {
          unlinkNode(curr);
          return true;
        }
        curr = curr.nextNode;
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
      @Nullable Node curr = globalHead.nextNode;
      while (curr != null && curr != globalTail) {
        if (o.equals(curr.task)) {
          return true;
        }
        curr = curr.nextNode;
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
      @Nullable Node curr = globalHead.nextNode;
      while (curr != null && curr != globalTail && added < maxElements) {
        @Nullable Node next = curr.nextNode;
        unlinkNode(curr);
        Runnable task = curr.task;
        if (task != null) {
          c.add(task);
          ++added;
        }
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
      @Nullable Node curr = globalHead.nextNode;
      while (curr != null && curr != globalTail) {
        @Nullable Node next = curr.nextNode;
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
      List<Runnable> snapshot = new ArrayList<>(size);
      @Nullable Node curr = globalHead.nextNode;
      while (curr != null && curr != globalTail) {
        if (curr.task != null) {
          snapshot.add(curr.task);
        }
        curr = curr.nextNode;
      }
      return Collections.unmodifiableList(snapshot).iterator();
    } finally {
      lock.unlock();
    }
  }

  static final class QueueKey {
    private final String computationId;
    private final Work.@Nullable KeyGroup keyGroup;

    private QueueKey(String computationId, Work.@Nullable KeyGroup keyGroup) {
      this.computationId = Objects.requireNonNull(computationId);
      this.keyGroup = keyGroup;
    }

    public static QueueKey create(String computationId, Work.@Nullable KeyGroup keyGroup) {
      return new QueueKey(computationId, keyGroup);
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
      return computationId.equals(other.computationId) && Objects.equals(keyGroup, other.keyGroup);
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
