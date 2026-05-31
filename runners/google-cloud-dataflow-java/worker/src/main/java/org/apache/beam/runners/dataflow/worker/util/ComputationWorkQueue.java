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

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor.QueuedWork;

/**
 * A custom, thread-safe doubly-linked BlockingQueue that groups pending tasks by computation ID.
 * Achieves true O(1) targeted work stealing and FIFO queue mutations.
 */
@SuppressWarnings({
  "nullness", // Suppress Checker Framework nullness warnings for pointer operations
  "initialization" // Suppress initialization and underinitialization warnings for sentinel pointer
  // setup
})
class ComputationWorkQueue extends AbstractQueue<Runnable> implements BlockingQueue<Runnable> {

  static class Node {
    final Runnable task;
    final String computationId;

    Node prevGlobal;
    Node nextGlobal;
    Node prevComp;
    Node nextComp;

    boolean dequeued = false;

    Node(Runnable task) {
      this.task = task;
      if (task instanceof QueuedWork) {
        this.computationId = ((QueuedWork) task).getWork().getComputationId();
      } else {
        this.computationId = null;
      }
    }
  }

  private static class ComputationList {
    final Node head;
    final Node tail;

    ComputationList() {
      head = new Node(null);
      tail = new Node(null);
      head.nextComp = tail;
      tail.prevComp = head;
    }

    boolean isEmpty() {
      return head.nextComp == tail;
    }

    void append(Node node) {
      Node last = tail.prevComp;
      node.prevComp = last;
      node.nextComp = tail;
      last.nextComp = node;
      tail.prevComp = node;
    }

    void remove(Node node) {
      if (node.prevComp != null && node.nextComp != null) {
        node.prevComp.nextComp = node.nextComp;
        node.nextComp.prevComp = node.prevComp;
        node.prevComp = null;
        node.nextComp = null;
      }
    }
  }

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition notEmpty = lock.newCondition();

  // Sentinels for the global list
  private final Node globalHead = new Node(null);
  private final Node globalTail = new Node(null);

  // Map of active computation queues
  private final Map<String, ComputationList> compLists = new HashMap<>();

  private int size = 0;

  public ComputationWorkQueue() {
    globalHead.nextGlobal = globalTail;
    globalTail.prevGlobal = globalHead;
  }

  private void unlinkNode(Node node) {
    if (node.dequeued) {
      return;
    }
    node.dequeued = true;

    // 1. Unlink from global list
    Node prevG = node.prevGlobal;
    Node nextG = node.nextGlobal;
    if (prevG != null && nextG != null) {
      prevG.nextGlobal = nextG;
      nextG.prevGlobal = prevG;
    }
    node.prevGlobal = null;
    node.nextGlobal = null;

    // 2. Unlink from computation list
    if (node.computationId != null) {
      ComputationList compList = compLists.get(node.computationId);
      if (compList != null) {
        compList.remove(node);
        if (compList.isEmpty()) {
          compLists.remove(node.computationId); // Prevent memory leaks of empty keys
        }
      }
    }

    size--;
  }

  private Node removeFirstGlobal() {
    Node first = globalHead.nextGlobal;
    if (first == globalTail) {
      return null;
    }
    unlinkNode(first);
    return first;
  }

  public QueuedWork pollWork(String computationId) {
    if (computationId == null) {
      return null;
    }
    lock.lock();
    try {
      ComputationList compList = compLists.get(computationId);
      if (compList == null || compList.isEmpty()) {
        return null;
      }

      // Retrieve the first pending task for this computation in O(1)
      Node firstNode = compList.head.nextComp;
      unlinkNode(firstNode);

      return (QueuedWork) firstNode.task;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean offer(Runnable e) {
    if (e == null) throw new NullPointerException();
    lock.lock();
    try {
      Node node = new Node(e);

      // Append to global list tail
      Node lastG = globalTail.prevGlobal;
      node.prevGlobal = lastG;
      node.nextGlobal = globalTail;
      lastG.nextGlobal = node;
      globalTail.prevGlobal = node;

      // Append to computation list if applicable
      if (node.computationId != null) {
        ComputationList compList =
            compLists.computeIfAbsent(node.computationId, k -> new ComputationList());
        compList.append(node);
      }

      size++;
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
  public Runnable poll() {
    lock.lock();
    try {
      Node node = removeFirstGlobal();
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
      Node node = removeFirstGlobal();
      return node.task;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
    long nanos = unit.toNanos(timeout);
    lock.lockInterruptibly();
    try {
      while (size == 0) {
        if (nanos <= 0) {
          return null;
        }
        nanos = notEmpty.awaitNanos(nanos);
      }
      Node node = removeFirstGlobal();
      return node.task;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Runnable peek() {
    lock.lock();
    try {
      Node first = globalHead.nextGlobal;
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
      Node curr = globalHead.nextGlobal;
      while (curr != globalTail) {
        if (curr.task.equals(o)) {
          unlinkNode(curr);
          return true;
        }
        curr = curr.nextGlobal;
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
      Node curr = globalHead.nextGlobal;
      while (curr != globalTail) {
        if (curr.task.equals(o)) {
          return true;
        }
        curr = curr.nextGlobal;
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
      Node curr = globalHead.nextGlobal;
      while (curr != globalTail && added < maxElements) {
        Node next = curr.nextGlobal;
        unlinkNode(curr);
        c.add(curr.task);
        added++;
        curr = next;
      }
      return added;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void clear() {
    // WARNING: Calling clear() directly on this queue will unlink tasks but will NOT
    // close their respective work budget handles, leaking elementsOutstanding/bytesOutstanding
    // in BoundedQueueExecutor. Call executor.remove(r) or let worker threads consume them.
    lock.lock();
    try {
      Node curr = globalHead.nextGlobal;
      while (curr != globalTail) {
        Node next = curr.nextGlobal;
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
      java.util.List<Runnable> snapshot = new java.util.ArrayList<>(size);
      Node curr = globalHead.nextGlobal;
      while (curr != globalTail) {
        if (curr.task != null) {
          snapshot.add(curr.task);
        }
        curr = curr.nextGlobal;
      }
      return java.util.Collections.unmodifiableList(snapshot).iterator();
    } finally {
      lock.unlock();
    }
  }
}
