package cz.seznam.euphoria.flink.batch.io;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;


// FIXME can be dropped when proper InputSplitAssigner is implemented

/**
 * Simple implementation of {@link InputSplitAssigner} that ignores data locality.
 * Returns all inputs in order they were created. Will be replaced by similar
 * implementation as in {@link org.apache.flink.api.common.io.LocatableInputSplitAssigner}.
 */
class SimpleInputSplitAssigner implements InputSplitAssigner {

  private final Deque<PartitionWrapper<?>> partitions = new ArrayDeque<>();

  public SimpleInputSplitAssigner(PartitionWrapper<?>[] partitions) {
    Collections.addAll(this.partitions, partitions);
  }

  @Override
  public InputSplit getNextInputSplit(String host, int taskId) {
    synchronized (this.partitions) {
      return this.partitions.poll();
    }
  }
}
