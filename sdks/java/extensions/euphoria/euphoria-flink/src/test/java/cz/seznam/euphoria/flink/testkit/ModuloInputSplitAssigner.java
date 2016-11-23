package cz.seznam.euphoria.flink.testkit;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.stream.IntStream;

class ModuloInputSplitAssigner implements InputSplitAssigner {

  private final Deque<InputSplit>[] partitions;

  public ModuloInputSplitAssigner(InputSplit[] splits, int partitions) {
    this.partitions = IntStream.range(0, partitions)
        .mapToObj(i -> new ArrayDeque<>())
        .toArray(Deque[]::new);
    for (int i = 0; i < splits.length; i++) {
      this.partitions[i % partitions].push(splits[i]);
    }
  }

  @Override
  public InputSplit getNextInputSplit(String host, int taskId) {
    synchronized (this.partitions) {
      return this.partitions[taskId].poll();
    }
  }
}
