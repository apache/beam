package org.apache.beam.sdk.transforms.join;

public class PairResult<V1, V2> {
  private final V1 first;
  private final V2 second;

  public PairResult(V1 first, V2 second) {
    this.first = first;
    this.second = second;
  }

  public static <V1, V2> PairResult<V1, V2> of(V1 first, V2 second) {
    return new PairResult<>(first, second);
  }

  public V1 getFirst() {
    return first;
  }

  public V2 getSecond() {
    return second;
  }
}
