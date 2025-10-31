package org.apache.beam.sdk.ml.remoteinference.base;

import java.io.Serializable;

public class BatchConfig implements Serializable {

  private final int minBatchSize;
  private final int maxBatchSize;

  private BatchConfig(Builder builder) {
    this.minBatchSize = builder.minBatchSize;
    this.maxBatchSize = builder.maxBatchSize;
  }

  public int getMinBatchSize() {
    return minBatchSize;
  }

  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private int minBatchSize;
    private int maxBatchSize;

    public Builder minBatchSize(int minBatchSize) {
      this.minBatchSize = minBatchSize;
      return this;
    }

    public Builder maxBatchSize(int maxBatchSize) {
      this.maxBatchSize = maxBatchSize;
      return this;
    }

    public BatchConfig build() {
      return new BatchConfig(this);
    }
  }
}
