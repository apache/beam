package org.apache.beam.runners.samza.adapter;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.io.UnboundedSource;

/**
 * A integer CheckpointMark for testing.
 */
public class TestCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {
  final int checkpoint;

  private TestCheckpointMark(int checkpoint) {
    this.checkpoint = checkpoint;
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    //DO NOTHING
  }

  static TestCheckpointMark of(int checkpoint) {
    return new TestCheckpointMark(checkpoint);
  }
}
