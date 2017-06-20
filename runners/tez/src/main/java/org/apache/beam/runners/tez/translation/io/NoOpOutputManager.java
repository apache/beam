package org.apache.beam.runners.tez.translation.io;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * {@link TezOutputManager} implementation for when the {@link org.apache.tez.dag.api.Vertex} has no output.
 * Used in cases such as when the ParDo within the Vertex writes the output itself.
 */
public class NoOpOutputManager extends TezOutputManager {

  public NoOpOutputManager() {
    super(null);
  }

  @Override
  public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {}
}
