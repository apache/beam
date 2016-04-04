/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.io.Collector;
import java.io.Closeable;

/**
 * A state for statefull operations.
 */
public abstract class State<IN, OUT> implements Closeable {

  /** Is the state already closed? Needed for idempotence of close. */
  private boolean isClosed = false;

  /** Collector of output of this state. */
  protected final Collector<OUT> collector;

  /** Add element to this state. */
  public abstract void add(IN element);

  /** Flush the state to output. */
  public abstract void flush();

  protected State(Collector<OUT> collector) {
    this.collector = collector;
  }

  @Override
  public final void close() {
    if (!isClosed) {
      flush();
      isClosed = true;
    }
  }

}
