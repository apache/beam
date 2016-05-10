/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.io.Collector;

import java.io.Closeable;

/**
 * A state for stateful operations.
 */
public abstract class State<IN, OUT> implements Closeable {

  /** Collector of output of this state. */
  protected final Collector<OUT> collector;

  /** Add element to this state. */
  public abstract void add(IN element);

  /**
   * Flush the state to output. Invoked when window this
   * state is part of gets disposed/triggered.
   */
  public abstract void flush();

  protected State(Collector<OUT> collector) {
    this.collector = collector;
  }

  /**
   * Closes this state. Invoked after {@link #flush()} and before
   * this state gets disposed.
   */
  public void close() {
    // ~ no-op by default
  }

}
