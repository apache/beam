package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.io.Context;

abstract class FunctionContext<T> implements Context<T> {

  protected KeyedWindow window;

  @Override
  public abstract void collect(T elem);

  @Override
  public Object getWindow() {
    return this.window.window();
  }

  public void setWindow(KeyedWindow window) {
    this.window = window;
  }
}
