package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

class IntWindow extends Window implements Comparable<IntWindow> {
  private int val;

  IntWindow(int val) {
    this.val = val;
  }

  public int getValue() {
    return val;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof IntWindow) {
      IntWindow that = (IntWindow) o;
      return this.val == that.val;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return val;
  }


  @Override
  public int compareTo(IntWindow o) {
    return val - o.val;
  }
}
