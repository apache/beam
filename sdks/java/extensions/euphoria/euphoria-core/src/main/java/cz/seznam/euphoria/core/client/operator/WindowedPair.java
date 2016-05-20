package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.util.Pair;

/** A pair along with a window identifier the pair belongs to. */
// ~ on purpose:
// - final (do not allow subclasses)
// - no equals/hashCode (need to derive the one from the parent)
public final class WindowedPair<W, A, B> extends Pair<A, B> {
  private final W wlabel;

  public static <WID, A, B> WindowedPair<WID, A, B>
  of(WID windowLabel, A first, B second) {
    return new WindowedPair<>(windowLabel, first, second);
  }

  WindowedPair(W wlabel, A first, B second) {
    super(first, second);
    this.wlabel = wlabel;
  }

  public W getWindowLabel() {
    return wlabel;
  }

  @Override
  public String toString() {
    return "WindowedPair{" +
        "wlabel=" + wlabel +
        ", first=" + getFirst() +
        ", second=" + getSecond() +
        '}';
  }
}
