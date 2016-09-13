package cz.seznam.euphoria.flink.streaming.windowing;

import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Objects;

public class EmissionWindow<W extends Window> extends Window {
  private final W inner;

  private long emissionWatermark = -1;

  public EmissionWindow(W inner) {
    this.inner = Objects.requireNonNull(inner);
  }

  public W getInner() {
    return inner;
  }

  public long getEmissionWatermark() {
    return emissionWatermark;
  }

  public void setEmissionWatermark(long emissionWatermark) {
    this.emissionWatermark = emissionWatermark;
  }

  @Override
  public long maxTimestamp() {
    return inner.maxTimestamp();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof EmissionWindow) {
      EmissionWindow<?> that = (EmissionWindow<?>) o;
      return this.inner.equals(that.inner);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return inner.hashCode();
  }

  @Override
  public String toString() {
    return "EmissionWindow{" +
        "inner=" + inner +
        ", emissionWatermark=" + emissionWatermark +
        '}';
  }
}