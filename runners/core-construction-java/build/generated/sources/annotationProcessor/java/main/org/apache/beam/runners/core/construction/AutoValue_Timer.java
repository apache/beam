package org.apache.beam.runners.core.construction;

import java.util.Collection;
import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Timer<K> extends Timer<K> {

  private final K userKey;

  private final String dynamicTimerTag;

  private final Collection<? extends BoundedWindow> windows;

  private final boolean clearBit;

  private final @Nullable Instant fireTimestamp;

  private final @Nullable Instant holdTimestamp;

  private final @Nullable PaneInfo pane;

  AutoValue_Timer(
      K userKey,
      String dynamicTimerTag,
      Collection<? extends BoundedWindow> windows,
      boolean clearBit,
      @Nullable Instant fireTimestamp,
      @Nullable Instant holdTimestamp,
      @Nullable PaneInfo pane) {
    this.userKey = userKey;
    if (dynamicTimerTag == null) {
      throw new NullPointerException("Null dynamicTimerTag");
    }
    this.dynamicTimerTag = dynamicTimerTag;
    if (windows == null) {
      throw new NullPointerException("Null windows");
    }
    this.windows = windows;
    this.clearBit = clearBit;
    this.fireTimestamp = fireTimestamp;
    this.holdTimestamp = holdTimestamp;
    this.pane = pane;
  }

  @Override
  public K getUserKey() {
    return userKey;
  }

  @Override
  public String getDynamicTimerTag() {
    return dynamicTimerTag;
  }

  @Override
  public Collection<? extends BoundedWindow> getWindows() {
    return windows;
  }

  @Override
  public boolean getClearBit() {
    return clearBit;
  }

  @Override
  public @Nullable Instant getFireTimestamp() {
    return fireTimestamp;
  }

  @Override
  public @Nullable Instant getHoldTimestamp() {
    return holdTimestamp;
  }

  @Override
  public @Nullable PaneInfo getPane() {
    return pane;
  }

  @Override
  public String toString() {
    return "Timer{"
        + "userKey=" + userKey + ", "
        + "dynamicTimerTag=" + dynamicTimerTag + ", "
        + "windows=" + windows + ", "
        + "clearBit=" + clearBit + ", "
        + "fireTimestamp=" + fireTimestamp + ", "
        + "holdTimestamp=" + holdTimestamp + ", "
        + "pane=" + pane
        + "}";
  }

}
