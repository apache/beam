package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.spark.broadcast.Broadcast;

/** Broadcast helper for side inputs. */
public class SideInputBroadcast implements Serializable {

  private Map<String, Broadcast<?>> bcast = new HashMap<>();
  private Map<String, Coder<?>> coder = new HashMap<>();

  public SideInputBroadcast(){}

  public void add(String key, Broadcast<?> bcast, Coder<?> coder) {
    this.bcast.put(key, bcast);
    this.coder.put(key, coder);
  }

  public Broadcast<?> getBroadcastValue(String key) {
    return bcast.get(key);
  }

  public Coder<?> getCoder(String key) {
    return coder.get(key);
  }
}
