package org.apache.beam.runners.fnexecution.control;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;

/**
 * A pair of {@link Coder} and {@link FnDataReceiver} which can be registered to receive elements
 * for a {@link LogicalEndpoint}.
 */
@AutoValue
public abstract class RemoteOutputReceiver<T> {
  public static <T> RemoteOutputReceiver of (Coder<T> coder, FnDataReceiver<T> receiver) {
    return new AutoValue_RemoteOutputReceiver<>(coder, receiver);
  }

  public abstract Coder<T> getCoder();
  public abstract FnDataReceiver<T> getReceiver();
}
