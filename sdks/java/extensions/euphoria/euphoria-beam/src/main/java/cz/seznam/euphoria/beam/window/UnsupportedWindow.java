package cz.seznam.euphoria.beam.window;

import java.util.Objects;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;


/**
 * Window used as type parameter of {@link BeamWindowing}.
 */
final class UnsupportedWindow extends Window<UnsupportedWindow> {

  private UnsupportedWindow(){
    //Do not instantiate
  }

  @Override
  public int compareTo(UnsupportedWindow o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long maxTimestamp() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object obj) {
    return Objects.equals(this, obj);
  }


}
