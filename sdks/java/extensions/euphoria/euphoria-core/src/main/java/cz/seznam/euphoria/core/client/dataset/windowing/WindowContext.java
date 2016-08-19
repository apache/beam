package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * A state of grouping of input elements. Within euphoria,
 * a {@link Windowing} strategy associates each input element with a window
 * group and label thereby grouping input elements
 * into chunks for further processing in small (micro-)batches.
 *
 * @see Windowing
 */
public abstract class WindowContext<GROUP, LABEL> implements Serializable {

  protected final WindowID<GROUP, LABEL> windowID;

  protected WindowContext(WindowID<GROUP, LABEL> windowID) {
    this.windowID = windowID;
  }

  /** Retrieve window ID. */
  public WindowID<GROUP, LABEL> getWindowID() {
    return windowID;
  }
  
  /**
   * Returns list of triggers used by this instance of {@link WindowContext}
   */
  public List<Trigger> createTriggers() {
    return Collections.emptyList();
  }

  /**
   * Called after element was added to the window.
   * @return true the window is complete and should be triggered
   * @return false if the window is not complete yet
   */
  public boolean onElementAdded() {
    return false;
  }

  @Override
  public final int hashCode() {
    return getWindowID().hashCode();
  }

  @Override
  public final boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final WindowContext<?, ?> other = (WindowContext<?, ?>) obj;
    return other.getWindowID().equals(windowID);
  }

}