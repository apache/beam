package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A state of grouping of input elements. Within euphoria,
 * a {@link Windowing} strategy associates each input element with a window
 * group and label thereby grouping input elements
 * into chunks for further processing in small (micro-)batches.
 *
 * @see Windowing
 */
public abstract class WindowContext<LABEL> implements Serializable {

  protected final WindowID<LABEL> windowID;

  protected WindowContext(WindowID<LABEL> windowID) {
    this.windowID = Objects.requireNonNull(windowID);
  }

  /** Retrieve window ID. */
  public WindowID<LABEL> getWindowID() {
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
    final WindowContext<?> other = (WindowContext<?>) obj;
    return other.getWindowID().equals(windowID);
  }

}