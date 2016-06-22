package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.io.Serializable;
import java.util.List;

/**
 * A grouping of input elements for further processing. Within euphoria,
 * a {@link Windowing} strategy associates each input element with a window
 * thereby grouping input elements into chunks for further processing in small
 * (micro-)batches. A window is considered equal to another iff both windows
 * {@link #getGroup()} and {@link #getLabel()} are equals respectively.
 *
 * @see Windowing
 */
public interface Window<GROUP, LABEL> extends Serializable {
  /**
   * Retrieves the key of group this window belongs to. Grouped windows
   * are subject to merging. Windows in different groups will never be
   * merged with windows from another group. <p />
   *
   * @return a group identifier (possibly {@code null})
   */
  GROUP getGroup();

  /**
   * Retrieves the identifier of this window within its group.
   *
   * @return the label identifying this window within its group;
   *          must not be {@code null}
   */
  LABEL getLabel();

  /**
   * Returns list of triggers used by this instance of {@link Window}
   */
  List<Trigger> createTriggers();

}