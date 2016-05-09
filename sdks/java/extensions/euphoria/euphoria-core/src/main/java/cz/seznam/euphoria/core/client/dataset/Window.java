package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import java.io.Serializable;

/**
 * A bunch of elements joined into window.
 */
// XXX consider dropping the GROUP type parameter by restricting the type merely to
// Object only; "groups" are only used by executors, hence, we could avoid one more
// type parameter in the client api
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
   * Register a function to be called by the triggering when a window
   * completion event occurs
   * @param triggering the registering service
   * @param evict the callback to be called when the trigger fires
   */
  void registerTrigger(
      Triggering triggering, UnaryFunction<Window<?, ?>, Void> evict);
}