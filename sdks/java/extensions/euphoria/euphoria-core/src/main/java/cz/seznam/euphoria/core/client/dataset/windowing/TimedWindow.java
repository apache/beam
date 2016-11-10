package cz.seznam.euphoria.core.client.dataset.windowing;

/**
 * Extension to {@link cz.seznam.euphoria.core.client.dataset.windowing.Window}
 * defining time based constraints on the implementor.
 */
public interface TimedWindow<W extends Window> {

  /**
   * Defines the timestamp/watermark until this window is considered open.
   * For time based window this is typically the stamp of the end of the window.
   *
   * @return the absolute timestamp (in milliseconds) until this window is open
   */
  long maxTimestamp();

}
